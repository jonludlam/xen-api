module D=Debug.Make(struct let name="xapi_database_backup" end)
open D

open Http
open Xmlrpc_client
open Db_cache_types
open Stdext
open Threadext

type delta =
  { fresh_token: int64
  ; last_event_token: int64
  ; counts : (string * int) list
  ; slice : Db_cache_types.TableSet.t
  } [@@deriving rpcty]

let rpc_of ty x = Rpcmarshal.marshal ty.Rpc.Types.ty x

exception Content_length_required
let delta_to_string (d:delta) = Jsonrpc.to_string (rpc_of delta d)

let get_gen db =
  db
  |> Database.manifest
  |> Manifest.generation

(* Master Functions *)

let get_table_list db =
  TableSet.fold_over_recent (-2L) (fun table stat value acc -> table::acc) (Database.tableset db) []

let counter db table =
  (table, (Table.fold (fun _ _ _ acc -> succ(acc)) (TableSet.find table (Database.tableset db)) 0))

let object_count db =
  List.rev_map (counter db) (get_table_list db)

let backup_condition = Condition.create ()

let get_delta __context token =
  Mutex.execute Db_lock.dbcache_mutex (fun () ->
      let rec get () =
        let db = Db_ref.get_database (Context.database_of __context) in
        let slice = Db_cache_types.TableSet.slice_recent token (Database.tableset db) in
        if Db_cache_types.TableSet.keys slice = [] then begin
          Condition.wait backup_condition Db_lock.dbcache_mutex;
          get ()
        end else
          (db,slice)
      in
      let (db,slice) = get () in
      let t = Manifest.generation (Database.manifest db) in
      {fresh_token=t;
       last_event_token=Int64.pred t;
       counts=object_count db;
       slice=Db_cache_types.TableSet.slice_recent token (Database.tableset db)})

let database_callback update t =
  Db_lock.with_lock (fun () ->
      Condition.broadcast backup_condition)

let handler (req: Request.t) s _ =
  try
    let token = Generation.of_string (snd (List.find (fun (x,y) -> x = "token") req.query)) in
    Server_helpers.exec_with_new_task "Database backup" ~task_in_database:false (fun __context ->
        token
        |> get_delta __context
        |> rpc_of delta
        |> Jsonrpc.to_string
        |> Http_svr.response_str req s
      )
  with Not_found -> debug "No valid token found in request"

(* Slave Functions *)


let compare_by_tblname (c1: (string * int)) (c2: (string * int)) =
  String.compare (fst c1) (fst c2)

let count_check (c1_list: (string * int) list) (c2_list: (string * int) list) =
  let c1_list = List.sort compare_by_tblname c1_list in
  let c2_list = List.sort compare_by_tblname c2_list in
  try
    List.for_all2 (fun (a,b) (c,d) -> a=c && b=d) c1_list c2_list
  with invalid_arg -> false


let apply_changes (delta:delta) =
  let open Db_cache_types in
  let db = !Xapi_slave_db.slave_db in
  let objects = TableSet.fold (fun tblname _st tbl acc ->
    Table.keys tbl
    |> List.fold_left (fun acc ref -> (tblname,ref)::acc) acc) delta.slice []
  in
  let objects = TableSet.fold (fun tblname _st tbl acc ->
    Table.get_deleted tbl
    |> List.fold_left (fun acc (_,_,ref) -> (tblname,ref)::acc) acc) delta.slice objects
  in
  let newdb = 
    Db_cache_types.Database.update_tableset (fun tables ->
      Db_cache_types.TableSet.apply_recent tables ~deltas:delta.slice) db
  in
  Xapi_slave_db.slave_db := Database.set_generation delta.fresh_token newdb;
  objects


let write_db_to_disk =
  if !Xapi_globs.slave_dbs then
    begin
      debug "Writing slave db to slave disk";
      Db_cache_impl.sync (Db_conn_store.read_db_connections ()) !Xapi_slave_db.slave_db
    end

let start_stunnel_connection () =
  let addr = Pool_role.get_master_address() in
  let _psec = !Xapi_globs.pool_secret in
  debug "Starting master connection watchdog";
  Slave_backup_master_connection.get_master_address := Pool_role.get_master_address;
  Slave_backup_master_connection.master_rpc_path := Constants.database_backup_uri;
  Slave_backup_master_connection.on_database_connection_established := (fun () -> Slave_backup_master_connection.get_master_address := Pool_role.get_master_address);
  Slave_backup_master_connection.start_connection addr;
  Slave_backup_master_connection.open_secure_connection ()

let loop ~__context () =
  let delay_seconds = 300.0 in
  let start = Mtime_clock.counter () in
  let token_str = ref "-2" in
  let psec = Xapi_globs.pool_secret in
  let addr = ref (Pool_role.get_master_address()) in
  while (true) do
    let new_master_address = Pool_role.get_master_address () in
    let new_pool_secret = !Xapi_globs.pool_secret in
    if new_master_address <> !addr || new_pool_secret <> !psec then begin
      addr := new_master_address;
      psec := new_pool_secret;
      start_stunnel_connection ();
    end;
    let now = Mtime_clock.count start in

    let changes_result =
      !token_str
      |> Slave_backup_master_connection.execute_remote_fn
      |> Jsonrpc.of_string
      |> Rpcmarshal.unmarshal delta.Rpc.Types.ty
    in

    let changes =
      match changes_result with
      | Ok changes -> changes
      | Error (`Msg m) -> failwith m
    in
  
    let objs = Mutex.execute Xapi_slave_db.slave_db_mutex (fun () -> apply_changes changes) in
    List.iter (fun (tblname,objref)-> debug "event trigger: mod on %s/%s" tblname objref; Db_action_helper.events_notify tblname "mod" objref) objs;
    if Mtime.Span.to_s now > delay_seconds then write_db_to_disk;
    if changes.last_event_token > (get_gen !Xapi_slave_db.slave_db) then begin
      token_str := Int64.to_string (get_gen !Xapi_slave_db.slave_db);
    end else
      token_str := Int64.to_string changes.last_event_token
  done

let slave_db_backup_loop ~__context () =
  while (true) do
    try
      start_stunnel_connection ();
      debug "Starting slave db thread with clear db";
      Xapi_slave_db.clear_db ();
      loop __context ()
    with
      e ->
      Debug.log_backtrace e (Backtrace.get e);
      debug "Exception in Slave Database Backup Thread - %s" (Printexc.to_string e);
      Thread.delay 0.5;
  done

