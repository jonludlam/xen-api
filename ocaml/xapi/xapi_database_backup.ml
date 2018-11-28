module D=Debug.Make(struct let name="xapi_database_backup" end)
open D

open Http
open Xmlrpc_client
open Db_cache_types
open Stdext
open Threadext

type stat =
  { created :  int64
  ; modified : int64
  ; deleted :  int64
  } [@@deriving rpc]

type field =
  { fldname : string
  ; stat :    stat
  ; value :   Schema.Value.t
  } [@@deriving rpc]

type row =
  { objref : string
  ; stat :   stat
  ; fields :  field list
  } [@@deriving rpc]

type table =
  { tblname : string
  ; stat :    stat
  ; rows :   row list
  } [@@deriving rpc]

type del_tables =
  { key:   string
  ; table: string
  } [@@deriving rpc]

type delta =
  { fresh_token: int64
  ; last_event_token: int64
  ; tables :     table list
  ; deletes :    del_tables list
  ; counts : (string * int) list
  } [@@deriving rpc]

type update =
  { stat:stat
  ; tblname:string
  ; objref:string
  ; fldname:string
  ; value:Schema.Value.t
  }

exception Content_length_required
let delta_to_string (d:delta) = Jsonrpc.to_string (rpc_of_delta d)

let get_gen db =
  db
  |> Database.manifest
  |> Manifest.generation

(* Master Functions *)

let get_table_list db =
  TableSet.fold_over_recent (-2L) (fun table stat value acc -> table::acc) (Database.tableset db) []

let get_del_from_table_name token ts table =
  Table.fold_over_deleted token (fun key _ acc -> ({key; table})::acc) (TableSet.find table ts) []

let get_deleted ~token db =
  List.concat (List.rev_map (get_del_from_table_name token (Database.tableset db)) (get_table_list db))

let check_for_updates token db =
  let ts = Database.tableset db in
  let handle_obj obj = Row.fold_over_recent token (fun fldname {created; modified; deleted} value acc ->
      {fldname; stat={created;modified;deleted}; value=value}::acc) obj [] in
  let handle_table table = Table.fold_over_recent token (fun objref {created; modified; deleted} value acc ->
      {objref; stat={created;modified;deleted}; fields=(handle_obj value)}::acc) table [] in
  TableSet.fold_over_recent token (fun tblname {created; modified; deleted} value acc ->
      {tblname; stat={created;modified;deleted}; rows=(handle_table value)}::acc) ts []

let counter db table =
  (table, (Table.fold (fun _ _ _ acc -> succ(acc)) (TableSet.find table (Database.tableset db)) 0))

let object_count db =
  List.rev_map (counter db) (get_table_list db)

let backup_condition = Condition.create ()

let get_delta __context token =
  Mutex.execute Db_lock.dbcache_mutex (fun () ->
      let rec get () =
        let db = Db_ref.get_database (Context.database_of __context) in
        let tables = check_for_updates token db in
        let deleted = get_deleted token db in
        if tables=[] && deleted=[] then begin
          Condition.wait backup_condition Db_lock.dbcache_mutex;
          get ()
        end else
          (db,tables,deleted)
      in
      let (db,tables,deleted) = get () in
      let t = Manifest.generation (Database.manifest db) in
      {fresh_token=t;
       last_event_token=Int64.pred t;
       tables=tables;
       deletes=deleted;
       counts=object_count db})

let database_callback update t =
  Db_lock.with_lock (fun () ->
      Condition.broadcast backup_condition)

let handler (req: Request.t) s _ =
  try
    let token = Generation.of_string (snd (List.find (fun (x,y) -> x = "token") req.query)) in
    Server_helpers.exec_with_new_task "Database backup" ~task_in_database:false (fun __context ->
        token
        |> get_delta __context
        |> rpc_of_delta
        |> Jsonrpc.to_string
        |> Http_svr.response_str req s
      )
  with Not_found -> debug "No valid token found in request"

(* Slave Functions *)

let apply time tblname objref fldname newval =
  (fun _ -> newval)
  |> Row.update time fldname newval
  |> Table.update time objref Row.empty
  |> TableSet.update time tblname Table.empty
  |> Database.update

let make_from_barrier stat tblname objref db =
  db
  |> (
    Table.touch stat.modified objref Row.empty
    |> TableSet.update stat.modified tblname Table.empty
    |> Database.update
  )
  |> Database.increment

let make_empty_table stat tblname =
  (fun t -> t)
  |> TableSet.update stat.created tblname Table.empty
  |> Database.update

let make_change stat tblname objref fldname newval db =
  if TableSet.mem tblname (Database.tableset db) then begin
    if (Table.mem objref (TableSet.find tblname (Database.tableset db))) then begin
      if (Row.mem fldname (Table.find objref (TableSet.find tblname (Database.tableset db)))) then
        apply stat.modified tblname objref fldname newval db
      else
        begin
          if stat.created = stat.modified then
            apply stat.created tblname objref fldname newval db
          else
            apply stat.modified tblname objref fldname newval (apply stat.created tblname objref fldname (Schema.Value.String "XXX") db)
        end
    end
    else
      begin
        if stat.created = stat.modified then
          apply stat.created tblname objref fldname newval db
        else
          apply stat.modified tblname objref fldname newval (apply stat.created tblname objref fldname (Schema.Value.String (tblname^objref)) db)
      end
  end
  else
    apply stat.modified tblname objref fldname newval (make_empty_table {stat with created=(-1L);} tblname db)


let compare_by_tblname (c1: (string * int)) (c2: (string * int)) =
  String.compare (fst c1) (fst c2)

let count_check (c1_list: (string * int) list) (c2_list: (string * int) list) =
  let c1_list = List.sort compare_by_tblname c1_list in
  let c2_list = List.sort compare_by_tblname c2_list in
  try
    List.for_all2 (fun (a,b) (c,d) -> a=c && b=d) c1_list c2_list
  with invalid_arg -> false

let compare_by_modified u1 u2 =
  if u1.stat.modified = u2.stat.modified then
    Int64.compare u1.stat.created u2.stat.created
  else
    Int64.compare u1.stat.modified u2.stat.modified

let f_field u (field:field) =
  {u with
   stat = field.stat;
   fldname = field.fldname;
   value = field.value;}

let f_row u (row:row) =
  let u = {u with
           stat = {row.stat with modified = (max row.stat.modified u.stat.modified)};
           objref = row.objref;} in
  match row.fields with
  | [] -> [u]
  | _ -> List.rev_map (f_field u) row.fields

let f_table (table:table) =
  let u = {stat = table.stat;
           tblname = table.tblname;
           objref = "";
           fldname = "";
           value = (Schema.Value.String "");
          } in
  match table.rows with
  | [] -> [u]
  | _ -> List.concat (List.rev_map (f_row u) table.rows)

let f_all table_list =
  List.sort compare_by_modified (List.concat (List.rev_map f_table table_list))


let make_change_with_db_reply db (update:update) =
  match update.objref with
  | "" -> make_empty_table update.stat update.tblname db
  | _ -> (match update.fldname with
      | "" -> make_from_barrier update.stat update.tblname update.objref db
      | _ -> make_change update.stat update.tblname update.objref update.fldname update.value db)

let make_delete_with_db_reply db delete =
  remove_row delete.table delete.key db

let modified_check db tblname =
  let _t = TableSet.find tblname (Database.tableset db) in
  Table.fold (fun name tstat value ->
      (fun (b:bool) ->
         fst ((
             (Row.fold (fun ref rowstat v ->
                  (fun ((b:bool), (s:Db_cache_types.Stat.t)) ->
                     (b && s.modified >= rowstat.modified, s)
                  )))
               value)
              (true, tstat)))) _t true

let created_check db tblname =
  let _t = TableSet.find tblname (Database.tableset db) in
  Table.fold (fun name tstat value ->
      (fun (b:bool) ->
         fst ((
             (Row.fold (fun ref rowstat v ->
                  (fun ((b:bool), (s:Db_cache_types.Stat.t)) ->
                     (b && s.created <= rowstat.created, s)
                  )))
               value)
              (true, tstat)))) _t true

let modified_time_stamp_sanity db =
  List.for_all (modified_check db) (get_table_list db)

let created_time_stamp_sanity db =
  List.for_all (created_check db) (get_table_list db)

let apply_changes (delta:delta) =
  if (Manifest.generation (Database.manifest !Xapi_slave_db.slave_db)) < delta.fresh_token then
    begin
      let new_db =
        !Xapi_slave_db.slave_db
        |> (fun db -> List.fold_left (make_change_with_db_reply) db (f_all delta.tables))
        |> (fun db -> List.fold_left (make_delete_with_db_reply) db delta.deletes)
        |> Database.set_generation delta.fresh_token
      in
      Xapi_slave_db.slave_db := new_db;
    end;
  (* Need to check that we haven't missed anything *)
  let slave_counts = object_count !Xapi_slave_db.slave_db in
  if not (count_check slave_counts delta.counts) then
    begin
      debug "The local database and the master database do not appear to be the same size - clearing slave db";
      Xapi_slave_db.clear_db ()
    end;
  if not (created_time_stamp_sanity !Xapi_slave_db.slave_db && modified_time_stamp_sanity !Xapi_slave_db.slave_db) then begin
    debug "The stat fields are not correct - clearing slave db";
    Xapi_slave_db.clear_db ()
  end

let send_notification __context token (u:update) =
  let __context = Xapi_slave_db.update_context_db __context in
  let reasons = ref [] in
  if token < u.stat.created && u.stat.created < u.stat.modified then
    reasons := ["add"; "mod"];
  if token < u.stat.created && u.stat.created = u.stat.modified then
    reasons := ["add"];
  if u.stat.created < token && token < u.stat.modified then
    reasons := ["mod"];
  if u.stat.modified = u.stat.deleted then
    reasons := [];

  List.iter (fun r ->
      if u.tblname = "session" then () else debug "RWD: sendning %s notification for %s/%s" r u.tblname u.objref;
      Db_action_helper.events_notify u.tblname r u.objref;
    ) !reasons

let handle_notifications __context token (master_delta:delta) =
  let dels = ref [] in
  Mutex.execute Xapi_slave_db.slave_db_mutex (fun () ->
      dels := get_deleted token !Xapi_slave_db.slave_db;
    );
  List.iter (fun (d:del_tables) -> Db_action_helper.events_notify d.table "del" d.key) !dels;
  List.iter (send_notification __context token) (f_all master_delta.tables)

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

    let changes =
      !token_str
      |> Slave_backup_master_connection.execute_remote_fn
      |> Jsonrpc.of_string
      |> delta_of_rpc
    in

    Mutex.execute Xapi_slave_db.slave_db_mutex (fun () -> apply_changes changes);
    handle_notifications __context (Int64.of_string !token_str) changes;
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

