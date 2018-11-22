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

let get_delta __context token =
  let db = Db_ref.get_database (Context.database_of __context) in
  let token_str = (Int64.to_string token) ^ "," ^ (Int64.to_string token) in
  let _e = Xapi_event.from __context ["*"] token_str 10. |> Event_types.parse_event_from in
  let _ = Xapi_event.from __context ["*"] _e.token 10. in
  Mutex.execute Db_lock.dbcache_mutex (fun () ->
      let t = Manifest.generation (Database.manifest db) in
      {fresh_token=t;
       last_event_token=Int64.pred t;
       tables=(check_for_updates token db);
       deletes=(get_deleted token db);
       counts=(object_count db)})

let handler (req: Request.t) s _ =
  req.Http.Request.close <- true;
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

let make_empty_table stat tblname =
  (fun t -> t)
  |> TableSet.update stat.created tblname Table.empty
  |> Database.update

let make_change stat tblname objref fldname newval db =
  if TableSet.mem tblname (Database.tableset db) then begin
    if (Table.mem objref (TableSet.find tblname (Database.tableset db))) then
      begin
        if (Row.mem fldname (Table.find objref (TableSet.find tblname (Database.tableset db)))) then
          apply stat.modified tblname objref fldname newval db
        else
          begin
            if stat.created = stat.modified then
              apply stat.created tblname objref fldname newval db
            else begin
              apply stat.created tblname objref fldname (Schema.Value.String "XXX") db
              |> apply stat.modified tblname objref fldname newval

            end
          end
      end
    else
      begin
        if stat.created = stat.modified then
          apply stat.created tblname objref fldname newval db
        else
          begin
            apply stat.created tblname objref fldname (Schema.Value.String (tblname^objref)) db
            |> apply stat.modified tblname objref fldname newval
          end
      end
  end
  else
    begin
      make_empty_table {stat with created=(-1L);} tblname db
      |> apply stat.modified tblname objref fldname newval
    end

let count_check (c1_list: (string * int) list) (c2_list: (string * int) list) =
  try
    List.for_all2 (fun (a,b) (c,d) -> a=c && b=d) c1_list c2_list
  with invalid_arg -> false

let compare_by_modified u1 u2 =
  if u1.stat.modified = u2.stat.modified then
    Int64.compare u1.stat.created u2.stat.created
  else
    Int64.compare u1.stat.modified u2.stat.modified

let flatten_one_field u (f:field) =
  {u with
   stat = f.stat;
   fldname = f.fldname;
   value = f.value;}

let flatten_one_row u (r:row) =
  List.map (flatten_one_field {u with objref=r.objref; stat = r.stat;}) r.fields

let flatten_one_table (t:table) =
  let u = {tblname=t.tblname; stat=t.stat; objref = ""; fldname = ""; value = (Schema.Value.String "")} in
  match t.rows with
    [] -> [u]
  | _ -> List.concat (List.map (flatten_one_row u) t.rows)

let flatten tl =
  List.sort compare_by_modified (List.concat (List.map flatten_one_table tl))

let row_to_notification (u:update) (r:row) =
  {u with objref=r.objref; stat = r.stat;}

let extract (t:table) =
  let u = {tblname=t.tblname; stat=t.stat; objref = ""; fldname = ""; value = (Schema.Value.String "")} in
  match t.rows with
    [] -> [u]
  | _ -> List.map (row_to_notification u) t.rows

let table_list_to_notifications tl =
  List.sort compare_by_modified (List.concat (List.map extract tl))


let make_change_with_db_reply db (update:update) =
  match update.fldname with
  | "" -> make_empty_table update.stat update.tblname db
  | _ -> make_change update.stat update.tblname update.objref update.fldname update.value db

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
        |> Database.set_generation delta.fresh_token
        |> (fun db -> List.fold_left (make_change_with_db_reply) db (flatten delta.tables))
        |> (fun db -> List.fold_left (make_delete_with_db_reply) db delta.deletes)
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
  if u.stat.created = u.stat.modified then
    reasons := ["add"];
  if token <= u.stat.created && u.stat.created < u.stat.modified then begin
    (* Created, then modified *)
    reasons := ["add"; "mod";];
  end
  else
    reasons := ["mod"];
  if u.stat.modified = u.stat.deleted then
    reasons := [];
  if u.fldname = "" then () else
    List.iter (fun r ->
                  debug "RWD: sendning %s notification for %s/%s" r u.tblname u.objref;
                    Db_action_helper.events_notify u.tblname r u.objref;
                ) !reasons

let handle_notifications __context token (master_delta:delta) =
  let dels = ref [] in
  Mutex.execute Xapi_slave_db.slave_db_mutex (fun () ->
      dels := get_deleted token !Xapi_slave_db.slave_db;
    );
  List.iter (fun (d:del_tables) -> Db_action_helper.events_notify d.table "del" d.key) !dels;
  List.iter (send_notification __context token) (table_list_to_notifications master_delta.tables)

let write_db_to_disk =
  if !Xapi_globs.slave_dbs then
    begin
      debug "Writing slave db to slave disk";
      Db_cache_impl.sync (Db_conn_store.read_db_connections ()) !Xapi_slave_db.slave_db
    end

let loop ~__context () =
  let uri = Constants.database_backup_uri in
  let delay_seconds = 300.0 in
  let start = Mtime_clock.counter () in
  let token_str = ref "-2" in
  while (true) do
    let now = Mtime_clock.count start in
    let addr = Pool_role.get_master_address() in
    let psec = !Xapi_globs.pool_secret in
    let req = Xapi_http.http_request ~cookie:["pool_secret", psec] ~query:[("token", !token_str)] Http.Get uri in
    let transport = SSL(SSL.make (), addr, !Xapi_globs.https_port) in
    let changes = with_transport transport
        (with_http req
           (fun (response, fd) ->
              let res = match response.Http.Response.content_length with
                | None -> raise Content_length_required
                | Some l -> Xapi_stdext_unix.Unixext.really_read_string fd (Int64.to_int l)
              in
              delta_of_rpc (Jsonrpc.of_string res)
           )
        ) in
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
      debug "Starting slave db thread with clear db";
      Xapi_slave_db.clear_db ();
      loop __context ()
    with
      e ->
      Debug.log_backtrace e (Backtrace.get e);
      debug "Exception in Slave Database Backup Thread - %s" (Printexc.to_string e);
      Thread.delay 0.5;
  done

