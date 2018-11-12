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

type count =
  { tblname:string
  ; count:  int
  } [@@deriving rpc]

type delta =
  { fresh_token: int64
  ; last_event_token: int64
  ; tables :     table list
  ; deletes :    del_tables list
  ; counts : count list
  } [@@deriving rpc]

type edits =
  { ts :     TableSet.t
  ; tables : string list
  }

type pair =
  { tbl: string
  ; row: string
  ; rowstat:stat
  }

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
  |> Db_cache_types.Database.manifest
  |> Db_cache_types.Manifest.generation

(* Master Functions *)

let get_del_from_table_name token ts table =
  Table.fold_over_deleted token (fun key _ acc ->
      ({key; table})::acc) (TableSet.find table ts) []

let get_deleted ~token db =
  let objs = Dm_api.objects_of_api Datamodel.all_api in
  let _tables = List.rev_map (fun x->x.Datamodel_types.name) objs in
  List.concat (List.rev_map (get_del_from_table_name token (Database.tableset db)) _tables)

let check_for_updates token db =
  let ts = Database.tableset db in
  let handle_obj obj = Row.fold_over_recent token (fun fldname {created; modified; deleted} value acc ->
      {fldname; stat={created;modified;deleted}; value=value}::acc) obj [] in
  let handle_table table = Table.fold_over_recent token (fun objref {created; modified; deleted} value acc ->
      {objref; stat={created;modified;deleted}; fields=(handle_obj value)}::acc) table [] in
  TableSet.fold_over_recent token (fun tblname {created; modified; deleted} value acc ->
      {tblname; stat={created;modified;deleted}; rows=(handle_table value)}::acc) ts []

let get_table_list db =
  Db_cache_types.TableSet.fold_over_recent (-2L) (fun table stat value acc -> table::acc) (Db_cache_types.Database.tableset db) []

let counter db table =
  let t = Db_cache_types.TableSet.find table (Db_cache_types.Database.tableset db) in
  {tblname=table; count=Db_cache_types.Table.fold (fun _ _ _ acc -> succ(acc)) t 0;}

let object_count db =
  List.rev_map (counter db) (get_table_list db)

let get_delta __context token =
  let db = Db_ref.get_database (Context.database_of __context) in
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
    Xapi_http.with_context "Database backup" req s (fun __context ->
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

let make_change __context stat tblname objref fldname newval db =
  let record = (Eventgen.find_get_record tblname ~__context ~self:objref) () in
  let return_db = ref db in
  begin match record with
    | None ->
      (* add, or add + mod *)
      return_db := apply stat.created tblname objref fldname newval db;
      if stat.created < stat.modified then
        return_db := apply stat.modified tblname objref fldname newval !return_db
    | Some _ ->
      return_db := apply stat.created tblname objref fldname newval db
  end;
  !return_db

let make_empty_table stat tblname =
  (fun t -> t)
  |> TableSet.update stat.created tblname Table.empty
  |> Database.update

let count_eq_check (c1:count) (c2:count) =
  c1.tblname = c2.tblname && c1.count = c2.count

let count_check (c1_list: count list) (c2_list: count list) =
  try
    List.for_all2 count_eq_check c1_list c2_list
  with invalid_arg -> false

let flat_row_list update (rlist:row list) =
  match rlist with
  | [] -> [update]
  | _ -> List.fold_left (fun acc (r:row) ->
      (List.rev_map (fun (f:field) ->
           {update with stat=f.stat; objref=r.objref; fldname=f.fldname; value=f.value;}) r.fields) @ acc) [] rlist

let flatten_table_to_update (tbllist:table list) =
  List.concat (List.rev_map (fun (t:table) ->
      (flat_row_list ({stat=t.stat; tblname=t.tblname; objref=""; fldname=""; value=Schema.Value.String "";})) t.rows) tbllist)

let make_change_with_db_reply __context db (update:update) =
  match update.fldname with
  | "" -> make_empty_table update.stat update.tblname db
  | _ -> make_change __context update.stat update.tblname update.objref update.fldname update.value db

let make_delete_with_db_reply db delete =
  Db_cache_types.remove_row delete.table delete.key db

let apply_changes __context (delta:delta) =
  if (Db_cache_types.Manifest.generation (Db_cache_types.Database.manifest !Xapi_slave_db.slave_db)) < delta.fresh_token then
    begin
      let new_db =
        !Xapi_slave_db.slave_db
        |> Database.set_generation delta.fresh_token
        |> (fun db -> List.fold_left (make_change_with_db_reply __context) db (flatten_table_to_update delta.tables))
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
    end

let fold_row tblname (rlist:row list) =
  List.fold_left (fun acc (r:row) -> [{tbl=tblname; row=r.objref; rowstat=r.stat}] @ acc) [] rlist

let info_extract (tlist:table list) =
  List.fold_left (fun pl (t:table) -> (fold_row t.tblname t.rows) @ pl) [] tlist

let send_notification __context token p =
  let __context = Xapi_slave_db.update_context_db __context in
  let reasons = ref [] in
  if p.rowstat.created = p.rowstat.modified then
    reasons := ["add"];
  if token < p.rowstat.created && p.rowstat.created < p.rowstat.modified then begin
    (* Created, then modified *)
    reasons := ["add"; "mod";];
  end
  else
    reasons := ["mod"];

  List.iter (fun r -> let record = (Eventgen.find_get_record p.tbl ~__context ~self:p.row) () in
              begin match record with
                | None -> debug "Could not send %s event for %s in %s table" r p.row p.tbl;
                | Some record -> Db_action_helper.events_notify ~snapshot:record p.tbl r p.row;
              end) !reasons

let handle_notifications __context token =
  let db = !Xapi_slave_db.slave_db in
  let deletes = get_deleted token db in
  let tables = check_for_updates token db in

  List.iter (fun (d:del_tables) -> Db_action_helper.events_notify d.table "del" d.key) deletes;
  List.iter (send_notification __context token) (info_extract tables)

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
    Mutex.execute Xapi_slave_db.slave_db_mutex (fun () -> apply_changes __context changes);
    Mutex.execute Xapi_slave_db.slave_db_mutex (fun () -> handle_notifications __context (Int64.of_string!token_str));
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

