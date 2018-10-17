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
  ; stat : stat
  } [@@deriving rpc]

type delta =
  { fresh_token: int64
  ; tables :     table list
  ; deletes :    del_tables list
  } [@@deriving rpc]

type edits =
  { ts :     TableSet.t
  ; tables : string list
  }

type count =
  { tblname:string
  ; count:  int
  ; del:    int
  }

type pair =
  { tbl: string
  ; row: string
  }

type update =
  { mtime: int64
  ; tblname:string
  ; objref:string
  ; fldname:string
  ; value:Schema.Value.t
  }

exception Content_length_required

let delta_to_string (d:delta) = Jsonrpc.to_string (rpc_of_delta d)

(* Master Functions *)

let get_del_from_table_name token ts table =
  Table.fold_over_deleted token (fun key {created; modified; deleted} acc ->
      ({key; table; stat={created;modified;deleted}})::acc) (TableSet.find table ts) []

let get_deleted ~token db =
  let tables =
    let all =
      let objs = List.filter (fun x -> x.Datamodel_types.gen_events) (Dm_api.objects_of_api Datamodel.all_api) in
      List.rev_map (fun x->x.Datamodel_types.name) objs in
    List.filter (fun table -> true) all in
  List.concat (List.rev_map (get_del_from_table_name token (Database.tableset db)) tables)

let check_for_updates token db =
  let ts = Database.tableset db in
  let handle_obj obj = Row.fold_over_recent token (fun fldname {created; modified; deleted} value acc
                                                    -> {fldname; stat={created;modified;deleted}; value=value}::acc) obj [] in
  let handle_table table = Table.fold_over_recent token (fun objref {created; modified; deleted} value acc
                                                          -> {objref; stat={created;modified;deleted}; fields=(handle_obj value)}::acc) table [] in
  TableSet.fold_over_recent token (fun tblname {created; modified; deleted} value acc
                                    -> {tblname; stat={created;modified;deleted}; rows=(handle_table value)}::acc) ts []

let get_delta __context token =
  let db = Db_ref.get_database (Context.database_of __context) in
  Mutex.execute Db_lock.dbcache_mutex (fun () ->
      {fresh_token=Manifest.generation (Database.manifest db);
       tables=(check_for_updates token db);
       deletes=(get_deleted token db)})

let handler (req: Request.t) s _ =
  req.Http.Request.close <- true;
  try
    let token = Generation.of_string (snd (List.find (fun (x,y) -> x = "token") req.query)) in
    Xapi_http.with_context "Database backup" req s (fun __context ->
        (token:Db_cache_types.Time.t)
        |> get_delta __context
        |> rpc_of_delta
        |> Jsonrpc.to_string
        |> Http_svr.response_str req s
      )
  with Not_found -> debug "No valid token found in request"

(* Slave Functions *)

let send_notification (c:Context.t) reason p =
  let record = (Eventgen.find_get_record p.tbl ~__context:c ~self:p.row) () in
  begin match record with
    | None -> debug "Could not send %s event for %s in %s table" reason p.row p.tbl;
    | Some record -> Db_action_helper.events_notify ~snapshot:record p.tbl reason p.row;
  end

let make_change mtime tblname objref fldname newval =
  Database.update (
    (fun _ -> newval)
    |> Row.update mtime fldname newval
    |> Table.update mtime objref Row.empty
    |> TableSet.update mtime tblname Table.empty)

let make_empty_table mtime tblname =
  Database.update (
    (fun _ -> Table.empty)
    |> TableSet.update mtime tblname Table.empty)

let get_table_list db =
  Db_cache_types.TableSet.fold_over_recent (-2L) (fun table stat value acc -> table::acc) (Db_cache_types.Database.tableset db) []

let counter db table =
  let t = Db_cache_types.TableSet.find table (Db_cache_types.Database.tableset db) in
  {tblname=table; count=0; del=(Db_cache_types.Table.get_deleted_len t)}

let object_count db =
  List.rev_map (counter db) (get_table_list db)

let count_eq_check (c1:count) (c2:count) =
  c1.tblname = c2.tblname && c1.count = c2.count && c1.del = c2.del

let count_check (c1_list: count list) (c2_list: count list) =
    List.for_all2 count_eq_check c1_list c2_list

let flat_row_list update (rlist:row list) =
  match rlist with
  | [] -> [update]
  | _ -> List.fold_left (fun acc (r:row) ->
      (List.rev_map (fun (f:field) ->
           {update with mtime=f.stat.modified; objref=r.objref; fldname=f.fldname; value=f.value;}) r.fields) @ acc) [] rlist

let flatten_table_to_update (tbllist:table list) =
  List.concat (List.rev_map (fun (t:table) ->
      (flat_row_list ({mtime=t.stat.modified; tblname=t.tblname; objref=""; fldname=""; value=Schema.Value.String "";})) t.rows) tbllist)

let make_change_with_db_reply db (update:update) =
  match update.fldname with
  | "" -> make_empty_table update.mtime update.tblname db
  | _ -> make_change update.mtime update.tblname update.objref update.fldname update.value db

let make_delete_with_db_reply __context db delete =
  if Db_cache_impl.is_valid_ref (Context.database_of __context) delete.key then
    Db_cache_types.remove_row delete.table delete.key db
  else
    begin
      debug "Not valid ref";
      db
    end

let apply_changes (delta:delta) __context =
  if (Db_cache_types.Manifest.generation (Db_cache_types.Database.manifest !Xapi_slave_db.slave_db)) < delta.fresh_token then
    begin
      let new_db =
        !Xapi_slave_db.slave_db
        |> Database.set_generation delta.fresh_token
        |> (fun db -> List.fold_left make_change_with_db_reply db (flatten_table_to_update delta.tables))
        |> (fun db -> List.fold_left (make_delete_with_db_reply __context) db delta.deletes)
      in
      Xapi_slave_db.slave_db := new_db;
    end;
  (* Need to check that we haven't missed anything *)
  if not (count_check (object_count !Xapi_slave_db.slave_db) (object_count (Db_ref.get_database (Context.database_of __context)))) then
    Xapi_slave_db.clear_db ()

let fold_row tblname (rlist:row list) =
  List.fold_left (fun acc (r:row) -> [{tbl=tblname; row=r.objref}] @ acc) [] rlist

let info_extract (tlist:table list) =
  List.fold_left (fun pl (t:table) -> (fold_row t.tblname t.rows) @ pl) [] tlist

let handle_notifications __context (d:delta) =
  List.iter ((fun __context (d:del_tables) -> send_notification __context "del" {tbl=d.table; row=d.key}) __context) d.deletes;
  List.iter (send_notification __context "mod") (info_extract d.tables)

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
  while (true) do
    let now = Mtime_clock.count start in
    let addr = Pool_role.get_master_address() in
    let psec = !Xapi_globs.pool_secret in
    let token_str = Int64.to_string (Manifest.generation (Database.manifest !Xapi_slave_db.slave_db)) in
    let req = Xapi_http.http_request ~cookie:["pool_secret", psec] ~query:[("token", token_str)] Http.Get uri in
    let transport = SSL(SSL.make (), addr, !Xapi_globs.https_port) in
    with_transport transport
      (with_http req
         (fun (response, fd) ->
            let res = match response.Http.Response.content_length with
              | None -> raise Content_length_required
              | Some l -> Xapi_stdext_unix.Unixext.really_read_string fd (Int64.to_int l)
            in
            let delta_change = delta_of_rpc (Jsonrpc.of_string res) in
            Mutex.execute Xapi_slave_db.slave_db_mutex (fun () -> apply_changes delta_change __context);
            handle_notifications __context delta_change;
            if Mtime.Span.to_s now > delay_seconds then write_db_to_disk;
         )
      )
  done

let slave_db_backup_loop ~__context () =
  while (true) do
    try
      loop __context ()
    with
      e -> debug "Exception in Slave Database Backup Thread - %s" (Printexc.to_string e); Thread.delay 0.5;
  done

