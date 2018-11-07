(*
 * Copyright (C) Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)

open Test_common
open Event_types

let check_db_equals dba dbb =
  let mf = Db_cache_types.Database.manifest in
  let _gen = Db_cache_types.Manifest.generation (mf dba) = Db_cache_types.Manifest.generation (mf dbb) in
  let _man = Db_cache_types.Database.manifest dba = Db_cache_types.Database.manifest dbb in
  let _ts = Db_cache_types.Database.tableset dba = Db_cache_types.Database.tableset dbb in
  _gen && _man && _ts

let get_tables db =
  Db_cache_types.TableSet.fold_over_recent (-2L) (fun table stat value acc -> table::acc) (Db_cache_types.Database.tableset db) []

let sort_and_flatten l =
  let sorted = List.sort compare (List.map (List.sort compare) l) in
  let flattened = List.sort compare (List.flatten sorted) in
  String.concat ", " flattened

let dump db =
  Db_cache_types.TableSet.iter (fun tblname table ->
      Printf.printf "\n# TABLE: %s\n\n" tblname;
      Db_cache_types.Table.iter (fun objref row ->
          Printf.printf "## Object: %s\n" objref;
          Db_cache_types.Row.iter (fun fldname v ->
              Printf.printf "  %s: %s\n" fldname (Schema.Value.marshal v)) row) table) (Db_cache_types.Database.tableset db)

let tbl_to_string (tbl:Db_cache_types.Table.t) =
  let row_func = Db_cache_types.Row.fold (fun str stat value acc -> (str ^ (Schema.Value.marshal value)) :: acc) in
  let lofl = Db_cache_types.Table.fold (fun str stat value acc -> (row_func value []) :: acc) tbl [] in
  sort_and_flatten lofl

let tbl_eq dba dbb tbl =
  let tbl_str_a = tbl_to_string (Db_cache_types.TableSet.find tbl (Db_cache_types.Database.tableset dba)) in
  let tbl_str_b = tbl_to_string (Db_cache_types.TableSet.find tbl (Db_cache_types.Database.tableset dbb)) in
  if tbl_str_a <> tbl_str_b then
    begin
      Printf.printf "\nThe %s tables are not equal" tbl;
      Printf.printf "\nTable a: '%s'" tbl_str_a;
      Printf.printf "\nTable b: '%s'\n" tbl_str_b;
      false
    end
  else
    true

let get_gen db =
  db
  |> Db_cache_types.Database.manifest
  |> Db_cache_types.Manifest.generation


let db_to_str db =
  let tbl_list = (get_tables db) in
  let tbl_t_list = List.map (fun tblname -> (Db_cache_types.TableSet.find tblname (Db_cache_types.Database.tableset db))) tbl_list in
  let tbl_as_str_list = List.map tbl_to_string tbl_t_list in
  String.concat ", " tbl_as_str_list

let print_db db =
  Printf.printf "DB: %s" (db_to_str db)

let dbs_are_equal dba dbb =
  List.for_all (fun x -> x) (List.map (tbl_eq dba dbb) (get_tables dba))

let test_db_backup () =
  Xapi_slave_db.clear_db ();

  let mf = Db_cache_types.Database.manifest in
  let gen = Db_cache_types.Manifest.generation in
  let __context = make_test_database () in
  let init_db = Db_ref.get_database (Context.database_of __context) in

  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let changes_db = !(Xapi_slave_db.slave_db) in

  let gen_init = gen (mf init_db) in
  let gen_changes = gen (mf changes_db) in
  Printf.printf "\nGeneration 1: %Li" gen_init;
  Printf.printf "\nGeneration 2: %Li\n" gen_changes;

  Alcotest.(check bool) "Test the equality check" true (dbs_are_equal init_db init_db);
  Alcotest.(check bool) "Database generations are equal" true (gen_init = gen_changes);
  Alcotest.(check bool) "Database tables are equal" true (dbs_are_equal init_db changes_db);

  Xapi_slave_db.clear_db ()

let test_db_with_vm () =
  Xapi_slave_db.clear_db ();

  let mf = Db_cache_types.Database.manifest in
  let gen = Db_cache_types.Manifest.generation in
  let __context = make_test_database () in

  let (_vm_ref: API.ref_VM) = make_vm __context () in

  let init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let changes_db = !(Xapi_slave_db.slave_db) in

  Printf.printf "Changes: %s" (Xapi_database_backup.delta_to_string changes);
  Alcotest.(check bool) "VM generation is the same" true (gen (mf init_db) = (gen (mf changes_db)));
  Alcotest.(check bool) "VM table updates are equal" true (dbs_are_equal init_db changes_db);

  Xapi_slave_db.clear_db ()

let test_db_with_name_change () =
  Xapi_slave_db.clear_db ();

  let mf = Db_cache_types.Database.manifest in
  let gen = Db_cache_types.Manifest.generation in
  let __context = make_test_database () in
  let vm = make_vm __context () in
  Db.VM.set_name_description __context vm "NewName";
  let init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let changes_db = !(Xapi_slave_db.slave_db) in

  Alcotest.(check bool) "changes are reflected in both tables' generations" true (gen (mf init_db) = (gen (mf changes_db)));
  Alcotest.(check bool) "All info in VM tables is correct" true (dbs_are_equal init_db changes_db);

  Xapi_slave_db.clear_db ()

let test_db_with_multiple_changes () =
  Xapi_slave_db.clear_db ();

  let mf = Db_cache_types.Database.manifest in
  let gen = Db_cache_types.Manifest.generation in
  let __context = make_test_database () in
  let vm = make_vm __context () in
  Db.VM.set_name_description __context vm "NewName1";
  let init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let changes_db = !(Xapi_slave_db.slave_db) in

  Alcotest.(check bool) "First vm created - generations equal" true (gen (mf init_db) = (gen (mf changes_db)));
  Alcotest.(check bool) "First vm created - tables correct" true (dbs_are_equal init_db changes_db);

  let vm2 = make_vm __context () in
  Db.VM.set_name_description __context vm2 "NewName2";
  let init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let changes_db = !(Xapi_slave_db.slave_db) in

  Alcotest.(check bool) "Second vm created - generations equal" true (gen (mf init_db) = (gen (mf changes_db)));
  Alcotest.(check bool) "Second vm created - tables correct" true (dbs_are_equal init_db changes_db);

  let vm3 = make_vm __context () in
  Db.VM.set_name_description __context vm3 "NewName3";
  let init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let changes_db = !(Xapi_slave_db.slave_db) in

  Alcotest.(check bool) "Third vm created - generations equal" true (gen (mf init_db) = (gen (mf changes_db)));
  Alcotest.(check bool) "Third vm created - tables correct" true (dbs_are_equal init_db changes_db);

  Xapi_slave_db.clear_db ()

let test_db_events () =
  (* Test that events on one object create the correct number of events on other objects *)
  Xapi_slave_db.clear_db ();

  let __context = make_test_database () in
  let vma = make_vm __context () in
  let _init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let _changes_db = !(Xapi_slave_db.slave_db) in

  let evs = Xapi_event.from __context ["vm"] "" 30.0 |> parse_event_from in
  let tok = evs.token in
  let vm_ev = List.filter (fun ev -> ev.ty="vm") evs.events in
  Alcotest.(check int) "Creation of: vm table; dom0 vm and test vma" 3 (List.length vm_ev);

  Db.VM.set_name_description __context vma "NewName1";

  let vmb = make_vm __context () in
  let evs2 = Xapi_event.from __context ["vm"] tok 30.0 |> parse_event_from in
  let tok2 = evs2.token in
  let vm_ev2 = List.filter (fun ev -> ev.ty="vm") evs2.events in
  Alcotest.(check int) "Rename vma and creation of test vmb" 2 (List.length vm_ev2);

  let vbd = make_vbd ~__context ~vM:vma () in
  let evs3 = Xapi_event.from __context ["vm"] tok2 30.0 |> parse_event_from in
  let tok3 = evs3.token in
  let vm_ev3 = List.filter (fun ev -> ev.ty="vm") evs3.events in
  Alcotest.(check int) "Creation of vbd for vma" 1 (List.length vm_ev3);

  Db.VBD.set_VM __context vbd vmb;
  let evs4 = Xapi_event.from __context ["vm"] tok3 30.0 |> parse_event_from in
  let vm_ev4 = List.filter (fun ev -> ev.ty="vm") evs4.events in
  Alcotest.(check int) "Change vbd from vma to vmb" 2 (List.length vm_ev4);
  Xapi_slave_db.clear_db ()

let test_db_events_through_slave_db () =
  (* Test that events on one object create the correct number of events on other objects *)
  Xapi_slave_db.clear_db ();

  let __context = make_test_database () in
  let vma = make_vm __context () in
  let _init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let _changes_db = !(Xapi_slave_db.slave_db) in

  let classes = ["vm"] in
  let timeout = 30.0 in
  let token = "" in

  let evs = Xapi_slave_db.call_with_updated_context __context (Xapi_event.from ~classes ~token ~timeout) |> parse_event_from in
  let tok = evs.token in
  let vm_ev = List.filter (fun ev -> ev.ty="vm") evs.events in
  Alcotest.(check int) "Creation of: vm table; dom0 vm and test vma" 3 (List.length vm_ev);

  Db.VM.set_name_description __context vma "NewName1";

  let _vmb = make_vm __context () in
  let token = tok in
  let evs2 = Xapi_slave_db.call_with_updated_context __context (Xapi_event.from ~classes ~token ~timeout) |> parse_event_from in
  let vm_ev2 = List.filter (fun ev -> ev.ty="vm") evs2.events in
  Alcotest.(check int) "Rename vma and creation of test vmb" 2 (List.length vm_ev2);
  Xapi_slave_db.clear_db ()

let test_db_events_without_session () =
  (* Test that events on one object create the correct number of events on other objects *)
  Xapi_slave_db.clear_db ();

  let __context = make_test_database () in
  let __context = Context.update_session_id None __context in
  let vma = make_vm __context () in
  let _init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let _changes_db = !(Xapi_slave_db.slave_db) in
  Alcotest.(check bool) "Check that we don't have a session" false (Context.has_session_id __context);

  let classes = ["vm"] in
  let timeout = 30.0 in
  let token = "" in

  let evs = Xapi_slave_db.call_with_updated_context __context (Xapi_event.from ~classes ~token ~timeout) |> parse_event_from in
  let tok = evs.token in
  let vm_ev = List.filter (fun ev -> ev.ty="vm") evs.events in
  Alcotest.(check int) "Creation of 2 vms without session (vm table; vma; vmb)" 3 (List.length vm_ev);

  Db.VM.set_name_description __context vma "NewName1";

  let _vmb = make_vm __context () in
  let token = tok in
  let evs2 = Xapi_slave_db.call_with_updated_context __context (Xapi_event.from ~classes ~token ~timeout) |> parse_event_from in
  let vm_ev2 = List.filter (fun ev -> ev.ty="vm") evs2.events in
  Alcotest.(check int) "Rename vm and make new vm" 2 (List.length vm_ev2);
  Xapi_slave_db.clear_db ()

let test_db_events_with_session () =
  Xapi_slave_db.clear_db ();
  let __context = make_test_database () in
  let rpc, session_id = Test_common.make_client_params ~__context in
  let __context = Context.update_session_id (Some session_id) __context in
  Alcotest.(check bool) "Check that we have a session" true (Context.has_session_id __context);

  let vma = make_vm __context () in
  let _init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let _changes_db = !(Xapi_slave_db.slave_db) in

  let classes = ["vm"] in
  let timeout = 30.0 in
  let token = "" in

  let evs = Xapi_slave_db.call_with_updated_context __context ~session_id:(Some session_id)
      (Xapi_event.from ~classes ~token ~timeout) |> parse_event_from in
  let tok = evs.token in
  let vm_ev = List.filter (fun ev -> ev.ty="vm") evs.events in
  Alcotest.(check int) "Creation of 2 vms with session (vm table; vma; vmb)" 3 (List.length vm_ev);

  Db.VM.set_name_description __context vma "NewName1";

  let _vmb = make_vm __context () in
  let token = tok in
  let evs2 = Xapi_slave_db.call_with_updated_context __context ~session_id:(Some session_id)
      (Xapi_event.from ~classes ~token ~timeout) |> parse_event_from in
  let vm_ev2 = List.filter (fun ev -> ev.ty="vm") evs2.events in
  Alcotest.(check int) "Rename vm and make new vm" 2 (List.length vm_ev2);
  Xapi_slave_db.clear_db ()

let test_db_counts () =
  Xapi_slave_db.clear_db ();
  let mf = Db_cache_types.Database.manifest in
  let gen = Db_cache_types.Manifest.generation in
  let __context = make_test_database () in

  let _vma = make_vm ~__context ~name_label:"vma" () in

  let _init_db = Db_ref.get_database (Context.database_of __context) in

  let changes = Xapi_database_backup.get_delta __context (-2L) in
  Xapi_database_backup.apply_changes changes;
  let _changes_db = !(Xapi_slave_db.slave_db) in
  let token = get_gen _changes_db in

  Alcotest.(check bool) "Created vms - object count equal" true
    (Xapi_database_backup.count_check (Xapi_database_backup.object_count _init_db) (Xapi_database_backup.object_count _changes_db));
  Alcotest.(check bool) "Vms created - generations equal" true (gen (mf _init_db) = (gen (mf _changes_db)));
  Alcotest.(check bool) "Vms created - tables correct" true (dbs_are_equal _init_db _changes_db);

  Db.VM.destroy ~__context ~self:_vma;
  let _init_db = Db_ref.get_database (Context.database_of __context) in

  Alcotest.(check bool) "Database generation has been incremented" false ((get_gen _init_db) = token);

  let changes = Xapi_database_backup.get_delta __context token in
  Xapi_database_backup.apply_changes changes;
  let _changes_db = !(Xapi_slave_db.slave_db) in
  let _init_db = Db_ref.get_database (Context.database_of __context) in

  Alcotest.(check bool) "Database tables are equal" true (dbs_are_equal _init_db _changes_db);
  Alcotest.(check bool) "Deleted vms - object count equal" true
    (Xapi_database_backup.count_check (Xapi_database_backup.object_count _init_db) (Xapi_database_backup.object_count _changes_db));

  Xapi_slave_db.clear_db ()

let test =
  [
    "test_db_backup", `Quick, test_db_backup;
    "test_db_with_vm", `Quick, test_db_with_vm;
    "test_db_with_name_change", `Quick,  test_db_with_name_change;
    "test_db_with_multiple_changes", `Quick,  test_db_with_multiple_changes;
    "test_db_events", `Quick, test_db_events;
    "test_db_events_through_slave_db", `Quick, test_db_events_through_slave_db;
    "test_db_events_without_session", `Quick, test_db_events_without_session;
    "test_db_events_with_session", `Quick, test_db_events_with_session;
    "test_db_counts", `Quick, test_db_counts;
  ]

