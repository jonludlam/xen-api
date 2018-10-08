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

let stat_to_string (stat:Db_cache_types.Stat.t) =
  "{created: " ^ (Int64.to_string stat.created) ^ "; modified: " ^ (Int64.to_string stat.modified) ^ "; deleted: " ^ (Int64.to_string stat.deleted) ^ ";}"

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

  let changes = Xapi_database_backup.get_delta ~__context:__context ~token:(-2L) in
  Xapi_database_backup.apply_changes changes __context;
  let changes_db = !(Xapi_slave_db.slave_db) in

  let gen_init = gen (mf init_db) in
  let gen_changes = gen (mf changes_db) in
  Printf.printf "\nGeneration 1: %Li" gen_init;
  Printf.printf "\nGeneration 2: %Li\n" gen_changes;

  (*Printf.printf "Changes: %s" (Xapi_database_backup.delta_to_string changes);*)

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
  let changes = Xapi_database_backup.get_delta ~__context:__context ~token:(-2L) in
  Xapi_database_backup.apply_changes changes __context;
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
  let changes = Xapi_database_backup.get_delta ~__context:__context ~token:(-2L) in
  Xapi_database_backup.apply_changes changes __context;
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
  let changes = Xapi_database_backup.get_delta ~__context:__context ~token:(-2L) in
  Xapi_database_backup.apply_changes changes __context;
  let changes_db = !(Xapi_slave_db.slave_db) in

  Alcotest.(check bool) "First vm created - generations equal" true (gen (mf init_db) = (gen (mf changes_db)));
  Alcotest.(check bool) "First vm created - tables correct" true (dbs_are_equal init_db changes_db);

  let vm2 = make_vm __context () in
  Db.VM.set_name_description __context vm2 "NewName2";
  let init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta ~__context:__context ~token:(-2L) in
  Xapi_database_backup.apply_changes changes __context;
  let changes_db = !(Xapi_slave_db.slave_db) in

  Alcotest.(check bool) "Second vm created - generations equal" true (gen (mf init_db) = (gen (mf changes_db)));
  Alcotest.(check bool) "Second vm created - tables correct" true (dbs_are_equal init_db changes_db);

  let vm3 = make_vm __context () in
  Db.VM.set_name_description __context vm3 "NewName3";
  let init_db = Db_ref.get_database (Context.database_of __context) in
  let changes = Xapi_database_backup.get_delta ~__context:__context ~token:(-2L) in
  Xapi_database_backup.apply_changes changes __context;
  let changes_db = !(Xapi_slave_db.slave_db) in

  Alcotest.(check bool) "Third vm created - generations equal" true (gen (mf init_db) = (gen (mf changes_db)));
  Alcotest.(check bool) "Third vm created - tables correct" true (dbs_are_equal init_db changes_db);

  Xapi_slave_db.clear_db ()

let test =
  [
    "test_db_backup", `Quick, test_db_backup;
    "test_db_with_vm", `Quick, test_db_with_vm;
    "test_db_with_name_change", `Quick,  test_db_with_name_change;
    "test_db_with_multiple_changes", `Quick,  test_db_with_multiple_changes;
  ]

