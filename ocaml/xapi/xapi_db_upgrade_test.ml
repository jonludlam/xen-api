(*
 * Copyright (C) 2011 Citrix Systems Inc.
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

open Xapi_test_common
open Xapi_db_upgrade

let upgrade_vm_memory_for_dmc () = 
	let __context = make_test_database () in

	let self = List.hd (Db.VM.get_all ~__context) in

	(* Set control domain's dynamic_min <> dynamic_max <> target *)
	Db.VM.set_memory_dynamic_min ~__context ~self ~value:1L;
	Db.VM.set_memory_target ~__context ~self ~value:2L;
	Db.VM.set_memory_dynamic_max ~__context ~self ~value:3L;
	(* Apply the upgrade rule *)
	upgrade_vm_memory_for_dmc.fn ~__context; 
	let r = Db.VM.get_record ~__context ~self in
	if r.API.vM_memory_dynamic_min <> r.API.vM_memory_target
	then failwith "upgrade_vm_memory_for_dmc: control domain memory_dynamic_min <> memory_target";
	if r.API.vM_memory_dynamic_max <> r.API.vM_memory_target
	then failwith "upgrade_vm_memory_for_dmc: control domain memory_dynamic_max <> memory_target";

	(* Make this a non-control domain and change all memory fields *)
	Db.VM.set_is_control_domain ~__context ~self ~value:false;
	Db.VM.set_memory_static_min ~__context ~self ~value:5L;
	Db.VM.set_memory_dynamic_min ~__context ~self ~value:1L;
	Db.VM.set_memory_target ~__context ~self ~value:2L;
	Db.VM.set_memory_dynamic_max ~__context ~self ~value:3L;	
	Db.VM.set_memory_static_max ~__context ~self ~value:4L;
	(* Apply the upgrade rule *)
	upgrade_vm_memory_for_dmc.fn ~__context;
	let r = Db.VM.get_record ~__context ~self in
	if r.API.vM_memory_dynamic_max <> r.API.vM_memory_static_max
	then failwith "upgrade_vm_memory_for_dmc: memory_dynamic_max <> memory_static_max";
	if r.API.vM_memory_target <> r.API.vM_memory_static_max
	then failwith "upgrade_vm_memory_for_dmc: memory_target <> memory_static_max";
	if r.API.vM_memory_dynamic_min <> r.API.vM_memory_static_max
	then failwith "upgrade_vm_memory_for_dmc: memory_dynamic_min <> memory_static_max";
	if r.API.vM_memory_static_min > r.API.vM_memory_static_max
	then failwith "upgrade_vm_memory_for_dmc: memory_static_min > memory_static_max";
	Printf.printf "upgrade_vm_memory_for_dmc: OK\n"

let upgrade_bios () = 

	let sort l =
		List.sort (fun x y -> compare (fst x) (fst y)) l in
	let check inventory bios_strings = 
		Unixext.mkdir_safe "/var/tmp" 0o755;
		Unixext.write_string_to_file "/var/tmp/.previousInventory" inventory;
		let __context = make_test_database () in
		upgrade_bios_strings.fn ~__context; 
		let _, vm_r = List.hd (Db.VM.get_all_records ~__context) in
		if (sort vm_r.API.vM_bios_strings) <> (sort bios_strings)
		then failwith "bios strings upgrade" in
	
	check "OEM_MANUFACTURER=Dell" Xapi_globs.old_dell_bios_strings;
	check "OEM_MANUFACTURER=HP" Xapi_globs.old_hp_bios_strings;
	check "" Xapi_globs.generic_bios_strings;
	Unixext.unlink_safe "/var/tmp/.previousInventory";
	Printf.printf "upgrade_bios: OK\n"

let update_snapshots () = 
	let __context = make_test_database () in
	let a = make_vm ~__context ~name_label:"a" () in
	let a_snap = make_vm ~__context ~name_label:"a snap" () in
	Db.VM.set_snapshot_of ~__context ~self:a_snap ~value:a;
	Db.VM.set_snapshot_time ~__context ~self:a_snap ~value:(Date.of_float 1.);

	let b = make_vm ~__context ~name_label:"b" () in
	let b_snap = make_vm ~__context ~name_label:"b snap" () in
	Db.VM.set_snapshot_of ~__context ~self:b_snap ~value:b;
	Db.VM.set_snapshot_time ~__context ~self:b_snap ~value:(Date.of_float 1.);
	let b_snap2 = make_vm ~__context ~name_label:"b snap2" () in
	Db.VM.set_snapshot_of ~__context ~self:b_snap2 ~value:b;
	Db.VM.set_snapshot_time ~__context ~self:b_snap2 ~value:(Date.of_float 2.);
	
	update_snapshots.fn ~__context;

	(* a.parent = a_snap *)
	if Db.VM.get_parent ~__context ~self:a <> a_snap
	then failwith "a.parent <> a_snap";

	(* b.parent = b_snap2 *)
	if Db.VM.get_parent ~__context ~self:b <> b_snap2
	then failwith "b.parent <> b_snap2";

	(* b_snap2.parent = b_snap *)
	if Db.VM.get_parent ~__context ~self:b_snap2 <> b_snap
	then failwith "b_snap2.parent <> b_snap";

	Printf.printf "update_snapshots: OK\n"

let all () = 
	upgrade_vm_memory_for_dmc ();
	upgrade_bios ();
	update_snapshots ()
