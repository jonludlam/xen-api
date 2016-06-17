(*
 * Copyright (C) 2016 Citrix Systems Inc.
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

(* This module implements methods for the PVS_farm class *)

module D = Debug.Make(struct let name = "xapi_pvs_farm" end)

let not_implemented x =
  raise (Api_errors.Server_error (Api_errors.not_implemented, [ x ]))

let introduce ~__context ~name =
	let pvs_farm = Ref.make () in
	let uuid = Uuid.to_string (Uuid.make_uuid ()) in
	Db.PVS_farm.create ~__context
		~ref:pvs_farm ~uuid ~name ~cache_storage:[];
	pvs_farm

let forget ~__context ~self =
	let open Db_filter_types in
	(* Check there are no running proxies. *)
	let running_proxies = Db.PVS_proxy.get_refs_where ~__context
		~expr:(And
			((Eq (Field "farm", Literal (Ref.string_of self))),
			(Eq (Field "currently_attached", Literal "true"))))
	in
	if running_proxies <> []
	then raise Api_errors.(Server_error
		(pvs_farm_contains_running_proxies,
		List.map Ref.string_of running_proxies));
	(* Check there are no servers. *)
	let servers = Db.PVS_farm.get_servers ~__context ~self in
	if servers <> []
	then raise Api_errors.(Server_error
		(pvs_farm_contains_servers, List.map Ref.string_of servers));
	Db.PVS_farm.destroy ~__context ~self

let set_name ~__context ~self ~value =
  not_implemented "PVS_farm.set_name"

let add_cache_storage ~__context ~self ~value =
  not_implemented "PVS_farm.add_cache_storage"

let remove_cache_storage ~__context ~self ~value =
  not_implemented "PVS_farm.remove_cache_storage"


