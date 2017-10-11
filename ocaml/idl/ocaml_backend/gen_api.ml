(*
 * Copyright (C) 2006-2009 Citrix Systems Inc.
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
open Stdext.Listext
open Printf

module DT = Datamodel_types
module DU = Datamodel_utils
module OU = Ocaml_utils

module O = Ocaml_syntax

let oc = ref stdout
let print s = output_string !oc (s^"\n")

let overrides = [
  "vm_operations_to_string_map",(
    "let rpc_of_vm_operations_to_string_map x = Rpc.Dict (List.map (fun (x,y) -> (match rpc_of_vm_operations x with Rpc.String x -> x | _ -> failwith \"Marshalling error\"), Rpc.String y) x)\n" ^
    "let vm_operations_to_string_map_of_rpc x = match x with Rpc.Dict l -> List.map (function (x,y) -> vm_operations_of_rpc (Rpc.String x), string_of_rpc y) l | _ -> failwith \"Unmarshalling error\"\n");
  "int64_to_float_map",(
    "let rpc_of_int64_to_float_map x = Rpc.Dict (List.map (fun (x,y) -> Int64.to_string x, Rpc.Float y) x)\n" ^
    "let int64_to_float_map_of_rpc x = match x with Rpc.Dict x -> List.map (fun (x,y) -> Int64.of_string x, float_of_rpc y) x | _ -> failwith \"Unmarshalling error\"");
  "int64_to_int64_map",(
    "let rpc_of_int64_to_int64_map x = Rpc.Dict (List.map (fun (x,y) -> Int64.to_string x, Rpc.Int y) x)\n" ^
    "let int64_to_int64_map_of_rpc x = match x with Rpc.Dict x -> List.map (fun (x,y) -> Int64.of_string x, int64_of_rpc y) x | _ -> failwith \"Unmarshalling error\"");
  "int64_to_string_set_map",(
    "let rpc_of_int64_to_string_set_map x = Rpc.Dict (List.map (fun (x,y) -> Int64.to_string x, rpc_of_string_set y) x)\n" ^
    "let int64_to_string_set_map_of_rpc x = match x with Rpc.Dict x -> List.map (fun (x,y) -> Int64.of_string x, string_set_of_rpc y) x | _ -> failwith \"Unmarshalling error\"");
]


let gen_type highapi tys =
  let rec aux accu = function
    | []                           -> accu
    | DT.String               :: t
    | DT.Int                  :: t
    | DT.Float                :: t
    | DT.Bool                 :: t -> aux accu t
    | DT.Record record        :: t ->
    let obj_name = OU.ocaml_of_record_name record in
    let all_fields = DU.fields_of_obj (Dm_api.get_obj_by_name highapi ~objname:record) in
    let field fld = OU.ocaml_of_record_field (obj_name :: fld.DT.full_name) in
    let map_fields fn = String.concat "; " (List.map (fun field -> fn field) all_fields) in
    let regular_def fld = sprintf "%s : %s [@key \"%s\"]" (field fld) (OU.alias_of_ty fld.DT.ty) (String.concat "_" fld.DT.full_name) in

    let type_t = sprintf "type %s_t = { %s } [@@deriving rpc]" obj_name (map_fields regular_def) in
    aux (type_t :: accu) t

    | DT.Set (e) as ty :: t ->
      aux (sprintf "type %s = %s list [@@deriving rpc]" (OU.alias_of_ty ty) (OU.alias_of_ty e) :: accu) t
    | ty                      :: t ->
      let alias = OU.alias_of_ty ty in
      if List.mem_assoc alias overrides
      then aux ((sprintf "type %s = %s\n%s\n" alias (OU.ocaml_of_ty ty) (List.assoc alias overrides))::accu) t
      else aux (sprintf "type %s = %s [@@deriving rpc]" (OU.alias_of_ty ty) (OU.ocaml_of_ty ty) :: accu) t in
  aux [] tys


let gen_client highapi =
  List.iter (List.iter print)
    (List.between [""] [
        [
          "open API";
          "open Rpc";
          "module type RPC = sig val rpc: Rpc.t -> Rpc.t end";
          "module type IO = sig type 'a t val bind : 'a t -> ('a -> 'b t) -> 'b t val return : 'a -> 'a t end";
          "";
          "let server_failure code args = raise (Api_errors.Server_error (code, args))";
        ];
        O.Module.strings_of (Gen_client.gen_module highapi);
        [ "module Id = struct type 'a t = 'a let bind x f = f x let return x = x end";
          "module Client = ClientF(Id)" ]
      ])

let add_set_enums types =
  List.concat (
    List.map (fun ty ->
        match ty with
        | DT.Enum _ ->
          if List.exists (fun ty2 -> ty2 = DT.Set ty) types then [ty] else [DT.Set ty; ty]
        | _ -> [ty]) types)

(* Returns a list of type sorted such that the first elements in the
   list have nothing depending on them. Later elements in the list may
   depend upon types earlier in the list *)
let toposort_types highapi types =
  Printf.fprintf stderr "In toposort\n%!";
  let rec inner result remaining =
    Printf.fprintf stderr "In inner\n%!";
    let rec references ty = function
      | ty' when ty=ty' -> true
      | DT.String
      | DT.Int
      | DT.Float
      | DT.Bool
      | DT.DateTime
      | DT.Ref _
      | DT.Enum _ -> false
      | DT.Set ty' -> references ty ty'
      | DT.Map (ty', ty'') -> (references ty ty') || (references ty ty'')
      | DT.Record record ->
        let all_fields = DU.fields_of_obj (Dm_api.get_obj_by_name highapi ~objname:record) in
        List.exists (fun fld -> references ty fld.DT.ty) all_fields
    in
    let (ty_ref,ty_not_ref) =
      List.partition (fun ty ->
        List.exists (fun ty' ->
          (ty != ty') && (references ty ty')
          ) remaining) remaining
    in
    if List.length ty_ref > 0
    then inner (result @ ty_not_ref) ty_ref
    else (result @ ty_not_ref)
  in
  let result = inner [] types in
  assert(List.length result = List.length types);
  assert(List.sort compare result = List.sort compare types);
  result

let gen_client_types highapi =
  let all_types = DU.Types.of_objects (Dm_api.objects_of_api highapi) in
  let all_types = add_set_enums all_types in
  List.iter (List.iter print)
    (List.between [""] [
        [
          "type failure = (string list) [@@deriving rpc]";
          "let response_of_failure code params =";
          "  Rpc.failure (rpc_of_failure (code::params))";
          "let response_of_fault code =";
          "  Rpc.failure (rpc_of_failure ([\"Fault\"; code]))";
        ]; [
          "include Rpc";
          "type string_list = string list [@@deriving rpc]";
        ]; [
          "module Ref = struct";
          "  include Ref";
          "  let rpc_of_t _ x = rpc_of_string (Ref.string_of x)";
          "  let t_of_rpc _ x = of_string (string_of_rpc x);";
          "end";
        ]; [
          "module Date = struct";
          "  open Stdext";
          "  include Date";
          "  let rpc_of_iso8601 x = DateTime (Date.to_string x)";
          "  let iso8601_of_rpc = function String x | DateTime x -> Date.of_string x | _ -> failwith \"Date.iso8601_of_rpc\"";
          "end";
        ]; [
          "let on_dict f = function | Rpc.Dict x -> f x | _ -> failwith \"Expected Dictionary\""
        ];
        gen_type highapi (toposort_types highapi all_types);
        O.Signature.strings_of (Gen_client.gen_signature highapi);
        [ "module Legacy = struct";
          "open XMLRPC";
          "module D=Debug.Make(struct let name=\"legacy_marshallers\" end)";
          "open D" ];
        GenOCaml.gen_of_xmlrpc highapi all_types;
        GenOCaml.gen_to_xmlrpc highapi all_types;
        ["end"];
      ])

let gen_server highapi =
  List.iter (List.iter print)
    (List.between [""] [
        [ "open API"; "open Server_helpers" ];
        O.Module.strings_of (Gen_server.gen_module highapi);
      ])

let gen_custom_actions highapi =
  List.iter (List.iter print)
    (List.between [""] [
        [ "open API" ];
        O.Signature.strings_of (Gen_empty_custom.gen_signature Gen_empty_custom.signature_name None highapi);
        O.Module.strings_of (Gen_empty_custom.gen_release_module highapi);
      ])

open Gen_db_actions

let gen_db_actions highapi =
  let all_types = DU.Types.of_objects (Dm_api.objects_of_api highapi) in
  let only_records = List.filter (function DT.Record _ -> true | _ -> false) all_types in

  List.iter (List.iter print)
    (List.between [""]
       [
         [ "open API" ];

         (* These records have the hidden fields inside *)
         gen_type highapi only_records;

         (* NB record types are ignored by dm_to_string and string_to_dm *)
         O.Module.strings_of (dm_to_string all_types);
         O.Module.strings_of (string_to_dm all_types);
         O.Module.strings_of (db_action highapi); ]
     @ (List.map O.Module.strings_of (Gen_db_check.all highapi)) @ [

     ]
    )

let gen_rbac highapi =
  print (Gen_rbac.gen_permissions_of_static_roles highapi)
