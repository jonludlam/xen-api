(*
 * Copyright (C) 2017 Citrix Systems Inc.
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

open Stdext
open OUnit
open Test_highlevel

module VMSetBiosStrings = Generic.Make (Generic.EncapsulateState (struct
  module Io = struct
    type input_t = (API.vm_bios_string_keys * string) list
    type output_t = (exn, (string * string) list) Either.t
    let string_of_input_t = fun x -> Printf.sprintf "%s"
      (String.concat "; " (List.map (fun (k,v) -> (Record_util.vm_bios_key_to_string k) ^ "=" ^ v) x))
    let string_of_output_t = Test_printers.(either exn (assoc_list string string))
  end
  module State = Test_state.XapiDb

  let name_label = "a"

  let load_input __context _ =
    ignore (Test_common.make_vm ~__context ~name_label ())

  let extract_output __context value =
    let self = List.hd (Db.VM.get_by_name_label ~__context ~label:name_label) in
    try
      Xapi_vm.set_bios_strings ~__context ~self ~value;
      Either.Right (Db.VM.get_bios_strings ~__context ~self)
    with e -> Either.Left e

  let big_str = String.make (Xapi_globs.bios_string_limit_size + 1) 'x'
  let non_printable_str1 = Printf.sprintf "xyz%c" (Char.chr 31)
  let non_printable_str2 = Printf.sprintf "xyz%c" (Char.chr 127)
  let bios_str1 = [`biosvendor, "Test"; `biosversion, "Test Inc. A08"]
  let bios_str2 = [`systemmanufacturer, "Test Inc."; `systemproductname, "Test bios strings"; `systemversion, "8.1.1 SP1 build 8901"; `systemserialnumber, "test-test-test-test"]
  let bios_str3 = [`enclosureassettag, "testassettag12345"]

  let tests = [
    (* Empty value *)
    [`enclosureassettag, ""],
    Either.Left Api_errors.(Server_error
      (invalid_value,
      ["enclosure-asset-tag"; "Value provided is empty"]));

    (* Value having more than 512 charactors *)
    [`enclosureassettag, big_str],
    Either.Left Api_errors.(Server_error
      (invalid_value,
      ["enclosure-asset-tag"; (Printf.sprintf "%s has length more than %d characters" big_str Xapi_globs.bios_string_limit_size)]));

    (* Value having non printable ascii characters *)
    [`enclosureassettag, non_printable_str1],
    Either.Left Api_errors.(Server_error
      (invalid_value,
      ["enclosure-asset-tag"; non_printable_str1 ^ " has non-printable ASCII characters"]));

    [`enclosureassettag, non_printable_str2],
    Either.Left Api_errors.(Server_error
      (invalid_value,
      ["enclosure-asset-tag"; non_printable_str2 ^ " has non-printable ASCII characters"]));

    (* Correct value *)
    bios_str1,
    Either.Right [
      "bios-vendor", "Test";
      "bios-version", "Test Inc. A08";
      "system-manufacturer", "Xen";
      "system-product-name", "HVM domU";
      "system-version", "";
      "system-serial-number", "";
      "enclosure-asset-tag", "";
      "hp-rombios", "";
      "oem-1", "Xen";
      "oem-2", "MS_VM_CERT/SHA1/bdbeb6e0a816d43fa6d3fe8aaef04c2bad9d3e3d"];

    bios_str2,
    Either.Right [
      "bios-vendor", "Xen";
      "bios-version", "";
      "system-manufacturer", "Test Inc.";
      "system-product-name", "Test bios strings";
      "system-version", "8.1.1 SP1 build 8901";
      "system-serial-number", "test-test-test-test";
      "enclosure-asset-tag", "";
      "hp-rombios", "";
      "oem-1", "Xen";
      "oem-2", "MS_VM_CERT/SHA1/bdbeb6e0a816d43fa6d3fe8aaef04c2bad9d3e3d"];

    bios_str3,
    Either.Right [
      "bios-vendor", "Xen";
      "bios-version", "";
      "system-manufacturer", "Xen";
      "system-product-name", "HVM domU";
      "system-version", "";
      "system-serial-number", "";
      "enclosure-asset-tag", "testassettag12345";
      "hp-rombios", "";
      "oem-1", "Xen";
      "oem-2", "MS_VM_CERT/SHA1/bdbeb6e0a816d43fa6d3fe8aaef04c2bad9d3e3d"];

    (bios_str1 @ bios_str2),
    Either.Right [
      "bios-vendor", "Test";
      "bios-version", "Test Inc. A08";
      "system-manufacturer", "Test Inc.";
      "system-product-name", "Test bios strings";
      "system-version", "8.1.1 SP1 build 8901";
      "system-serial-number", "test-test-test-test";
      "enclosure-asset-tag", "";
      "hp-rombios", "";
      "oem-1", "Xen";
      "oem-2", "MS_VM_CERT/SHA1/bdbeb6e0a816d43fa6d3fe8aaef04c2bad9d3e3d"];

    (bios_str1 @ bios_str3),
    Either.Right [
      "bios-vendor", "Test";
      "bios-version", "Test Inc. A08";
      "system-manufacturer", "Xen";
      "system-product-name", "HVM domU";
      "system-version", "";
      "system-serial-number", "";
      "enclosure-asset-tag", "testassettag12345";
      "hp-rombios", "";
      "oem-1", "Xen";
      "oem-2", "MS_VM_CERT/SHA1/bdbeb6e0a816d43fa6d3fe8aaef04c2bad9d3e3d"];

    (bios_str2 @ bios_str3),
    Either.Right [
      "bios-vendor", "Xen";
      "bios-version", "";
      "system-manufacturer", "Test Inc.";
      "system-product-name", "Test bios strings";
      "system-version", "8.1.1 SP1 build 8901";
      "system-serial-number", "test-test-test-test";
      "enclosure-asset-tag", "testassettag12345";
      "hp-rombios", "";
      "oem-1", "Xen";
      "oem-2", "MS_VM_CERT/SHA1/bdbeb6e0a816d43fa6d3fe8aaef04c2bad9d3e3d"];

    (bios_str1 @ bios_str2 @ bios_str3),
    Either.Right [
      "bios-vendor", "Test";
      "bios-version", "Test Inc. A08";
      "system-manufacturer", "Test Inc.";
      "system-product-name", "Test bios strings";
      "system-version", "8.1.1 SP1 build 8901";
      "system-serial-number", "test-test-test-test";
      "enclosure-asset-tag", "testassettag12345";
      "hp-rombios", "";
      "oem-1", "Xen";
      "oem-2", "MS_VM_CERT/SHA1/bdbeb6e0a816d43fa6d3fe8aaef04c2bad9d3e3d"];

  ]
end))

let test_marshalling () =
  let test = Rpc.Dict [ "biosvendor", Rpc.String "test" ] in
  let test' = API.vm_bios_string_keys_to_string_map_of_rpc test in
  let expected = [ `biosvendor, "test" ] in
  OUnit.assert_equal test' expected


let test =
  "test_vm" >:::
  [
    "test_vm_set_bios_strings" >::: VMSetBiosStrings.tests;
    "test_vm_bios_string_keys_marshalling" >:: test_marshalling;
  ]
