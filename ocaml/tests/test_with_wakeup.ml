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

let test_wakeup_twice () =
  let __context = make_test_database () in
  let wf = ref (fun () -> ()) in
  let _vm = make_vm __context () in
  let woken_up_by_task = ref false in
  Xapi_event.with_wakeup __context "Some location string" (fun wakeup_function wakeup_classes task ->
      let init_event = Xapi_event.from __context ["vm"] "" 30.0 |> Event_types.parse_event_from in
      wf := wakeup_function;
      let classes = wakeup_classes @ ["vm"] in
      let task_destroy_thread = Thread.create (fun () -> Thread.delay 0.1; !wf (); !wf ();) () in

      while (Db.is_valid_ref __context task) do
        let evs = Xapi_event.from __context classes init_event.token 30.0 |> Event_types.parse_event_from in
        List.iter (function
            | {ty = "task"; reference = t_ref} ->
              woken_up_by_task := t_ref = (Ref.string_of task)
            | {ty = "vm"; reference = vm_ref} ->
              Printf.printf "Receieved an event on a vm"
            | _ -> Printf.printf "Received some other event"
          ) evs.events;
      done;

      Thread.join task_destroy_thread
    );
  Alcotest.(check bool) "Task kill function can be safely called repeatedly" true !woken_up_by_task

let test =
  [
    "test_wakeup_twice", `Quick, test_wakeup_twice;
  ]


