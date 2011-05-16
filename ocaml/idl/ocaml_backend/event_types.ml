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
(** Types used to store events: *****************************************************************)
type op = 
    | Add   (** Object has been created *)
    | Del   (** Object has been deleted *)
    | Mod   (** Object has been modified *)
    | Dummy (** A dummy or filler event inserted by coalesce_events *)

type event = {
	id: int64;
	ts: float;
	ty: string;
	op: op;
	reference: string;
	snapshot: XMLRPC.xmlrpc option;
}

open Printf
open Pervasiveext

let string_of_op = function Add -> "add" | Mod -> "mod" | Del -> "del" | Dummy -> "dummy"
let op_of_string x = match String.lowercase x with
  | "add" -> Add | "mod" -> Mod | "del" -> Del
  | x -> failwith (sprintf "Unknown operation type: %s" x)

let string_of_event ev = sprintf "%Ld %s %s %s %s" ev.id ev.ty (string_of_op ev.op) ev.reference
  (if ev.snapshot = None then "(no snapshot)" else "OK")

(* Print a single event record as an XMLRPC value *)
let xmlrpc_of_event ev = 
  XMLRPC.To.structure 
    ([
       "id", XMLRPC.To.string (Int64.to_string ev.id);
       "timestamp", XMLRPC.To.string (string_of_float ev.ts);
       "class", XMLRPC.To.string ev.ty;
       "operation", XMLRPC.To.string (string_of_op ev.op);
       "ref", XMLRPC.To.string ev.reference;
    ] @ (default [] (may (fun x -> [ "snapshot", x ]) ev.snapshot)))

exception Event_field_missing of string

(* Convert a single XMLRPC value containing an encoded event into the event record *)
let event_of_xmlrpc x = 
  let kvpairs = XMLRPC.From.structure x in
  let find x = 
    if not(List.mem_assoc x kvpairs)
    then raise (Event_field_missing x) else List.assoc x kvpairs in
  { id = Int64.of_string (XMLRPC.From.string (find "id"));
    ts = float_of_string (XMLRPC.From.string (find "timestamp"));
    ty = XMLRPC.From.string (find "class");
    op = op_of_string (XMLRPC.From.string (find "operation"));
    reference = XMLRPC.From.string (find "ref");
    snapshot = if List.mem_assoc "snapshot" kvpairs then Some (List.assoc "snapshot" kvpairs) else None
  }

(* Convert an XMLRPC array of events into a list of event records *)
let events_of_xmlrpc xml = 
	let kvpairs = XMLRPC.From.structure xml in
	let events = try List.assoc "events" kvpairs with _ -> failwith "Event records corrupted!" in
	XMLRPC.From.array event_of_xmlrpc events
