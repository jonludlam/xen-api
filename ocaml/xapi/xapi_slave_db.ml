module D=Debug.Make(struct let name="xapi_slave_db" end)
open D

open Stdext
open Threadext

let slave_db_mutex = Mutex.create ()
let slave_db = ref (Db_ref.get_database (Db_backend.make ()))

let clear_db () =
  Mutex.execute slave_db_mutex (fun () -> slave_db := (Db_cache_types.Database.make Schema.empty))

let update_context_db (__context:Context.t) =
  if Pool_role.is_slave () then
    Context.update_database __context (Db_ref.in_memory (ref slave_db))
  else
    __context

