(* OASIS_START *)
(* OASIS_STOP *)

let custom_dispatch = function
| After_rules ->
  let autogen_rule ~mode ~filter ~filterinternal ?(gendebug=false) output =
    let gen_api_main = "ocaml/idl/ocaml_backend/gen_api_main.native" in
    rule output ~dep:gen_api_main ~prod:output (fun _ _ ->
      Cmd(S(
        [ P gen_api_main ]
        @ [ A "-mode"; A mode ]
        @ [ A "-filterinternal"; A (string_of_bool filterinternal) ]
        @ [ A "-filter"; A filter ]
        @ if gendebug then [ A "-gendebug" ] else []
        @ [ Sh ">"; A output ]))) in

  autogen_rule "ocaml/autogen/client.ml"
    ~mode:"client"
    ~filterinternal:true
    ~filter:"closed";

  autogen_rule "ocaml/autogen/server.ml"
    ~mode:"server"
    ~filterinternal:true
    ~filter:"closed"
    ~gendebug:true;
  
  autogen_rule "ocaml/autogen/aPI.ml"
    ~mode:"api"
    ~filterinternal:true
    ~filter:"closed";

  autogen_rule "ocaml/autogen/db_actions.ml"
    ~mode:"db"
    ~filterinternal:false
    ~filter:"nothing";

  autogen_rule "ocaml/autogen/custom_actions.ml"
    ~mode:"actions"
    ~filterinternal:true
    ~filter:"closed";

  autogen_rule "ocaml/autogen/rbac_static.ml"
    ~mode:"rbac"
    ~filterinternal:true
    ~filter:"closed";

  autogen_rule "ocaml/autogen/rbac_static.csv"
    ~mode:"rbac"
    ~filterinternal:true
    ~filter:"closed"
    ~gendebug:true;
| _ -> ();;

Ocamlbuild_plugin.dispatch (fun h -> custom_dispatch h; dispatch_default h);;
