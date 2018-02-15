# Handling of resident_on and power_state

## Background

The field `resident_on` is a critically important one for the safety of VMs in XenServer. It is used to for two main purposes: to forward API calls to the correct host (in message_forwarding) and it is used when considering resources allocated on a host.

The safety-critical aspect is that if a VM is marked as resident on a particular host, that host is required to update the state of the VM
as it receives events from xenopsd. Thus if the VM is not running in
xenopsd the host will mark the VM as powered off. The danger then comes from the VM being incorrectly marked as resident on a host other than the one it is actually running on. This can occur due to the database rolling back in time (e.g. non-cooperative master handover), or by an interrupted migration or similar events.

Migration in particular is awkward in current XenServer releases, because for a reasonably large time the metadata about the VM exists in both source and destination xenopsds. Therefore events come referencing the VM's uuid on both hosts and we have to carefully ignore events for whichever host doesn't have the _running_ VM.

If xapi is interrupted or restarted when the xenopsd are in that state there is currently no mechanism for xapi to correctly determine whether the VM should be running on source or destination, whether it's still running at all, or whether it should be marked as halted.

## Suggested fix

The suggested fix is in two parts: changes in xenopsd relating to  migration and how it handles metadata, and some subsequent changes in how xapi responds to events from xenopsd. There are some slightly modified semantics too:

1. `VM.resident_on` == metadata present in xenopsd
2. VM lifecycle ops must consult `VM.resident_on` - start/resume/etc must only be allowed if not `is_valid_ref(VM.resident_on)`
3. `resident_on=host` means that host is responsible for the `power_state` field of the VM (this is already the case).

### Xenopsd changes

First, a low-level part changing the way migration is handled in xenopsd. Rather than importing the metadata into the remote xenopsd with the same uuid, a different (but related) uuid is chosen and this temporary uuid is used for the construction of the domain on the destination. Once the VM has suspended and the last of the memory has been copied to the destination, the source domain and VM metadata is renamed to a third uuid. Only when this has been done is the VM on the destination renamed to be the original uuid and subsequently unpaused. The source xenopsd then removes all metadata associated with the original domain, which by this point had been renamed anyway. Using this mechanism there is now never a time when the same uuid is in use by xenopsd on two different hosts. This simplifies the logic in xapi.

The second change relates to how metadata is pushed into xenopsd before a VM is started or resumed. The idea is to maintain an invariant that xapi will always know when it is the right time to _remove_ metadata from xenopsd. That will be when Xenopsd knows about a VM that is in xapi's database, it is halted and has no current operations associated with it.
This invariant is not currently true - during VM.start for example we push the metadata down and only subsequently queue the VM.start operation.
The plan is to take inspiration from the above migration change and push down the initial metadata under a different uuid. The VM.start / VM.resume would then move the metadata to the correct uuid at some point during the operation.

### Events handling in xapi

The events thread in xapi can now accept _all_ incoming events without reference to `resident_on` -- in fact, `resident_on` should now be set whenever an event comes in for a particular VM. We also remove some logic because xapi can _only_ react to events emitted by xenopsd itself and _mustn't_ update anything automatically for any other reason.

The condition for a VM to be marked as halted is that the metadata is in xenopsd, the VM is not running and there are no current operations on the VM. The events thread should then set the power_state to `Halted` in the database, remove the metadata from xenops and then unset `resident_on`.

### Failure modes

There are two main failure modes we want to be robust against. Firstly is a breakdown in communications between master and slave at some point during these operations. The second is the database reverting to an older snapshot, as might happen during an uncooperative master failover. For both of these the fix will be to resynchronise xapi's database with the state in xenopsd.

Migration is the most complex case, so we consider that first. Initially, `resident_on` is set to be the source host, but when the migration starts xapi then has no control over when it completes, so at some point the VM turns up on the destination emitting events. However, if there is a break in communications before the `resident_on` field is set by the destination host, we are left with a situation where `resident_on` is set to be the source while the VM is actually running on the destination.

Should xapi restart on the source at this point, it must be careful that simply because `resident_on` is set to itself and xenopsd has no knowledge of the VM, that does not imply the host can set the VM to be 'powered_off'. Indeed, it shouldn't even set `resident_on` to null with the standard Db setter in case it's racing with another host setting it to themselves.

What might still happen is that a failure occurs during the time when the VM metadata is nowhere in any xenopsd, ie. after it has been renamed on the source but before it has been renamed on the destination. Thus no xapi will receive any event telling it that the VM is now halted / suspended.

At this point, we need a way to take an overview of the _whole_ pool - one that will contact all hosts and find out which VMs are actually running on each host. We may be in a situation where a host is running the VM but is not on the management network (e.g. management PIF failure), so we must handle this situation -- although 'handling' in this case may simply failing the entire call. This call will require each host to synchronise `resident_on` with its xenopsd - that is, by the end of the call all VMs that are in that host's xenopsd will have `resident_on` set to that host, and no VMs that _aren't_ in that host's xenopsd will have `resident_on` set to that host. Once that has been done for all hosts, the master can then `force_state_reset` on all VMs that have no `resident_on` set but that are marked as running.

This will be done via a set of new API calls:

```ocaml
  VM.clear_resident_on : session_id -> host_id -> unit
  Pool.calculate_inconsistent_vms : session_id -> pool_ref -> VM_ref list
  Host.show_me_what_you_got : session_id -> host_ref -> unit
```

1. `VM.clear_resident_on` is to set resident_on to null, but it takes the current value as parameter and will only set it if the current value is equal to the parameter of the call.

2. `Pool.calculate_inconsistent_vms` will be called during the startup sequence on the master during the Dbsync_master, and all inconsistent VMs will have their `power_state`s reset.

The implementations of these calls are roughly:

#### Pool.calculate_inconsistent_vms

1. Iterate over all hosts and call `Host.show_me_what_you_got`, with a short timeout
2. If these calls are all successful, return a list of references to VMs that have no resident_on but that do not have `power_state` ∈ `[Running, Paused]`

#### Host.show_me_what_you_got

1. List all known xenopsds
2. List all VMs present in xenopsd
3. Cancel any ongoing migrations
4. Inject events for all of these
5. Inject a barrier
6. Wait for barrier to be processed



## Appendix: Uses of `resident_on` in the code:

* [cancel_tests.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/cancel_tests.ml#L218-L221) - used for determining host parameter to pass to VM.pool_migrate:

```ocaml
debug "execute: VM.pool_migrate %s to localhost" (Ref.string_of vm);
let host = Client.VM.get_resident_on ~rpc ~session_id ~self:vm in
Client.Async.VM.pool_migrate ~rpc ~session_id ~vm ~host ~options:["live", "true"]
```

* [console.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/console.ml#L197-L208) - used in the console handler to verify the VM is running on this host:

```ocaml
let resident_on = Db.VM.get_resident_on ~__context ~self:vm in
if resident_on <> localhost then begin
  error "VM %s (Console %s) has resident_on = %s <> localhost" (Ref.string_of vm) (Ref.string_of console) (Ref.string_of resident_on);
  raise Failure
end
```

* [dbysnc_master.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/dbsync_master.ml#L70-L77) - used to set the console URIs:

```ocaml
let vm = Db.Console.get_VM ~__context ~self:console in
let host = Db.VM.get_resident_on ~__context ~self:vm in
let url_should_be = match Db.Host.get_address ~__context ~self:host with
  | "" -> ""
  | address ->
    Printf.sprintf "https://%s%s?ref=%s" address Constants.console_uri (Ref.string_of console) in
Db.Console.set_location ~__context ~self:console ~value:url_should_be
```

* [dbsync_master.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/dbsync_master.ml#L82-L90) - used to reset the state of VMs that are resident on non-existent hosts, which cover the case where a pool-restore-database has happened:

```ocaml
let vm_r = Db.VM.get_record ~__context ~self:vm in
let valid_resident_on = Db.is_valid_ref __context vm_r.API.vM_resident_on in
if (not valid_resident_on) && (vm_r.API.vM_power_state = `Running) then begin
  let msg = Printf.sprintf "Resetting VM uuid '%s' to Halted because VM.resident_on refers to a Host which is no longer in the Pool" vm_r.API.vM_uuid in
  info "%s" msg;
  Helpers.log_exn_continue msg (fun () -> Xapi_vm_lifecycle.force_state_reset ~__context ~self:vm ~value:`Halted) ()
```

* [helpers.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/helpers.ml#L345-L349) - used to test whether a VM is dom0 or not:

```ocaml
let is_domain_zero ~__context vm_ref =
  let host_ref = Db.VM.get_resident_on ~__context ~self:vm_ref in
  (Db.VM.get_is_control_domain ~__context ~self:vm_ref)
  && (Db.is_valid_ref __context host_ref)
  && (Db.Host.get_control_domain ~__context ~self:host_ref = vm_ref)
```

* [helpers.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/helpers.ml#L375-L384) - belt & braces check before changing dom0's `name_label`:

```ocaml
let update_domain_zero_name ~__context host hostname =
  let stem = "Control domain on host: " in
  let full_name = stem ^ hostname in
  let dom0 = get_domain_zero ~__context in
  (* Double check host *)
  let dom0_host = Db.VM.get_resident_on ~__context ~self:dom0 in
  if dom0_host <> host
  then
    error "Unexpectedly incorrect dom0 record in update_domain_zero_name"
  else begin
  ...
```

* [helpers.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/helpers.ml#L748-L755) - used for finding the suspend_SR for a VM:

```ocaml
let choose_suspend_sr ~__context ~vm =
  (* If the VM.suspend_SR exists, use that. If it fails, try the Pool.suspend_image_SR. *)
  (* If that fails, try the Host.suspend_image_SR. *)
  let vm_sr = Db.VM.get_suspend_SR ~__context ~self:vm in
  let pool = get_pool ~__context in
  let pool_sr = Db.Pool.get_suspend_image_SR ~__context ~self:pool in
  let resident_on = Db.VM.get_resident_on ~__context ~self:vm in
  let host_sr = Db.Host.get_suspend_image_sr ~__context ~self:resident_on in
  ...
```

* [helpers.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/helpers.ml#L769-L779) - similar for crashdump SR:

```ocaml
let choose_crashdump_sr ~__context ~vm =
  (* If the Pool.crashdump_SR exists, use that. Otherwise try the Host.crashdump_SR *)
  let pool = get_pool ~__context in
  let pool_sr = Db.Pool.get_crash_dump_SR ~__context ~self:pool in
  let resident_on = Db.VM.get_resident_on ~__context ~self:vm in
  let host_sr = Db.Host.get_crash_dump_sr ~__context ~self:resident_on in
  match check_sr_exists ~__context ~self:pool_sr, check_sr_exists ~__context ~self:host_sr with
```

* [memory_check.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/memory_check.ml#L135-L144) - uses both `resident_on` and `scheduled_to_be_resident_on` to calculate memory usage:

```ocaml
let get_host_memory_summary ~__context ~host =
  let metrics = Db.Host.get_metrics ~__context ~self:host in
  let host_memory_total_bytes =
    Db.Host_metrics.get_memory_total ~__context ~self:metrics in
  let host_memory_overhead_bytes =
    Db.Host.get_memory_overhead ~__context ~self:host in
  let host_maximum_guest_memory_bytes =
    host_memory_total_bytes --- host_memory_overhead_bytes in
  let resident = Db.VM.get_refs_where ~__context
      ~expr:(Eq (Field "resident_on", Literal (Ref.string_of host))) in
  let scheduled = Db.VM.get_refs_where ~__context
      ~expr:(Eq (Field "scheduled_to_be_resident_on", Literal (
          Ref.string_of host))) in
  {
    host_maximum_guest_memory_bytes = host_maximum_guest_memory_bytes;
    resident = resident;
    scheduled = scheduled;
  }
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L840-L845) - for forwarding a VM operation:

```ocaml
let forward_vm_op ~local_fn ~__context ~vm op =
  let power_state = Db.VM.get_power_state ~__context ~self:vm in
  if List.mem power_state [`Running; `Paused] then
    do_op_on ~local_fn ~__context ~host:(Db.VM.get_resident_on ~__context ~self:vm) op
  else
    local_fn ~__context
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L962-L972) - used when changing the dynamic memory range of a VM to ensure that the host has enough memory to do it:

```ocaml
    let reserve_memory_for_dynamic_change ~__context ~vm
        new_dynamic_min new_dynamic_max f =
      let host = Db.VM.get_resident_on ~__context ~self:vm in
      let old_dynamic_min = Db.VM.get_memory_dynamic_min ~__context ~self:vm in
      let old_dynamic_max = Db.VM.get_memory_dynamic_max ~__context ~self:vm in
      let restore_old_values_on_error = ref false in
      Helpers.with_global_lock
        (fun () ->
           let host_mem_available =
             Memory_check.host_compute_free_memory_with_maximum_compression
               ~__context ~host None in
               ... checks ...
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L1432-L1467) - used in `VM.hard_shutdown`, which is special-cased to handle hard-shutdown of a suspended VM:

```ocaml
    let hard_shutdown ~__context ~vm =
      info "VM.hard_shutdown: VM = '%s'" (vm_uuid ~__context vm);
      let local_fn = Local.VM.hard_shutdown ~vm in
      let host = Db.VM.get_resident_on ~__context ~self:vm in
      with_vm_operation ~__context ~self:vm ~doc:"VM.hard_shutdown" ~op:`hard_shutdown
        (fun () ->
           cancel ~__context ~vm ~ops:[ `clean_shutdown; `clean_reboot; `hard_reboot; `pool_migrate; `call_plugin; `suspend ];
           (* If VM is actually suspended and we ask to hard_shutdown, we need to
              					   forward to any host that can see the VDIs *)
           let policy =
             if Db.VM.get_power_state ~__context ~self:vm = `Suspended
             then
               begin
                 debug "VM '%s' is suspended. Shutdown will just delete suspend VDI" (Ref.string_of vm);
                 (* this expression evaluates to a fn that forwards to a host that can see all vdis: *)
                 let all_vm_srs = Xapi_vm_helpers.compute_required_SRs_for_shutting_down_suspended_domains ~__context ~vm in
                 let suitable_host = Xapi_vm_helpers.choose_host ~__context ~vm:vm
                     ~choose_fn:(Xapi_vm_helpers.assert_can_see_specified_SRs ~__context ~reqd_srs:all_vm_srs) () in
                 do_op_on ~host:suitable_host
               end
             else
               (* if we're nt suspended then just forward to host that has vm running on it: *)
               do_op_on ~host:host
           in
           policy ~local_fn ~__context (fun session_id rpc -> Client.VM.hard_shutdown rpc session_id vm)
        );
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L1469-L1483) - in `VM.hard_reboot`, although I don't know why it doesn't just use `forward_vm_op` rather than `do_op_on`:

```ocaml
let hard_reboot ~__context ~vm =
      info "VM.hard_reboot: VM = '%s'" (vm_uuid ~__context vm);
      let local_fn = Local.VM.hard_reboot ~vm in
      let host = Db.VM.get_resident_on ~__context ~self:vm in
      with_vm_operation ~__context ~self:vm ~doc:"VM.hard_reboot" ~op:`hard_reboot
        (fun () ->
           cancel ~__context ~vm ~ops:[ `clean_shutdown; `clean_reboot; `pool_migrate; `call_plugin; `suspend ];
           with_vbds_marked ~__context ~vm ~doc:"VM.hard_reboot" ~op:`attach
             (fun vbds ->
                with_vifs_marked ~__context ~vm ~doc:"VM.hard_reboot" ~op:`attach
                  (fun vifs ->
                     (* CA-31903: we don't need to reserve memory for reboot because the memory settings can't
                        									   change across reboot. *)
                     do_op_on ~host:host ~local_fn ~__context
                       (fun session_id rpc -> Client.VM.hard_reboot rpc session_id vm))));
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L1678-L1685) - used to check the host's software version in VM.pool_migrate to check we're not doing a 'downhill' migrate:

```ocaml
let source_host = Db.VM.get_resident_on ~__context ~self:vm in

let to_equal_or_greater_version = Helpers.host_versions_not_decreasing ~__context
  ~host_from:(Helpers.LocalObject source_host)
  ~host_to:(Helpers.LocalObject host) in

  if (Helpers.rolling_upgrade_in_progress ~__context) && (not to_equal_or_greater_version) then
    raise (Api_errors.Server_error (Api_errors.not_supported_during_upgrade, []));
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L1998-L2005) - in the body of `atomic_set_resident_on`:

```ocaml
let atomic_set_resident_on ~__context ~vm ~host =
  info "VM.atomic_set_resident_on: VM = '%s'" (vm_uuid ~__context vm);
  (* Need to prevent the host chooser being run while these fields are being modified *)
  Helpers.with_global_lock
    (fun () ->
       Db.VM.set_resident_on ~__context ~self:vm ~value:host;
       Db.VM.set_scheduled_to_be_resident_on ~__context ~self:vm ~value:Ref.null
    )
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L2842-L2847) - used to forward general VIF operations to the correct host:

```ocaml
let forward_vif_op ~local_fn ~__context ~self op =
  let vm = Db.VIF.get_VM ~__context ~self in
  let host_resident_on = Db.VM.get_resident_on ~__context ~self:vm in
  if host_resident_on = Ref.null
  then local_fn ~__context
  else do_op_on ~local_fn ~__context ~host:host_resident_on op
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L3587-L3591) - used in `VDI.pool_migrate` to determine host on which this operation should be run:

```ocaml
let snapshot, host =
  if Xapi_vm_lifecycle.is_live ~__context ~self:vm then
    (Db.VM.get_record ~__context ~self:vm,
    Db.VM.get_resident_on ~__context ~self:vm)
  else
   ... other logic to figure out where to run the operation ...
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L3728-L3733) - in `forward_vbd_op` to forward general VBD operations to the appropriate host:

```ocaml
let forward_vbd_op ~local_fn ~__context ~self op =
  let vm = Db.VBD.get_VM ~__context ~self in
  let host_resident_on = Db.VM.get_resident_on ~__context ~self:vm in
  if host_resident_on = Ref.null
  then local_fn ~__context
  else do_op_on ~local_fn ~__context ~host:host_resident_on op
```

* [message_forwarding.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L4137-L4140) - in choosing host for PVS_proxy operations:

```ocaml
let choose_host ~__context ~vIF =
  let vm = Db.VIF.get_VM ~__context ~self:vIF in
  let host = Db.VM.get_resident_on ~__context ~self:vm in
  if Db.is_valid_ref __context host then host else Helpers.get_localhost ~__context
```

* [message_forwarding](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/message_forwarding.ml#L4239-L4244) - forwarding general VUSB operations to the appropriate host:

```ocaml
let forward_vusb_op ~local_fn ~__context ~self op =
  let vm = Db.VUSB.get_VM ~__context ~self in
  let host_resident_on = Db.VM.get_resident_on ~__context ~self:vm in
  if host_resident_on = Ref.null
  then local_fn ~__context
  else do_op_on ~local_fn ~__context ~host:host_resident_on op
```

* [mtc.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/mtc.ml) - for some reason...?

* [records.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/records.ml) - for information only

* [rrdd_proxy.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/rrdd_proxy.ml#L91-L95) - in the rrd handler to redirect requests to the VM's host

```ocaml
let owner = Db.VM.get_resident_on ~__context ~self:vm_ref in
let owner_uuid = Ref.string_of owner in
let is_owner_localhost = (owner_uuid = localhost_uuid) in
if is_owner_localhost then (
  if is_master then unarchive () else unarchive_at_master ()
) else (
  if is_owner_online owner && not is_xapi_initialising
  then read_at_owner owner
  else unarchive_at_master ()
)
```

* [xapi_bond.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/xapi_bond.ml#L81-L92) - used to determine if the VM is current running on this host or not to decide whether VIFs should be moved:

```ocaml
let is_local vm =
  (* Only move the VIFs of a VM if this VM is resident, or can _only_ start on _this_ host or nowhere. *)
  (* Do the latter check only if needed, as it is expensive. *)
  let resident_on = Db.VM.get_resident_on ~__context ~self:vm in
  if resident_on = host then
    true
  else if resident_on <> Ref.null then
    false
  else begin
    let hosts = Xapi_vm.get_possible_hosts ~__context ~vm in
    (List.mem host hosts && List.length hosts = 1) || (List.length hosts = 0)
  end
```

* [xapi_bond.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/xapi_bond.ml#L156-L161) - moves VIFs on VMs resident on this host when executing `move_vlan`:

```ocaml
(* Call Xapi_vif.move_internal on VIFs of running VMs to make sure they end up on the right vSwitch *)
  let vifs = Db.Network.get_VIFs ~__context ~self:network in
  let vifs = List.filter (fun vif ->
      Db.VM.get_resident_on ~__context ~self:(Db.VIF.get_VM ~__context ~self:vif) = host)
    vifs in
  ignore (List.map (Xapi_vif.move_internal ~__context ~network:network) vifs);
```

* [xapi_bond.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/xapi_bond.ml#L184-L189) - similar to the above (identical!):

```ocaml
(* Call Xapi_vif.move_internal to make sure vifs end up on the right vSwitch *)
let vifs = Db.Network.get_VIFs ~__context ~self:network in
let vifs = List.filter (fun vif ->
    Db.VM.get_resident_on ~__context ~self:(Db.VIF.get_VM ~__context ~self:vif) = host)
  vifs in
ignore (List.map (Xapi_vif.move_internal ~__context ~network:network) vifs);
```

* [xapi_gpumon.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/xapi_gpumon.ml#L119-L120) - only for reporting the host in an error:

```ocaml
let host = Db.VM.get_resident_on ~__context ~self:vm in
raise Api_errors.(Server_error (nvidia_tools_error, [Ref.string_of host]))
```

* [xapi_ha_vm_failover.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/xapi_ha_vm_failover.ml#L176-L184): Used for the planner in checking memory:

```ocaml
(* If a VM is marked as resident on a live_host then it will already be accounted for in the host's current free memory. *)
  let vm_accounted_to_host vm =
    let vm_t = List.assoc vm vms_to_ensure_running in
    if List.mem vm_t.API.vM_resident_on live_hosts
    then Some vm_t.API.vM_resident_on
    else
      let scheduled = Db.VM.get_scheduled_to_be_resident_on ~__context ~self:vm in
      if List.mem scheduled live_hosts
      then Some scheduled else None in
```

* [xapi_host.ml](https://github.com/xapi-project/xen-api/blob/28bdafcb5514f3c621618b22c19269aa853bcd83/ocaml/xapi/xapi_host.ml#L117-L129) - used when checking for `bacon_mode`:

```ocaml
let control_domain_vbds =
  List.filter (fun vm ->
      Db.VM.get_resident_on ~__context ~self:vm = host
      && Db.VM.get_is_control_domain ~__context ~self:vm
    ) (Db.VM.get_all ~__context)
  |> List.map (fun self -> Db.VM.get_VBDs ~__context ~self)
  |> List.flatten
  |> List.filter (fun self -> Db.VBD.get_currently_attached ~__context ~self) in
if List.length control_domain_vbds > 0 then
  raise (Api_errors.Server_error (
      Api_errors.host_in_use,
      [ selfref; "vbd"; List.hd (List.map Ref.string_of control_domain_vbds) ]
    ));
```
