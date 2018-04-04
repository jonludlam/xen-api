# What is a pool?

A XenServer pool is a collection of hosts that operate under the control of a specified pool master. This is useful for several different but related reasons. This document discusses these
reasons and attempts to separate out the different use cases to help direct the future evolution of the toolstack. Note that some of the issues here were discussed previously with [relation to cloudstack/openstack](https://code.citrite.net/projects/XSC/repos/xs-documents/browse/architecture/evoplan/clustering_pool_management)

## What does a pool give us?

There are several slices through the functionality of XenServer that make Pools useful. These are discussed in more detail elsewhere but are outlined here for reference.

### Management

* XenCenter connects to the one pool master and receives all updates for all VMs on all of the pool members.
* Should a host fail, even the master, a new host can be set up to take its place without significant interruption of services.
* Upgrades are done without taking down VMs at the pool level (rolling pool upgrade).
* Resource management. Limited resources such as host memory and VGPUs are managed at the pool level such that, for example, a `VM.start` API call can come in and the VM will be started on a suitable host.

### Storage

* Currently storage repositories are either accessible to one host in the pool or to _all_ hosts in the pool. This allows hosts to be treated as interchangable which is useful for HA / maintenance / etc.
* Currently writable storage repositories must be exclusively used by only one pool.

### Networking

* Shared network configuration. Networking is set up at a pool level so VMs can be started or migrated to/from any host.

### High Availability

* HA ensures there is always a pool master to talk to and to make decisions
* HA for VM protection - an analysis of the number of host failures that can be tolerated and still keep the important VMs running is done at the pool level.

## Questions:

* Why is managability anything to do with HA? - DB persistence, election of master.
* Why can't you talk to any host?
* Why are we limited to small pool sizes?

## What might we do instead?

"pools of one" from dave's doc
replication of db (to shared storage? dr style?)
cross-pool SRs

