---
title: NUMA Support
layout: documentation
documentation: true
---

# Table of Contents
1. [Introduction](#Introduction)
2. [Architecture](#Architecture)
3. [Resource Isolation Interface interaction](#RII-interaction)
    1. [CgroupManager](#Setting-Memory-Requirement)
    2. [DockerManager](#Setting-Shared-Memory)
    3. [RuncLibManager](#Setting-CPU-Requirement)
4. [Configuring NUMA](#Configuring-NUMA)
    
<div id='Introduction'/>

## Introduction

Non Uniform Memory Access ([NUMA](https://www.cc.gatech.edu/~echow/ipcc/hpc-course/HPC-numa.pdf)) is a new system architecture
where the hosts resources are grouped by cores and memory in NUMA zones. Storm supports isolating/pinning worker prcesses to specific
NUMA zones via the supervisor to take advantage of this resource isolation and avoid the penalty of using cross zone bus transfers
<div id='Architecture'/>

## Architecture

Once Storm supervisors are configured for NUMA (see section below) they now heartbeat multiple heartbeats - one for each NUMA zone.
Each of these NUMA supervisors have a supervisor id with the same prefixed supervisor id with the NUMA id differentiating them.
Nimbus, and by extension the scheduler, see these heartbeats and views the supervisor as multiple supervisors - one per configured NUMA zone. 
Nimbus schedules topologies and assignments accordingly. The supervisor reads all assignments with the prefixed assignments and then
pins each worker to the NUMA zone according to the numa id in the assignment. The pinning depends on the Resource Isolation Interface used
and is elaborated on in the following section.

<div id='RII-Interaction'/>

### Resource Isolation Interface interaction

Each implementation of the Resource Isolation Interface (RII) should now implement NUMA pinning. The following are the current/soon to be available
implementations                                                                                                                                                  

#### CgroupManager

The CgroupManager prefixes the worker launch command with the numactl command for Linux hosts-

```
numactl --cpunodebind=<numaId>> --membind=<numaId> <worker launch command>
```

The worker is then bound to the NUMA zone's CPU cores and memory zone.
#### DockerManager

Will be updated upon adding Docker support

#### RuncLibManager

Will be updated upon adding Runc support

<div id='Configuring NUMA'/>

### Configuring NUMA

In the Supervisor Config the following settings need to be set

```
supervisor.numa.meta:
    "0": # Numa zone id
        numa.cores: # Cores in NUMA zone (can be determined by using the numastat command)
            - 0
            - 1
            - 2
            - 3
            - 4
            - 5
            - 12
            - 13
            - 14
            - 15
            - 16

        numa.generic.resources.map: # Generic Resources in the zone to be used for generic resource scheduling (optional)
            network.resource.units: 50.0

        numa.memory.mb: 42461 # Size of memory zone
        numa.ports: # Ports to be assigned to workers pinned to the NUMA zone (this may include ports not specified in SUPERVISOR_SLOTS_PORTS
            - 6700
            - 6701
            - 6702
            - 6703
            - 6704
            - 6705
            - 6706
            - 6707
            - 6708
            - 6709
            - 6710
            - 6711
```