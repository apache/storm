---
title: CGroup Enforcement
layout: documentation
documentation: true
---

# CGroups in Storm

CGroups are used by Storm to limit the resource usage of workers to guarantee fairness and QOS.  

**Please note: CGroups are currently supported only on Linux platforms (kernel version 2.6.24 and above)** 

## Setup

To use CGroups make sure to install cgroups and configure cgroups correctly.  For more information about setting up and configuring, please visit:

https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/ch-Using_Control_Groups.html

A sample/default cgconfig.conf file is supplied in the <stormroot>/conf directory.  The contents are as follows:

```
mount {
	cpuset	= /cgroup/cpuset;
	cpu	= /cgroup/storm_resources;
	cpuacct	= /cgroup/storm_resources;
	memory	= /cgroup/storm_resources;
	devices	= /cgroup/devices;
	freezer	= /cgroup/freezer;
	net_cls	= /cgroup/net_cls;
	blkio	= /cgroup/blkio;
}

group storm {
       perm {
               task {
                      uid = 500;
                      gid = 500;
               }
               admin {
                      uid = 500;
                      gid = 500;
               }
       }
       cpu {
       }
       memory {
       }
       cpuacct {
       }
}
```

For a more detailed explanation of the format and configs for the cgconfig.conf file, please visit:

https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/ch-Using_Control_Groups.html#The_cgconfig.conf_File

To let storm manage the cgroups for individual workers you need to make sure that the resources you want to control are mounted under the same directory as in the example above.
If they are not in the same directory the supervisor will throw an exception.

The perm section needs to be configured so that the user the supervisor is running as can modify the group.

If "run as user" is enabled so that the supervisor spawns other processes as the user that launched the topology, make sure that the permissions are such that individual users have read access but not write access.

# Settings Related To CGroups in Storm

| Setting                       | Function                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| storm.resource.isolation.plugin.enable  | This config is used to set whether a resource isolation plugin will be used. Default set to "false". When this config is set to false, unit tests related to cgroups will be skipped.                                                                                                                                                                                                                                                              |
| storm.resource.isolation.plugin| Select a resource isolation plugin to use when `storm.resource.isolation.plugin.enable` is set to true. Default to "org.apache.storm.container.cgroup.CgroupManager" |
| storm.cgroup.hierarchy.dir   | The path to the cgroup hierarchy that storm will use.  Default set to "/cgroup/storm_resources"                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| storm.cgroup.resources       | A list of subsystems that will be regulated by CGroups. Default set to cpu and memory.  Currently only cpu and memory are supported                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| storm.supervisor.cgroup.rootdir     | The root cgroup used by the supervisor.  The path to the cgroup will be \<storm.cgroup.hierarchy.dir>/\<storm.supervisor.cgroup.rootdir>.  Default set to "storm"                                                                                                                                                                                                                                                                                                                                                                           |
| storm.cgroup.cgexec.cmd            | Absolute path to the cgexec command used to launch workers within a cgroup. Default set to "/bin/cgexec"                                                                                                                                                                                                                                                                                                                                                                                                                            |
| storm.worker.cgroup.memory.mb.limit | The memory limit in MB for each worker.  This can be set on a per supervisor node basis.  This config is used to set the cgroup config memory.limit_in_bytes.  For more details about memory.limit_in_bytes, please visit:  https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/sec-memory.html.    Please note, if you are using the Resource Aware Scheduler, please do NOT set this config as this config will override the values calculated by the Resource Aware Scheduler |
| storm.worker.cgroup.cpu.limit       | The cpu share for each worker. This can be set on a per supervisor node basis.  This config is used to set the cgroup config cpu.share. For more details about cpu.share, please visit:   https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/sec-cpu.html. Please note, if you are using the Resource Aware Scheduler, please do NOT set this config as this config will override the values calculated by the Resource Aware Scheduler.                                       |

Since limiting CPU usage via cpu.shares only limits the proportional CPU usage of a process, to limit the amount of CPU usage of all the worker processes on a supervisor node, please set the config supervisor.cpu.capacity. Where each increment represents 1% of a core thus if a user sets supervisor.cpu.capacity: 200, the user is indicating the use of 2 cores.

## Integration with Resource Aware Scheduler

CGroups can be used in conjunction with the Resource Aware Scheduler.  CGroups will then enforce the resource usage of workers as allocated by the Resource Aware Scheduler.  To use cgroups with the Resource Aware Scheduler, simply enable cgroups and be sure NOT to set storm.worker.cgroup.memory.mb.limit and storm.worker.cgroup.cpu.limit configs.

# CGroup Metrics

CGroups not only can limit the amount of resources a worker has access to, but it can also help monitor the resource consumption of a worker.  There are several metrics enabled by default that will check if the worker is a part of a CGroup and report corresponding metrics.

## CGroupCPU

org.apache.storm.metrics2.cgroup.CGroupCPU reports metrics similar to org.apache.storm.metrics.sigar.CPUMetric, but for everything within the CGroup.  It reports both user and system CPU usage in ms. 

```
   "CGroupCPU.user-ms": number
   "CGroupCPU.sys-ms": number
```

CGroup reports these as CLK_TCK counts, and not milliseconds so the accuracy is determined by what CLK_TCK is set to.  On most systems it is 100 times a second so at most the accuracy is 10 ms.

To make these metrics work cpuacct must be mounted.

## CGroupCpuGuarantee

org.apache.storm.metrics2.cgroup.CGroupCpuGuarantee reports back an approximate number of ms of CPU time that this worker is guaranteed to get.  This is calculated from the resources requested by the tasks in that given worker.

## CGroupCpuGuaranteeByCfsQuota

org.apache.storm.metrics2.cgroup.CGroupCpuGuaranteeByCfsQuota reports the percentage of the cpu guaranteed for the worker from cpu.cfs_period_us and cpu.cfs_quota_us.

## CGroupCpuStat

org.apache.storm.metrics2.cgroup.CGroupCpuStat reports the bandwidth statistics of the CGroup. It includes
```
   "CGroupCpuStat.nr.period-count": number
   "CGroupCpuStat.nr.throttled-count": number
   "CGroupCpuStat.nr.throttled-percentage": number
   "CGroupCpuStat.throttled.time-ms": number
```

It is based on the following `cpu.stat`:
  - `nr_periods`: Number of enforcement intervals that have elapsed.
  - `nr_throttled`: Number of times the group has been throttled/limited.
  - `throttled_time`: The total time duration (in nanoseconds) for which entities of the group have been throttled.

And the reported metrics are
  - `nr.period-count`: the difference of `nr_periods` between two consecutive reporting cycles
  - `nr.throttled-count`: the difference of `nr_throttled` between two consecutive reporting cycles
  - `nr.throttled-percentage`: (`nr.throttled-count` / `nr.period-count`)
  - `throttled.time-ms`: the difference of `throttled_time` in milliseconds between two consecutive reporting cycles

Note: when `org.apache.storm.container.docker.DockerManager` or `org.apache.storm.container.oci.RuncLibContainerManager` is used as `storm.resource.isolation.plugin`, use `org.apache.storm.metric.cgroup.CGroupCpuGuaranteeByCfsQuota` instead.

## CGroupMemory

org.apache.storm.metrics2.cgroup.CGroupMemoryUsage reports the current memory usage of all processes in the cgroup in bytes

## CGroupMemoryLimit

org.apache.storm.metrics2.cgroup.CGroupMemoryLimit report the current limit in bytes for all of the processes in the cgroup.  If running with CGroups enabled in storm this is the on-heap request + the off-heap request for all tasks within the worker + any extra slop space given to workers.

## Usage/Debugging CGroups in your topology

These metrics can be very helpful in debugging what has happened or is happening to your code when it is running under a CGroup.

### CPU

CPU guarantees under storm are soft.  It means that a worker can ea sly go over their guarantee if there is free CPU available.  To detect that your worker is using more CPU then it requested you can sum up the values in CGroupCPU and compare them to CGroupCpuGuarantee.  
If CGroupCPU is consistently higher then or equal to CGroupCpuGuarantee you probably want to look at requesting more CPU as your worker may be starved for CPU if more load is placed on the cluster.  Being equal to CGroupCpuGuarantee means your worker may already
be throttled.  If the used CPU is much smaller than CGroupCpuGuarantee then you are probably wasting resources and may want to reduce your CPU ask.

If you do have high CPU you probably also want to check out the GC metrics and/or the GC log for your worker.  Memory pressure on the heap can result in increased CPU as garbage collection happens.

### Memory

Memory debugging of java under a cgroup can be difficult for multiple reasons.

1. JVM memory management is complex
2. As of the writing of this documentation only experimental support for cgroups is in a few JVMs
3. JNI and other processes can use up memory within the cgroup that the JVM is not always aware of.
4. Memory pressure within the heap can result in increased CPU load instead of increased memory allocation.

There are several metrics that storm provides by default that can help you understand what is happening within your worker.

If CGroupMemory gets close to CGroupMemoryLimit then you know that bad things are likely to start happening soon with this worker.  Memory is not a soft guarantee like CPU.
If you go over the OOM killer on Linux will start to shoot processes withing your worker.  Please pay attention to these metrics.  If you are running a version of java that
is cgroup aware then going over the limit typically means that you will need to increase your off heap request.  If you are not, it could be that you need more off heap
memory or it could be that java has allocated more memory then it should have as part of the garbage collection process.  Figuring out which is typically best done with
trial and error (sorry).

Storm also reports the JVM's on heap and off heap usage through the "memory/heap" and "memory/nonHeap" metrics respectively.  These can be used to give you a hint on 
which to increase.  Looking at the "usedBytes" field under each can help you understand how much memory the JVM is currently using.  Although, like I said the off heap
portion is not always accurate and when the heap grows it can result in unrecorded off heap memory that will cause the cgroup to shoot processes.

The name of the GC metrics vary based off of the garbage collector you use, but they all start with "GC/".  If you sum up all of the "GC/*.timeMs" metrics for a given worker/window pair
you should be able to see how much of the CPU guarantee went to GC.  By default java allows 98% of cpu time to go towards GC before it throws an OutOfMemoryError.  This is far from ideal
for a near real time streaming system so pay attention to this ratio.

If the ratio is at a fairly steady state and your memory usage is not even close to the limit you might want to look at reducing your memory request.  This too can be complicated to figure
out.

## Future Work

There is a lot of work on adding in elasticity to storm.  Eventually we hope to be able to do all of the above analysis for you and grow/shrink your topology on demand.
