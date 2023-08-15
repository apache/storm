---
title: Docker Support
layout: documentation
documentation: true
---

# Docker Support

This page describes how storm supervisor launches the worker in a docker container. 

Note: This has only been tested on RHEL7.

## Motivation

This feature is mostly about security and portability. With workers running inside of docker containers, we isolate running user code from each other and from the hosted machine so that the whole system is less vulnerable to attack. 
It also allows users to run their topologies on different os versions using different docker images.

## Implementation

Essentially, `DockerManager` composes a docker-run command and uses `worker-launcher` executable to execute the command 
to launch a container. The `storm-worker-script.sh` script is the actual command to launch the worker process and logviewer in the container.
One container ID is mapped to one worker ID conceptually. When the worker process dies, the container exits. 

For security, when the supervisor launches the docker container, it makes the whole container read-only except some explicit bind mount locations.
It also drops all the kernel capabilities and disables container processes from gaining new privileges. 

For security reasons, we can drop privileges of containers like PTRACE. Consequently, `jmap`, `strace` and some other debugging tools cannot be used directly in the container when entered with docker-exec command. 
We need to install `nscd` and have it running in the system. Storm will bind mount nscd directory when it launches the container. 
And `nsenter` will be used to enter the docker container without losing privileges. This functionality is also implemented in `worker-launcher` executable.

The command that will be run by `worker-launcher` executable to launch a container will be something like:

```bash
run --name=8198e1f0-f323-4b9d-8625-e4fd640cd058 \
--user=<uid>:<gid> \
-d \
--net=host \
--read-only \
-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
-v /usr/share/apache-storm-2.3.0:/usr/share/apache-storm-2.3.0:ro \
-v /<storm-local-dir>/supervisor:/<storm-local-dir>/supervisor:ro \
-v /<storm-local-dir>/workers/8198e1f0-f323-4b9d-8625-e4fd640cd058:/<storm-local-dir>/workers/8198e1f0-f323-4b9d-8625-e4fd640cd058 \
-v /<workers-artifacts-dir>/workers-artifacts/word-count-1-1591895933/6703:/<workers-artifacts-dir>/workers-artifacts/word-count-1-1591895933/6703 \
-v /<storm-local-dir>/workers-users/8198e1f0-f323-4b9d-8625-e4fd640cd058:/<storm-local-dir>/workers-users/8198e1f0-f323-4b9d-8625-e4fd640cd058 \
-v /var/run/nscd:/var/run/nscd \
-v /<storm-local-dir>/supervisor/stormdist/word-count-1-1591895933/shared_by_topology:/<storm-local-dir>/supervisor/stormdist/word-count-1-1591895933/shared_by_topology \
-v /<storm-local-dir>/workers/8198e1f0-f323-4b9d-8625-e4fd640cd058/tmp:/tmp \
-v /etc/storm:/etc/storm:ro \
--cgroup-parent=/storm \
--group-add <gid> \
--workdir=/<storm-local-dir>/workers/8198e1f0-f323-4b9d-8625-e4fd640cd058 \
--cidfile=/<storm-local-dir>/workers/8198e1f0-f323-4b9d-8625-e4fd640cd058/container.cid \
--cap-drop=ALL \
--security-opt no-new-privileges \
--security-opt seccomp=/usr/share/apache-storm-2.3.0/conf/seccomp.json \
--cpus=2.6 xxx.xxx.com:8080/storm/storm/rhel7:latest \
bash /<storm-local-dir>/workers/8198e1f0-f323-4b9d-8625-e4fd640cd058/storm-worker-script.sh
```


## Setup

To make supervisor work with docker, you need to configure related settings correctly following the instructions below.

### Settings Related To Docker Support in Storm

| Setting                                   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `storm.resource.isolation.plugin.enable`  | set to `true` to enable isolation plugin. `storm.resource.isolation.plugin` determines which plugin to use. If this is set to `false`, `org.apache.storm.container.DefaultResourceIsolationManager` will be used.                                                                                                                                                                                                                                           |
| `storm.resource.isolation.plugin`         | set to `"org.apache.storm.container.docker.DockerManager"` to enable docker support                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `storm.oci.allowed.images`             | An allowlist of docker images that can be used. Users can only choose a docker image from the list.
| `storm.oci.image`                      | The default docker image to be used if user doesn't specify which image to use. And it must belong to the `storm.oci.allowed.images` 
| `topology.oci.image`                   | Topologies can specify which image to use. It must belong to the `storm.oci.allowed.images` |
| `storm.oci.cgroup.root`                | The root path of cgroup for docker to use. On RHEL7, it should be "/sys/fs/cgroup".
| `storm.oci.cgroup.parent`              | --cgroup-parent config for docker command. It must follow the constraints of docker commands. The path will be made as absolute path if it's a relative path because we saw some weird bugs ((the cgroup memory directory disappears after a while) when a relative path is used.
| `storm.oci.readonly.bindmounts`        | A list of read only bind mounted directories.
| `storm.oci.readwrite.bindmounts`        | A list of read write bind mounted directories.
| `storm.oci.nscd.dir`                   | The directory of nscd (name service cache daemon), e.g. "/var/run/nscd/". nscd must be running so that profiling can work properly.
| `storm.oci.seccomp.profile`            | Specify the seccomp Json file to be used as a seccomp filter
| `supervisor.worker.launcher`              | Full path to the worker-launcher executable. Details explained at [How to set up worker-launcher](#how-to-set-up-worker-launcher)

Note that we only support cgroupfs cgroup driver because of some issues with `systemd` cgroup driver; restricting to `cgroupfs` also makes cgroup paths simpler. Please make sure to use `cgroupfs` before setting up docker support.

#### Example

Below is a simple configuration example for storm on Rhel7. In this example, storm is deployed at `/usr/share/apache-storm-2.3.0`.

```bash
storm.resource.isolation.plugin.enable: true
storm.resource.isolation.plugin: "org.apache.storm.container.docker.DockerManager"
storm.oci.allowed.images: ["xxx.xxx.com:8080/storm/storm/rhel7:latest"]
storm.oci.image: "xxx.xxx.com:8080/storm/storm/rhel7:latest"
storm.oci.cgroup.root: "/storm"
storm.oci.cgroup.parent: "/sys/fs/cgroup"
storm.oci.readonly.bindmounts:
    - "/etc/storm"
storm.oci.nscd.dir: "/var/run/nscd"
supervisor.worker.launcher: "/usr/share/apache-storm-2.3.0/bin/worker-launcher"
```

### How to set up worker-launcher

The `worker-launcher` executable is a special program that is used to launch docker containers, run `docker` and `nsenter` commands.
For this to work, `worker-launcher` needs to be owned by root, but with the group set to be a group that only the supervisor headless user is a part of. 
`worker-launcher` also needs to have `6550` octal permissions. There is also a `worker-launcher.cfg` file, usually located under `/etc/storm`, that should look something like the following:
```
storm.worker-launcher.group=$(worker_launcher_group)
min.user.id=$(min_user_id)
worker.profiler.script.path=$(profiler_script_path)
```
where `storm.worker-launcher.group` is the same group the supervisor user is a part of, and `min.user.id` is set to the first real user id on the system. This config file also needs to be owned by root and not have world nor group write permissions. 
`worker.profiler.script.path` points to the profiler script. For security, the script should be only writable by root. Note that it's the only profiler script that will be used and `DaemonConfig.WORKER_PROFILER_COMMAND` will be ignored.

There are two optional configs that will be used by docker support: `docker.binary` and `nsenter.binary`. By default, they are set to
```
docker.binary=/usr/bin/docker
nsenter.binary=/usr/bin/nsenter
```
and you don't need to set them in the worker-launcher.cfg unless you need to change them.

## Profile the processes inside the container
You can profile your worker processes by clicking on the profiling buttons (jstack, heap, etc) on storm UI.
If you have sudo permission, you can also run `sudo nsenter --target <container-pid> --pid --mount --setuid <uid> --setgid <gid>` to enter the container. 
Then you can run `jstack`, `jmap` etc inside the container. `<container-pid>` is the pid of the container process on the host.
`<container-pid>` can be obtained by running `sudo docker inspect --format '{{.State.Pid}}' <container-id>` command. 
`<uid>` and `<gid>` are the user id and group id of the container owner, respectively.

## Seccomp security profiles

You can set `storm.oci.seccomp.profile` to restrict the actions available within the container. If it's not set, the [default docker seccomp profile](https://github.com/moby/moby/blob/master/profiles/seccomp/default.json)
is used. You can use `conf/seccomp.json.example` provided or you can specify our own `seccomp.json` file.

## CGroup Metrics

Docker internally uses cgroups to control resources for containers. The CGroup Metrics described at [cgroups_in_storm.md](cgroups_in_storm.md#CGroup-Metrics) still apply except CGroupCpuGuarantee. To get CGroup cpu guarantee, use CGroupCpuGuaranteeByCfsQuota instead.