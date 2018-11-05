---
title: Docker Support
layout: documentation
documentation: true
---

# Docker Support

This page describes how storm supervisor launches the worker in a docker container. 

### Motivation

This is mostly about security. With workers running inside of docker containers, we isolate running user code from each other and from the hosted machine so that the whole system is less vulnerable to attack.

## Implementation

The implementation is pretty easy to understand. Essentially, `DockerManager` composes a docker-run command and uses `worker-launcher` executable to execute the command 
to launch a container. The `storm-worker-script.sh` script to run when executing the container is the actual command to launch the worker process and logviewer in the container.
One container Id is mapped to one worker Id conceptually. When the worker process dies, the container exits. 

For security, when the supervisor launches the docker container, it makes the whole container read-only except some explicit bind mount locations.
It also drops all the kernel capabilities and disables container processes from gaining new privileges. 

Because of no new privileges is obtainable, `jstack` and other java debugging tools cannot be used directly in the container. 
We need to install `nscd` and have it running in the system. Storm will bind mount nscd directory when it launches the container. 
And `nsenter` will be used to enter the docker container to run the standard JVM debugging tools. This functionality is also implemented in `worker-launcher` executable.

The command that will be run by `worker-launcher` executable to launch a container will be something like:

```bash
run --name=06f8ddbd-88d8-454a-acf1-f9d1f3e6baec \
--user=1001:1003 \
--net=host \
--read-only \
-v /sys/fs/cgroup:/sys/fs/cgroup:ro \
-v /usr/share/apache-storm-2.0.0:/usr/share/apache-storm-2.0.0:ro \
-v /home/y/var/storm/supervisor:/home/y/var/storm/supervisor:ro \
-v /home/y/var/storm/workers/06f8ddbd-88d8-454a-acf1-f9d1f3e6baec:/home/y/var/storm/workers/06f8ddbd-88d8-454a-acf1-f9d1f3e6baec \
-v /home/y/var/storm/workers-artifacts/wc-1-1539979318/6700:/home/y/var/storm/workers-artifacts/wc-1-1539979318/6700 \
-v /home/y/var/storm/workers-users/06f8ddbd-88d8-454a-acf1-f9d1f3e6baec:/home/y/var/storm/workers-users/06f8ddbd-88d8-454a-acf1-f9d1f3e6baec \
-v /var/run/nscd:/var/run/nscd \
-v /home/y/var/storm/supervisor/stormdist/wc-1-1539979318/shared_by_topology/tmp:/tmp \
--cgroup-parent=storm.slice \
--group-add 1003 \
--workdir=/home/y/var/storm/workers/06f8ddbd-88d8-454a-acf1-f9d1f3e6baec \
--cidfile=/home/y/var/storm/workers/06f8ddbd-88d8-454a-acf1-f9d1f3e6baec/container.cid \
--cap-drop=ALL \
--security-opt no-new-privileges \
--cpus=1.0 \
xxx.xxx.com:8080/storm/docker_images/rhel6:latest \
bash /home/y/var/storm/workers/06f8ddbd-88d8-454a-acf1-f9d1f3e6baec/storm-worker-script.sh
```


## Setup

To make supervisor work with docker, you need to configure related settings correctly following the instructions below.

### Settings Related To Docker Support in Storm

| Setting                                   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `storm.resource.isolation.plugin.enable`  | set to `true` to enable isolation plugin. `storm.resource.isolation.plugin` determines which plugin to use. If this is set to `false`, `org.apache.storm.container.DefaultResourceIsolationManager` will be used.                                                                                                                                                                                                                                           |
| `storm.resource.isolation.plugin`         | set to `"org.apache.storm.container.docker.DockerManager"` to enable docker support                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `storm.docker.allowed.images`             | A whitelist of docker images that can be used. Users can only choose a docker image from the list.
| `storm.docker.image`                      | The default docker image to be used if user doesn't specify which image to use. And it must belong to the `storm.docker.allowed.images` 
| `supervisor.worker.launcher`              | Full path to the worker-launcher executable. Details explained at [How to set up worker-launcher](#how-to-set-up-worker-launcher)
| `storm.docker.cgroup.root`                | The root path of cgroup for docker to use. On RHEL7, it should be "/sys/fs/cgroup".
| `storm.docker.cgroup.parent`              | --cgroup-parent config for docker command. It must follow the constraints of docker commands. Additionally, '-' is not allowed in the name since it makes the cgroup hierarchy unnecessarily complicated.
| `storm.docker.cgroup.sub.path.template`   | The template for cgroup sub path. Details explained at [How to configure cgroup related settings](#how-to-configure-cgroup-related-settings)
| `storm.docker.readonly.bindmounts`        | A list of read only bind mounted directories.
| `storm.docker.nscd.dir`                   | The directory of nscd (name service cache daemon), e.g. "/var/run/nscd/". nscd must be running so that profiling can work properly.
| `storm.docker.seccomp.profile`            | White listed syscalls seccomp Json file to be used as a seccomp filter
| `storm.local.dir`                         | This is not a new config and it's not specific to docker support. But it must not be under `STORM_HOME` directory because of the way how we bind mount directories. 
| `storm.workers.artifacts.dir`             | This is not a new config and it's not specific to docker support. But it must not be under `STORM_HOME` directory because of the way how we bind mount directories. 

### How to set up worker-launcher

The `worker-launcher` executable is a special program that is used to launch docker containers, run `docker` and `nsenter` commands.
For this to work, `worker-launcher` needs to be owned by root, but with the group set to be a group that only the supervisor headless user is a part of. 
`worker-launcher` also needs to have `6550 octal permissions. There is also a `worker-launcher`.cfg file, usually located under `/etc/`, that should look something like the following:
```
storm.worker-launcher.group=$(worker_launcher_group)
min.user.id=$(min_user_id)
worker.profiler.script.path=$(path_to_profiler_script)
```
where `storm.worker-launcher.group` is the same group the supervisor user is a part of, and `min.user.id` is set to the first real user id on the system. This config file also needs to be owned by root and not have world nor group write permissions. 
`worker.profiler.script.path` points to the profiler script. For security, the script should be only writable by root. Note that it's the only profiler script that will be used and `DaemonConfig.WORKER_PROFILER_COMMAND` will be ignored.

There are two optional configs that will be used by docker support: `docker.binary` and `nsenter.binary`. By default, they are set to
```
docker.binary=/usr/bin/docker
nsenter.binary=/usr/bin/nsenter
```
and you don't need to set them in the worker-launcher.cfg unless you need to change them.


### How to configure cgroup related settings

We let docker handle cgroup by itself. But we need to know the cgroup path for memory (or maybe other subsystems in the future) so that we can inspect the memory usage of the worker and the memory limit of the system. 
Depending on whether `docker` or `podman` is used as the binary, whether `cgroupfs` or `systemd` is used as the `cgroupdriver` (`cgroup_manager` in podman), whether the `--cgroup-parent` is an absolute or relative path, 
the cgroup path for memory (or other subsytems) is different. You need to configure `storm.docker.cgroup.sub.path.template` properly. Here are some possible cases and it's up to storm admins to configure correct template for cgroup path.

|binary | cgroupdriver     |cgroup-parent path    |template                                     |
|-------|------------------|----------------------|---------------------------------------------|
|docker | cgroupfs         |absolute              |  "/%CG-PARENT%/docker-%CONTAINER-ID%.scope" |
|docker | cgroupfs         |relative              |  "/%CG-PARENT%/%CONTAINER-ID%"              |
|podman | systemd          |absolute or relative  |  "/%CG-PARENT%/libpod-%CONTAINER-ID%.scope" |
|podman | cgroupfs         |absolute or relative  |  "/%CG-PARENT%/libpod-%CONTAINER-ID%"       |

`%CG-PARENT%` will be replaced with the value of `storm.docker.cgroup.parent`.
`%CONTAINER-ID%` will be replaced with the container id; and it must be in and only in the deepest level of this sub-path.

You also need to configure `storm.docker.cgroup.root` and `storm.docker.cgroup.parent` properly. So the full cgroup path for memory would be like:
`{storm.docker.cgroup.root}`/memory/`{storm.docker.cgroup.sub.path.template}` with values being replaced;
and the full cgroup path for cpu would be like:
`{storm.docker.cgroup.root}`/cpu/`{storm.docker.cgroup.sub.path.template}` with values being replaced.


## Podman
Podman implements almost all the Docker CLI commands. Technically they are interchangeable and replacing `docker` with `podman` in the commands will just work. 
To use podman instead of docker, set `docker.binary` in `worker-launcher.cfg` to the full path of podman executable.
However, we haven't really tested it in storm so please use it with caution. Or better not to use it until it's fully tested.
