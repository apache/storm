---
title: OCI/Squashfs Runtime
layout: documentation
documentation: true
---

# OCI/Squashfs Runtime for Workers Running in Containers

OCI/Squashfs is a container runtime that allows topologies to run inside docker containers. However, unlike the existing
Docker runtime, the images are fetched from HDFS rather than from the Docker registry or requiring images to be pre-loaded
into Docker on each node. Docker does not need to be installed on the nodes in order for this runtime to work.

Note: This has only been tested on RHEL7.

## Motivation

#### Docker runtime drawbacks
Using the current Docker runtime (see [Docker-support.md](Docker-support.md#Docker-Support) ) has some drawbacks:

##### Docker Daemons Dependency

The Docker daemons `dockerd` and `containerd` must be running on the system in order for the Docker runtime to function. 
And these daemons can get out of sync which could cause nontrivial issues to the containers.

##### Docker Registry Issues at Scale

Using the Docker runtime on a large scale Storm cluster can overwhelm the Docker registry. In practice this requires
admins to pre-load a Docker image on all the cluster nodes in a controlled fashion before a large job requesting 
the image can run.

##### Image Costs in Time and Space

Docker stores each image layer as a tar.gz archive. In order to use the layer, the compressed archive must be unpacked
into the node's filesystem. This can consume significant disk space, especially when the reliable image store location
capacity is relatively small. In addition, unpacking an image layer takes time, especially when the layer is large or 
contains thousands of files. This additional time for unpacking delays container launch beyond the time needed to transfer
the layer data over the network.

#### OCI/Squashfs Runtime advantages

The OCI/Squashfs runtime avoids the drawback listed above in the following ways.

##### No Docker dependencies on The Node

Docker does not need to be installed on each node, nor is there a dependency on a daemon or service that needs to be started
by an admin before containers can be launched. All that is required to be present on each node is an OCI-compatible runtime like
`runc`.

##### Leverages Distributed File Sytems For Scale

Image can be fetched via HDFS or other distributed file systems instead of the Docker registry. This prevents a large cluster from
overwhelming a Docker registry when a big topology causes all of the nodes to request an image at once. This also allows large clusters
to run topologies more dynamically, as images would not need to be pre-loaded by admins on each node to prevent a large Docker registry
image request storm.

##### Smaller, Faster images on The Node

The new runtime handles layer localization directly, so layer formats other than tar archive can be supported. For example, each image layer
can be converted to squashfs images as part of copying the layers to HDFS. squashfs is a file system optimized for running directly on a
compressed image. With squashfs layers the layer data can remain compressed on the node saving disk space. Container launch after layer
localization is also faster, as the layers no longer need to be unpacked into a directory to become usable.


## Prerequisite 

First you need to use the`docker-to-squash.py` script to download docker images and configs, convert layers to squashfs files and put them to a directory in HDFS, for example

```bash
python docker-to-squash.py pull-build-push-update --hdfs-root hdfs://hostname:port/containers \
                      docker.xxx.com:4443/hadoop-user-images/storm/rhel7:20201202-232133,storm/rhel7:dev_current --log DEBUG --bootstrap
```

With this command, all the layers belonging to this image will be converted to squashfs files and be placed under `./layers` directory
under the directory specified by `--hdfs-root`; 
the manifest of this image will be placed under `./manifests` directory with the name as the sha256 value of the manifest content;
the config of this image will be placed under `./config` directory with the name as the sha256 value of the config content;
the mapping from the image tag to the sha256 value of the manifest  will be written to the "./image-tag-to-manifest-file".

Note that `--hdfs-root` can be any directory on HDFS, as long as it matches with the `storm.oci.image.hdfs.toplevel.dir` config. 

##### Example

For example, the directory structure is like this:

```bash
-bash-4.2$ hdfs dfs -ls /containers/*
Found 1 items
-r--r--r--   3 hdfsqa hadoop       7877 2020-12-04 14:29 /containers/config/ef1ff2c7167a1a6cd01e106f51b84a4d400611ba971c53cbc28de7919515ca4e
-r--r--r--   3 hdfsqa hadoop        160 2020-12-04 14:30 /containers/image-tag-to-hash
Found 7 items
-r--r--r--   3 hdfsqa hadoop   84697088 2020-12-04 14:28 /containers/layers/152ee1d2cccea9dfe6393d2bdf9d077b67616b2b417b25eb74fc5ffaadcb96f5.sqsh
-r--r--r--   3 hdfsqa hadoop  545267712 2020-12-04 14:28 /containers/layers/18ee671016a1bf3ecab07395d93c2cbecd352d59c497a1551e2074d64e1098d9.sqsh
-r--r--r--   3 hdfsqa hadoop   12906496 2020-10-06 15:24 /containers/layers/1b73e9433ecca0a6bb152bd7525f2b7c233484d51c24f8a6ba483d5cfd3035dc.sqsh
-r--r--r--   3 hdfsqa hadoop       4096 2020-12-04 14:29 /containers/layers/344224962010c03c9ca1f11a9bff0dfcc296ac46d0a55e4ff30a0ad13b9817af.sqsh
-r--r--r--   3 hdfsqa hadoop   26091520 2020-10-06 15:22 /containers/layers/3692c3483ef6516fba685b316448e8aaf0fc10bb66818116edc8e5e6800076c7.sqsh
-r--r--r--   3 hdfsqa hadoop       4096 2020-12-04 14:29 /containers/layers/8710a3d72f75b45c48ab6b9b67eb6d77caea3dac91a0c30e0831f591cba4887e.sqsh
-r--r--r--   3 hdfsqa hadoop  121122816 2020-10-06 15:23 /containers/layers/ea067172a7138f035d89a5c378db6d66c1581d98b0497b21f256e04c3d2b5303.sqsh
Found 1 items
-r--r--r--   3 hdfsqa hadoop       1793 2020-12-04 14:29 /containers/manifests/26fd443859325d5911f3be5c5e231dddca88ee0d526456c0c92dd794148d8585
```

The `image-tag-to-manifest-file`:
```bash
-bash-4.2$ hdfs dfs -cat /containers/image-tag-to-hash
storm/rhel7:dev_current:26fd443859325d5911f3be5c5e231dddca88ee0d526456c0c92dd794148d8585#docker.xxx.com:4443/hadoop-user-images/storm/rhel7:20201202-232133
```

The manifest file `26fd443859325d5911f3be5c5e231dddca88ee0d526456c0c92dd794148d8585`:
```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
  "config": {
    "mediaType": "application/vnd.docker.container.image.v1+json",
    "size": 7877,
    "digest": "sha256:ef1ff2c7167a1a6cd01e106f51b84a4d400611ba971c53cbc28de7919515ca4e"
  },
  "layers": [
    {
      "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
      "size": 26858854,
      "digest": "sha256:3692c3483ef6516fba685b316448e8aaf0fc10bb66818116edc8e5e6800076c7"
    },
    {
      "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
      "size": 123300113,
      "digest": "sha256:ea067172a7138f035d89a5c378db6d66c1581d98b0497b21f256e04c3d2b5303"
    },
    {
      "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
      "size": 12927624,
      "digest": "sha256:1b73e9433ecca0a6bb152bd7525f2b7c233484d51c24f8a6ba483d5cfd3035dc"
    },
    {
      "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
      "size": 567401434,
      "digest": "sha256:18ee671016a1bf3ecab07395d93c2cbecd352d59c497a1551e2074d64e1098d9"
    },
    {
      "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
      "size": 85748864,
      "digest": "sha256:152ee1d2cccea9dfe6393d2bdf9d077b67616b2b417b25eb74fc5ffaadcb96f5"
    },
    {
      "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
      "size": 186,
      "digest": "sha256:344224962010c03c9ca1f11a9bff0dfcc296ac46d0a55e4ff30a0ad13b9817af"
    },
    {
      "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
      "size": 156,
      "digest": "sha256:8710a3d72f75b45c48ab6b9b67eb6d77caea3dac91a0c30e0831f591cba4887e"
    }
  ]
}
```

And the config file `ef1ff2c7167a1a6cd01e106f51b84a4d400611ba971c53cbc28de7919515ca4e` (some of the content is omitted):
```json
{
  "architecture": "amd64",
  "config": {
    "Hostname": "",
    "Domainname": "",
    "User": "root",
    "AttachStdin": false,
    "AttachStdout": false,
    "AttachStderr": false,
    "Tty": false,
    "OpenStdin": false,
    "StdinOnce": false,
    "Env": [
      "X_SCLS=rh-git218",
      "LD_LIBRARY_PATH=/opt/rh/httpd24/root/usr/lib64",
      "PATH=/opt/rh/rh-git218/root/usr/bin:/home/y/bin64:/home/y/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/y/share/yjava_jdk/java/bin",
      "PERL5LIB=/opt/rh/rh-git218/root/usr/share/perl5/vendor_perl",
      "LANG=en_US.UTF-8",
      "LANGUAGE=en_US:en",
      "LC_ALL=en_US.UTF-8",
      "JAVA_HOME=/home/y/share/yjava_jdk/java"
    ],
    "Cmd": [
      "/bin/bash"
    ],
    "Image": "sha256:6977cd0735c96d14248e834f775373e40230c134b70f10163c05ce6c6c8873ca",
    "Volumes": null,
    "WorkingDir": "",
    "Entrypoint": null,
    "OnBuild": null,
    "Labels": {
      "name": "xxxxx"
    }
  },
  "container": "344ff1084dea3e0501a0d426e52c43cd589d6b29f33ab0915b7be8906b9aec41",
  "container_config": {
    "Hostname": "344ff1084dea",
    "Domainname": "",
    "User": "root",
    "AttachStdin": false,
    "AttachStdout": false,
    "AttachStderr": false,
    "Tty": false,
    "OpenStdin": false,
    "StdinOnce": false,
    "Env": [
      "X_SCLS=rh-git218",
      "LD_LIBRARY_PATH=/opt/rh/httpd24/root/usr/lib64",
      "PATH=/opt/rh/rh-git218/root/usr/bin:/home/y/bin64:/home/y/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/y/share/yjava_jdk/java/bin",
      "PERL5LIB=/opt/rh/rh-git218/root/usr/share/perl5/vendor_perl",
      "LANG=en_US.UTF-8",
      "LANGUAGE=en_US:en",
      "LC_ALL=en_US.UTF-8",
      "JAVA_HOME=/home/y/share/yjava_jdk/java"
    ],
    "Cmd": [
      "/bin/sh",
      "-c"
    ],
    "Image": "sha256:6977cd0735c96d14248e834f775373e40230c134b70f10163c05ce6c6c8873ca",
    "Volumes": null,
    "WorkingDir": "",
    "Entrypoint": null,
    "OnBuild": null,
    "Labels": {
      "name": "xxxxx"
    }
  },
  "created": "2020-12-02T23:25:47.354704574Z",
  "docker_version": "19.03.8",
  "history": [
    {
      "created": "2020-02-18T21:43:36.934503462Z",
      "created_by": "/bin/sh"
    },
    {
      "created": "2020-02-18T21:45:05.729764427Z",
      "created_by": "/bin/sh"
    },
    {
      "created": "2020-02-18T21:46:36.638896031Z",
      "created_by": "/bin/sh"
    },
    {
      "created": "2020-12-02T23:21:54.595662813Z",
      "created_by": "/bin/sh -c #(nop)  USER root",
      "empty_layer": true
    },
    {
      "created": "2020-12-02T23:25:45.822235539Z",
      "created_by": "/bin/sh -c /opt/python/bin/pip3.6 install --no-cache-dir numpy scipy pandas requests setuptools scikit-learn matplotlib"
    },
    {
      "created": "2020-12-02T23:25:46.708884538Z",
      "created_by": "/bin/sh -c #(nop)  ENV JAVA_HOME=/home/y/share/yjava_jdk/java",
      "empty_layer": true
    },
    {
      "created": "2020-12-02T23:25:46.770226108Z",
      "created_by": "/bin/sh -c #(nop)  ENV PATH=/opt/rh/rh-git218/root/usr/bin:/home/y/bin64:/home/y/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/y/share/yjava_jdk/java/bin",
      "empty_layer": true
    },
    {
      "created": "2020-12-02T23:25:46.837263533Z",
      "created_by": "/bin/sh -c #(nop) COPY file:33283617fbd796b25e53eaf4d26012eea1f610ff9acc0706f11281e86be440dc in /etc/krb5.conf "
    },
    {
      "created": "2020-12-02T23:25:47.237515768Z",
      "created_by": "/bin/sh -c echo '7.7.4' \u003e /etc/hadoop-dockerfile-version"
    }
  ],
  "os": "linux",
  "rootfs": {
    "type": "layers",
    "diff_ids": [
      "sha256:9f627fdb0292afbe5e2eb96edc1b3a5d3a8f468e3acf1d29f1509509285c7341",
      "sha256:83d2667f9458eaf719588a96bb63f2520bd377d29d52f6dbd4ff13c819c08037",
      "sha256:fcba5f49eef4f3d77d3e73e499a1a4e1914b3f20d903625d27c0aa3ab82f41a3",
      "sha256:3bd4567d0726f5d6560b548bc0c0400e868f6a27067887a36edd7e8ceafff96c",
      "sha256:ad56900a1f10e6ef96f17c7e8019384540ab1b34ccce6bda06675473b08d787e",
      "sha256:ac0a645609f957ab9c4a8a62f8646e99f09a74ada54ed2eaca204c6e183c9ae8",
      "sha256:9bf10102fc145156f4081c2cacdbadab5816dce4f88eb02881ab739239d316e6"
    ]
  }
}
```

Note: To use the `docker-to-squash.py`, you need to install [skopeo](https://github.com/containers/skopeo), [jq](https://stedolan.github.io/jq/) and squashfs-tools.


## Configurations

Then you need to set up storm with the following configs:

| Setting                                   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `storm.resource.isolation.plugin.enable`  | set to `true` to enable isolation plugin. `storm.resource.isolation.plugin` determines which plugin to use. If this is set to `false`, `org.apache.storm.container.DefaultResourceIsolationManager` will be used.                                                                                                                                                                                                                                           |
| `storm.resource.isolation.plugin`         | set to `"org.apache.storm.container.oci.RuncLibContainerManager"` to enable OCI/Squash runtime support                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `storm.oci.allowed.images`             | An allowlist of docker images that can be used. Users can only choose a docker image from the list.
| `storm.oci.image`                      | The default docker image to be used if user doesn't specify which image to use. And it must belong to the `storm.oci.allowed.images` 
| `topology.oci.image`                   | Topologies can specify which image to use. It must belong to the `storm.oci.allowed.images` |
| `storm.oci.cgroup.root`                | The root path of cgroup for docker to use. On RHEL7, it should be "/sys/fs/cgroup".
| `storm.oci.cgroup.parent`              | --cgroup-parent config for docker command. It must follow the constraints of docker commands. The path will be made as absolute path if it's a relative path because we saw some weird bugs ((the cgroup memory directory disappears after a while) when a relative path is used.
| `storm.oci.readonly.bindmounts`        | A list of read only bind mounted directories.
| `storm.oci.readwrite.bindmounts`       | A list of read-write bind mounted directories.
| `storm.oci.nscd.dir`                   | The directory of nscd (name service cache daemon), e.g. "/var/run/nscd/". nscd must be running so that profiling can work properly.
| `storm.oci.seccomp.profile`            | Specify the seccomp Json file to be used as a seccomp filter
| `supervisor.worker.launcher`              | Full path to the worker-launcher executable.
| `storm.oci.image.hdfs.toplevel.dir`      |  The HDFS location under which the oci image manifests, layers and configs directories exist.
| `storm.oci.image.tag.to.manifest.plugin` |  The plugin to be used to get the image-tag to manifest mappings.
| `storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.hdfs.hash.file` | The hdfs location of the image-tag to manifest mapping file. If `org.apache.storm.container.oci.LocalOrHdfsImageTagToManifestPlugin` is used as `storm.oci.image.tag.to.manifest.plugin`, either `storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.hdfs.hash.file` or `storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.local.hash.file` needs to be configured.
| `storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.local.hash.file` | The local file system location where the image-tag to manifest mapping file exists. If `org.apache.storm.container.oci.LocalOrHdfsImageTagToManifestPlugin` is used as `storm.oci.image.tag.to.manifest.plugin`, either `storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.hdfs.hash.file` or `storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.local.hash.file` needs to be configured.
| `storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.cache.refresh.interval.secs` | The interval in seconds between refreshing the image-tag to manifest mapping cache, used by `org.apache.storm.container.oci.LocalOrHdfsImageTagToManifestPlugin`.|
| `storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.num.manifests.to.cache` | The number of manifests to cache, used by `org.apache.storm.container.oci.LocalOrHdfsImageTagToManifestPlugin`.|
| `storm.oci.manifest.to.resources.plugin` | The plugin to be used to get oci resource according to the manifest.
| `storm.oci.resources.localizer`   | The plugin to use for oci resources localization. |
| `storm.oci.resources.local.dir` | The local directory for localized oci resources. |

For example, 
```bash
storm.resource.isolation.plugin: "org.apache.storm.container.oci.RuncLibContainerManager"

storm.oci.allowed.images:
    - "storm/rhel7:dev_current"
    - "storm/rhel7:dev_previous"
    - "storm/rhel7:dev_test"
storm.oci.image: "storm/rhel7:dev_current"

storm.oci.cgroup.parent: "/storm"
storm.oci.cgroup.root: "/sys/fs/cgroup"
storm.oci.image.hdfs.toplevel.dir: "hdfs://host:port/containers/"
storm.oci.image.tag.to.manifest.plugin: "org.apache.storm.container.oci.LocalOrHdfsImageTagToManifestPlugin"
storm.oci.local.or.hdfs.image.tag.to.manifest.plugin.hdfs.hash.file: "hdfs://host:port/containers/image-tag-to-hash"
storm.oci.manifest.to.resources.plugin: "org.apache.storm.container.oci.HdfsManifestToResourcesPlugin"
storm.oci.readonly.bindmounts:
    - "/home/y/lib64/storm"
    - "/etc/krb5.conf"

storm.oci.resources.localizer: "org.apache.storm.container.oci.HdfsOciResourcesLocalizer"
storm.oci.seccomp.profile: "/home/y/conf/storm/seccomp.json"
```

To use built-in plugins from `external/storm-hdfs-oci`, you need to build `external/storm-hdfs-oci` and copy `storm-hdfs-oci.jar` and its dependencies to the `extlib-daemon` directory.

Additionally, if you want to access to secure hdfs, you also need to set the following configs.  
```
storm.hdfs.login.keytab
storm.hdfs.login.principal
```

For example,
```
storm.hdfs.login.keytab: /etc/keytab
storm.hdfs.login.principal: primary/instance@REALM
```

## Implementation

##### Launch a container

The supervisor calls RuncLibContainerManager to launch the container and the worker inside the container. It will first call the `storm.oci.image.tag.to.manifest.plugin`
to fetch the mapping of image tag to manifest. Then it calls `storm.oci.manifest.to.resources.plugin` to get the list of resources to be downloaded and invokes 
`storm.oci.resources.localizer` to download the config of the image and the layers of the image to a local directory. It then composes a `oci-config.json` (see example in Appendix) and 
invokes worker-launcher to launch the container.

The worker-launcher parses the `oci-config.json` file and do some necessary initialization and set up. It then creates /run/worker-launcher/layers/xxx/mnt directories 
and associate them with loopback devices, for example:

```bash
-bash-4.2$ cat /proc/mounts
...
/dev/loop3 /run/worker-launcher/layers/f7452c2657900c53da1a4f7e430485a267b89c7717466ee61ffefba85f690226/mnt squashfs ro,relatime 0 0
/dev/loop4 /run/worker-launcher/layers/8156da43228752c7364b71dabba6aef6bd1cc081e9ea59cf92ea0f79fd8a50b6/mnt squashfs ro,relatime 0 0
/dev/loop5 /run/worker-launcher/layers/c7c9b1d6df043edf307c49d75c7d2bc3df72f8dcaf7d17b733c97022387902e6/mnt squashfs ro,relatime 0 0
/dev/loop6 /run/worker-launcher/layers/f0d08d5707855b02def8ac622a6c60203b380e31c6c237e5b691f5856594a3e7/mnt squashfs ro,relatime 0 0
/dev/loop11 /run/worker-launcher/layers/34b0bc9c446a9be565fb50b04db1e9d1c1c4d14a22a885a7aba6981748b6635e/mnt squashfs ro,relatime 0 0
/dev/loop12 /run/worker-launcher/layers/0ba001c025aa172a7d630914c75c1772228606f622e2c9d46a8fedf10774623e/mnt squashfs ro,relatime 0 0
/dev/loop13 /run/worker-launcher/layers/a5e4e615565081e04eaf4c5ab5b20d37de271db704fc781c7b1e07c5dcdf96e5/mnt squashfs ro,relatime 0 0
...

```

Then it mounts the layers, for example:
```bash
-bash-4.2$ mount
...
/home/y/var/storm/supervisor/oci-resources/layers/3692c3483ef6516fba685b316448e8aaf0fc10bb66818116edc8e5e6800076c7.sqsh on /run/worker-launcher/layers/f7452c2657900c53da1a4f7e430485a267b89c7717466ee61ffefba85f690226/mnt type squashfs (ro,relatime)
/home/y/var/storm/supervisor/oci-resources/layers/ea067172a7138f035d89a5c378db6d66c1581d98b0497b21f256e04c3d2b5303.sqsh on /run/worker-launcher/layers/8156da43228752c7364b71dabba6aef6bd1cc081e9ea59cf92ea0f79fd8a50b6/mnt type squashfs (ro,relatime)
/home/y/var/storm/supervisor/oci-resources/layers/1b73e9433ecca0a6bb152bd7525f2b7c233484d51c24f8a6ba483d5cfd3035dc.sqsh on /run/worker-launcher/layers/c7c9b1d6df043edf307c49d75c7d2bc3df72f8dcaf7d17b733c97022387902e6/mnt type squashfs (ro,relatime)
/home/y/var/storm/supervisor/oci-resources/layers/18ee671016a1bf3ecab07395d93c2cbecd352d59c497a1551e2074d64e1098d9.sqsh on /run/worker-launcher/layers/f0d08d5707855b02def8ac622a6c60203b380e31c6c237e5b691f5856594a3e7/mnt type squashfs (ro,relatime)
/home/y/var/storm/supervisor/oci-resources/layers/152ee1d2cccea9dfe6393d2bdf9d077b67616b2b417b25eb74fc5ffaadcb96f5.sqsh on /run/worker-launcher/layers/34b0bc9c446a9be565fb50b04db1e9d1c1c4d14a22a885a7aba6981748b6635e/mnt type squashfs (ro,relatime)
/home/y/var/storm/supervisor/oci-resources/layers/344224962010c03c9ca1f11a9bff0dfcc296ac46d0a55e4ff30a0ad13b9817af.sqsh on /run/worker-launcher/layers/0ba001c025aa172a7d630914c75c1772228606f622e2c9d46a8fedf10774623e/mnt type squashfs (ro,relatime)
/home/y/var/storm/supervisor/oci-resources/layers/8710a3d72f75b45c48ab6b9b67eb6d77caea3dac91a0c30e0831f591cba4887e.sqsh on /run/worker-launcher/layers/a5e4e615565081e04eaf4c5ab5b20d37de271db704fc781c7b1e07c5dcdf96e5/mnt type squashfs (ro,relatime)
...
```

It creates the rootfs and mount the overlay filesystem (with lowerdir,upperdir,workdir) for the worker with the command 
```bash
mount -t overlay overlay -o lowerdir=/lower1:/lower2:/lower3,upperdir=/upper,workdir=/work /merged
```

```bash
-bash-4.2$ mount
...
overlay on /run/worker-launcher/6703-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/rootfs type overlay (rw,relatime,lowerdir=/run/worker-launcher/layers/a5e4e615565081e04eaf4c5ab5b20d37de271db704fc781c7b1e07c5dcdf96e5/mnt:/run/worker-launcher/layers/0ba001c025aa172a7d630914c75c1772228606f622e2c9d46a8fedf10774623e/mnt:/run/worker-launcher/layers/34b0bc9c446a9be565fb50b04db1e9d1c1c4d14a22a885a7aba6981748b6635e/mnt:/run/worker-launcher/layers/f0d08d5707855b02def8ac622a6c60203b380e31c6c237e5b691f5856594a3e7/mnt:/run/worker-launcher/layers/c7c9b1d6df043edf307c49d75c7d2bc3df72f8dcaf7d17b733c97022387902e6/mnt:/run/worker-launcher/layers/8156da43228752c7364b71dabba6aef6bd1cc081e9ea59cf92ea0f79fd8a50b6/mnt:/run/worker-launcher/layers/f7452c2657900c53da1a4f7e430485a267b89c7717466ee61ffefba85f690226/mnt,upperdir=/run/worker-launcher/6703-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/upper,workdir=/run/worker-launcher/6703-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/work)
...
```

It then produce a `config.json` (see example at Appendix) under `/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb` directory and launch the container with
the command
```bash
/usr/bin/runc run -d \
              --pid-file /home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/artifacts/container-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb.pid \
              -b /home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb \
              6703-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb
```

##### Kill a container

To kill a container, `RuncLibContainerManager` sends the `SIGTERM` or `SIGKILL` signal to the container process. It then invokes worker-launcher to to umount the mounts and clean up the directories. 
The worker-launcher will invoke `runc delete container-id` to delete the container at the end.


## Profile the processes inside the container
If you have sudo permission, you can also run `sudo nsenter --target <container-pid> --pid --mount --setuid <uid> --setgid <gid>` to enter the container. 
Then you can run `jstack`, `jmap` etc inside the container. `<container-pid>` is the pid of the container process on the host.
`<container-pid>` can be obtained by running `runc list` command.

## Seccomp security profiles

You can set `storm.oci.seccomp.profile` to restrict the actions available within the container. If it's not set, the container runs without
restrictions. You can use `conf/seccomp.json.example` provided or you can specify our own `seccomp.json` file. 


## Appendix

##### Example oci-config.json file
```json
{
  "version": "0.1",
  "username": "username1",
  "containerId": "6703-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
  "pidFile": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/artifacts/container-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb.pid",
  "containerScriptPath": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/storm-worker-script.sh",
  "layers": [
    {
      "mediaType": "application/vnd.squashfs",
      "path": "/home/y/var/storm/supervisor/oci-resources/layers/3692c3483ef6516fba685b316448e8aaf0fc10bb66818116edc8e5e6800076c7.sqsh"
    },
    {
      "mediaType": "application/vnd.squashfs",
      "path": "/home/y/var/storm/supervisor/oci-resources/layers/ea067172a7138f035d89a5c378db6d66c1581d98b0497b21f256e04c3d2b5303.sqsh"
    },
    {
      "mediaType": "application/vnd.squashfs",
      "path": "/home/y/var/storm/supervisor/oci-resources/layers/1b73e9433ecca0a6bb152bd7525f2b7c233484d51c24f8a6ba483d5cfd3035dc.sqsh"
    },
    {
      "mediaType": "application/vnd.squashfs",
      "path": "/home/y/var/storm/supervisor/oci-resources/layers/18ee671016a1bf3ecab07395d93c2cbecd352d59c497a1551e2074d64e1098d9.sqsh"
    },
    {
      "mediaType": "application/vnd.squashfs",
      "path": "/home/y/var/storm/supervisor/oci-resources/layers/152ee1d2cccea9dfe6393d2bdf9d077b67616b2b417b25eb74fc5ffaadcb96f5.sqsh"
    },
    {
      "mediaType": "application/vnd.squashfs",
      "path": "/home/y/var/storm/supervisor/oci-resources/layers/344224962010c03c9ca1f11a9bff0dfcc296ac46d0a55e4ff30a0ad13b9817af.sqsh"
    },
    {
      "mediaType": "application/vnd.squashfs",
      "path": "/home/y/var/storm/supervisor/oci-resources/layers/8710a3d72f75b45c48ab6b9b67eb6d77caea3dac91a0c30e0831f591cba4887e.sqsh"
    }
  ],
  "reapLayerKeepCount": 100,
  "ociRuntimeConfig": {
    "mounts": [
      {
        "destination": "/home/y/lib64/storm",
        "type": "bind",
        "source": "/home/y/lib64/storm",
        "options": [
          "ro",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/etc/krb5.conf",
        "type": "bind",
        "source": "/etc/krb5.conf",
        "options": [
          "ro",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/etc/resolv.conf",
        "type": "bind",
        "source": "/etc/resolv.conf",
        "options": [
          "ro",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/etc/hostname",
        "type": "bind",
        "source": "/etc/hostname",
        "options": [
          "ro",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/etc/hosts",
        "type": "bind",
        "source": "/etc/hosts",
        "options": [
          "ro",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/var/run/nscd",
        "type": "bind",
        "source": "/var/run/nscd",
        "options": [
          "ro",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/sys/fs/cgroup",
        "type": "bind",
        "source": "/sys/fs/cgroup",
        "options": [
          "ro",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/home/y/var/storm/supervisor",
        "type": "bind",
        "source": "/home/y/var/storm/supervisor",
        "options": [
          "ro",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
        "type": "bind",
        "source": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
        "options": [
          "rw",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/home/y/var/storm/workers-artifacts/wc1-2-1608581491/6703",
        "type": "bind",
        "source": "/home/y/var/storm/workers-artifacts/wc1-2-1608581491/6703",
        "options": [
          "rw",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/home/y/var/storm/workers-users/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
        "type": "bind",
        "source": "/home/y/var/storm/workers-users/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
        "options": [
          "rw",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/home/y/var/storm/supervisor/stormdist/wc1-2-1608581491/shared_by_topology",
        "type": "bind",
        "source": "/home/y/var/storm/supervisor/stormdist/wc1-2-1608581491/shared_by_topology",
        "options": [
          "rw",
          "rbind",
          "rprivate"
        ]
      },
      {
        "destination": "/tmp",
        "type": "bind",
        "source": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/tmp",
        "options": [
          "rw",
          "rbind",
          "rprivate"
        ]
      }
    ],
    "process": {
      "cwd": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
      "env": [
        "X_SCLS=rh-git218",
        "LD_LIBRARY_PATH=/opt/rh/httpd24/root/usr/lib64",
        "PATH=/opt/rh/rh-git218/root/usr/bin:/home/y/bin64:/home/y/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/y/share/yjava_jdk/java/bin",
        "PERL5LIB=/opt/rh/rh-git218/root/usr/share/perl5/vendor_perl",
        "LANG=en_US.UTF-8",
        "LANGUAGE=en_US:en",
        "LC_ALL=en_US.UTF-8",
        "JAVA_HOME=/home/y/share/yjava_jdk/java",
        "LD_LIBRARY_PATH=/home/y/var/storm/supervisor/stormdist/wc1-2-1608581491/resources/Linux-amd64:/home/y/var/storm/supervisor/stormdist/wc1-2-1608581491/resources:/home/y/lib64:/usr/local/lib64:/usr/lib64:/lib64:"
      ],
      "args": [
        "bash",
        "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/storm-worker-script.sh"
      ]
    },
    "linux": {
      "cgroupsPath": "/storm/6703-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
      "resources": {
        "cpu": {
          "quota": 140000,
          "period": 100000
        }
      },
      "seccomp": {
        "defaultAction": "SCMP_ACT_ERRNO",
        "syscalls": [
          {
            "names": [
              "accept",
              "accept4",
              "access",
              "alarm",
              "alarm",
              "bind",
              "brk",
              "capget",
              "capset",
              "chdir",
              "chmod",
              "chown",
              "chown32",
              "clock_getres",
              "clock_gettime",
              "clock_nanosleep",
              "close",
              "connect",
              "copy_file_range",
              "creat",
              "dup",
              "dup2",
              "dup3",
              "epoll_create",
              "epoll_create1",
              "epoll_ctl",
              "epoll_ctl_old",
              "epoll_pwait",
              "epoll_wait",
              "epoll_wait_old",
              "eventfd",
              "eventfd2",
              "execve",
              "execveat",
              "exit",
              "exit_group",
              "faccessat",
              "fadvise64",
              "fadvise64_64",
              "fallocate",
              "fanotify_mark",
              "fchdir",
              "fchmod",
              "fchmodat",
              "fchown",
              "fchown32",
              "fchownat",
              "fcntl",
              "fcntl64",
              "fdatasync",
              "fgetxattr",
              "flistxattr",
              "flock",
              "fork",
              "fremovexattr",
              "fsetxattr",
              "fstat",
              "fstat64",
              "fstatat64",
              "fstatfs",
              "fstatfs64",
              "fsync",
              "ftruncate",
              "ftruncate64",
              "futex",
              "futimesat",
              "getcpu",
              "getcwd",
              "getdents",
              "getdents64",
              "getegid",
              "getegid32",
              "geteuid",
              "geteuid32",
              "getgid",
              "getgid32",
              "getgroups",
              "getgroups32",
              "getitimer",
              "getpeername",
              "getpgid",
              "getpgrp",
              "getpid",
              "getppid",
              "getpriority",
              "getrandom",
              "getresgid",
              "getresgid32",
              "getresuid",
              "getresuid32",
              "getrlimit",
              "get_robust_list",
              "getrusage",
              "getsid",
              "getsockname",
              "getsockopt",
              "get_thread_area",
              "gettid",
              "gettimeofday",
              "getuid",
              "getuid32",
              "getxattr",
              "inotify_add_watch",
              "inotify_init",
              "inotify_init1",
              "inotify_rm_watch",
              "io_cancel",
              "ioctl",
              "io_destroy",
              "io_getevents",
              "ioprio_get",
              "ioprio_set",
              "io_setup",
              "io_submit",
              "ipc",
              "kill",
              "lchown",
              "lchown32",
              "lgetxattr",
              "link",
              "linkat",
              "listen",
              "listxattr",
              "llistxattr",
              "_llseek",
              "lremovexattr",
              "lseek",
              "lsetxattr",
              "lstat",
              "lstat64",
              "madvise",
              "mbind",
              "memfd_create",
              "mincore",
              "mkdir",
              "mkdirat",
              "mknod",
              "mknodat",
              "mlock",
              "mlock2",
              "mlockall",
              "mmap",
              "mmap2",
              "mprotect",
              "mq_getsetattr",
              "mq_notify",
              "mq_open",
              "mq_timedreceive",
              "mq_timedsend",
              "mq_unlink",
              "mremap",
              "msgctl",
              "msgget",
              "msgrcv",
              "msgsnd",
              "msync",
              "munlock",
              "munlockall",
              "munmap",
              "nanosleep",
              "newfstatat",
              "_newselect",
              "open",
              "openat",
              "pause",
              "pipe",
              "pipe2",
              "poll",
              "ppoll",
              "prctl",
              "pread64",
              "preadv",
              "prlimit64",
              "pselect6",
              "pwrite64",
              "pwritev",
              "read",
              "readahead",
              "readlink",
              "readlinkat",
              "readv",
              "recv",
              "recvfrom",
              "recvmmsg",
              "recvmsg",
              "remap_file_pages",
              "removexattr",
              "rename",
              "renameat",
              "renameat2",
              "restart_syscall",
              "rmdir",
              "rt_sigaction",
              "rt_sigpending",
              "rt_sigprocmask",
              "rt_sigqueueinfo",
              "rt_sigreturn",
              "rt_sigsuspend",
              "rt_sigtimedwait",
              "rt_tgsigqueueinfo",
              "sched_getaffinity",
              "sched_getattr",
              "sched_getparam",
              "sched_get_priority_max",
              "sched_get_priority_min",
              "sched_getscheduler",
              "sched_rr_get_interval",
              "sched_setaffinity",
              "sched_setattr",
              "sched_setparam",
              "sched_setscheduler",
              "sched_yield",
              "seccomp",
              "select",
              "semctl",
              "semget",
              "semop",
              "semtimedop",
              "send",
              "sendfile",
              "sendfile64",
              "sendmmsg",
              "sendmsg",
              "sendto",
              "setfsgid",
              "setfsgid32",
              "setfsuid",
              "setfsuid32",
              "setgid",
              "setgid32",
              "setgroups",
              "setgroups32",
              "setitimer",
              "setpgid",
              "setpriority",
              "setregid",
              "setregid32",
              "setresgid",
              "setresgid32",
              "setresuid",
              "setresuid32",
              "setreuid",
              "setreuid32",
              "setrlimit",
              "set_robust_list",
              "setsid",
              "setsockopt",
              "set_thread_area",
              "set_tid_address",
              "setuid",
              "setuid32",
              "setxattr",
              "shmat",
              "shmctl",
              "shmdt",
              "shmget",
              "shutdown",
              "sigaltstack",
              "signalfd",
              "signalfd4",
              "sigreturn",
              "socket",
              "socketcall",
              "socketpair",
              "splice",
              "stat",
              "stat64",
              "statfs",
              "statfs64",
              "symlink",
              "symlinkat",
              "sync",
              "sync_file_range",
              "syncfs",
              "sysinfo",
              "syslog",
              "tee",
              "tgkill",
              "time",
              "timer_create",
              "timer_delete",
              "timerfd_create",
              "timerfd_gettime",
              "timerfd_settime",
              "timer_getoverrun",
              "timer_gettime",
              "timer_settime",
              "times",
              "tkill",
              "truncate",
              "truncate64",
              "ugetrlimit",
              "umask",
              "uname",
              "unlink",
              "unlinkat",
              "utime",
              "utimensat",
              "utimes",
              "vfork",
              "vmsplice",
              "wait4",
              "waitid",
              "waitpid",
              "write",
              "writev",
              "mount",
              "umount2",
              "reboot",
              "name_to_handle_at",
              "unshare"
            ],
            "action": "SCMP_ACT_ALLOW"
          },
          {
            "names": [
              "personality"
            ],
            "action": "SCMP_ACT_ALLOW",
            "args": [
              {
                "index": 0,
                "value": 0,
                "valueTwo": 0,
                "op": "SCMP_CMP_EQ"
              }
            ]
          },
          {
            "names": [
              "personality"
            ],
            "action": "SCMP_ACT_ALLOW",
            "args": [
              {
                "index": 0,
                "value": 8,
                "valueTwo": 0,
                "op": "SCMP_CMP_EQ"
              }
            ]
          },
          {
            "names": [
              "personality"
            ],
            "action": "SCMP_ACT_ALLOW",
            "args": [
              {
                "index": 0,
                "value": 4294967295,
                "valueTwo": 0,
                "op": "SCMP_CMP_EQ"
              }
            ]
          },
          {
            "names": [
              "arch_prctl"
            ],
            "action": "SCMP_ACT_ALLOW"
          },
          {
            "names": [
              "modify_ldt"
            ],
            "action": "SCMP_ACT_ALLOW"
          },
          {
            "names": [
              "clone"
            ],
            "action": "SCMP_ACT_ALLOW",
            "args": [
              {
                "index": 0,
                "value": 2080505856,
                "valueTwo": 0,
                "op": "SCMP_CMP_MASKED_EQ"
              }
            ]
          }
        ]
      }
    }
  }
}
```

##### Example config.json file
```json
{
  "ociVersion": "1.0.0",
  "hostname": "hostname1",
  "root": {
    "path": "/run/worker-launcher/6703-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/rootfs",
    "readonly": true
  },
  "process": {
    "args": [
      "bash",
      "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/storm-worker-script.sh"
    ],
    "cwd": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
    "env": [
      "X_SCLS=rh-git218",
      "LD_LIBRARY_PATH=/opt/rh/httpd24/root/usr/lib64",
      "PATH=/opt/rh/rh-git218/root/usr/bin:/home/y/bin64:/home/y/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/y/share/yjava_jdk/java/bin",
      "PERL5LIB=/opt/rh/rh-git218/root/usr/share/perl5/vendor_perl",
      "LANG=en_US.UTF-8",
      "LANGUAGE=en_US:en",
      "LC_ALL=en_US.UTF-8",
      "JAVA_HOME=/home/y/share/yjava_jdk/java",
      "LD_LIBRARY_PATH=/home/y/var/storm/supervisor/stormdist/wc1-2-1608581491/resources/Linux-amd64:/home/y/var/storm/supervisor/stormdist/wc1-2-1608581491/resources:/home/y/lib64:/usr/local/lib64:/usr/lib64:/lib64:"
    ],
    "noNewPrivileges": true,
    "user": {
      "uid": 31315,
      "gid": 100,
      "additionalGids": [
        5548
      ]
    }
  },
  "mounts": [
    {
      "source": "proc",
      "destination": "/proc",
      "type": "proc"
    },
    {
      "source": "tmpfs",
      "destination": "/dev",
      "type": "tmpfs",
      "options": [
        "nosuid",
        "strictatime",
        "mode=755",
        "size=65536k"
      ]
    },
    {
      "source": "devpts",
      "destination": "/dev/pts",
      "type": "devpts",
      "options": [
        "nosuid",
        "noexec",
        "newinstance",
        "ptmxmode=0666",
        "mode=0620",
        "gid=5"
      ]
    },
    {
      "source": "shm",
      "destination": "/dev/shm",
      "type": "tmpfs",
      "options": [
        "nosuid",
        "noexec",
        "nodev",
        "mode=1777",
        "size=65536k"
      ]
    },
    {
      "source": "mqueue",
      "destination": "/dev/mqueue",
      "type": "mqueue",
      "options": [
        "nosuid",
        "noexec",
        "nodev"
      ]
    },
    {
      "source": "sysfs",
      "destination": "/sys",
      "type": "sysfs",
      "options": [
        "nosuid",
        "noexec",
        "nodev",
        "ro"
      ]
    },
    {
      "source": "cgroup",
      "destination": "/sys/fs/cgroup",
      "type": "cgroup",
      "options": [
        "nosuid",
        "noexec",
        "nodev",
        "relatime",
        "ro"
      ]
    },
    {
      "destination": "/home/y/lib64/storm",
      "type": "bind",
      "source": "/home/y/lib64/storm",
      "options": [
        "ro",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/etc/krb5.conf",
      "type": "bind",
      "source": "/etc/krb5.conf",
      "options": [
        "ro",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/etc/resolv.conf",
      "type": "bind",
      "source": "/etc/resolv.conf",
      "options": [
        "ro",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/etc/hostname",
      "type": "bind",
      "source": "/etc/hostname",
      "options": [
        "ro",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/etc/hosts",
      "type": "bind",
      "source": "/etc/hosts",
      "options": [
        "ro",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/var/run/nscd",
      "type": "bind",
      "source": "/var/run/nscd",
      "options": [
        "ro",
        "rbind",
        "rprivate"
      ]
    },
    
    {
      "destination": "/sys/fs/cgroup",
      "type": "bind",
      "source": "/sys/fs/cgroup",
      "options": [
        "ro",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/home/y/var/storm/supervisor",
      "type": "bind",
      "source": "/home/y/var/storm/supervisor",
      "options": [
        "ro",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
      "type": "bind",
      "source": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
      "options": [
        "rw",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/home/y/var/storm/workers-artifacts/wc1-2-1608581491/6703",
      "type": "bind",
      "source": "/home/y/var/storm/workers-artifacts/wc1-2-1608581491/6703",
      "options": [
        "rw",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/home/y/var/storm/workers-users/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
      "type": "bind",
      "source": "/home/y/var/storm/workers-users/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
      "options": [
        "rw",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/home/y/var/storm/supervisor/stormdist/wc1-2-1608581491/shared_by_topology",
      "type": "bind",
      "source": "/home/y/var/storm/supervisor/stormdist/wc1-2-1608581491/shared_by_topology",
      "options": [
        "rw",
        "rbind",
        "rprivate"
      ]
    },
    {
      "destination": "/tmp",
      "type": "bind",
      "source": "/home/y/var/storm/workers/1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb/tmp",
      "options": [
        "rw",
        "rbind",
        "rprivate"
      ]
    }
  ],
  "linux": {
    "cgroupsPath": "/storm/6703-1a23ca4b-6062-4d08-8ac3-b09e7d35e7cb",
    "resources": {
      "devices": [
        {
          "access": "rwm",
          "allow": false
        }
      ],
      "cpu": {
        "quota": 140000,
        "period": 100000
      }
    },
    "namespaces": [
      {
        "type": "pid"
      },
      {
        "type": "ipc"
      },
      {
        "type": "uts"
      },
      {
        "type": "mount"
      }
    ],
    "maskedPaths": [
      "/proc/kcore",
      "/proc/latency_stats",
      "/proc/timer_list",
      "/proc/timer_stats",
      "/proc/sched_debug",
      "/proc/scsi",
      "/sys/firmware"
    ],
    "readonlyPaths": [
      "/proc/asound",
      "/proc/bus",
      "/proc/fs",
      "/proc/irq",
      "/proc/sys",
      "/proc/sysrq-trigger"
    ],
    "seccomp": {
      "defaultAction": "SCMP_ACT_ERRNO",
      "syscalls": [
        {
          "names": [
            "accept",
            "accept4",
            "access",
            "alarm",
            "alarm",
            "bind",
            "brk",
            "capget",
            "capset",
            "chdir",
            "chmod",
            "chown",
            "chown32",
            "clock_getres",
            "clock_gettime",
            "clock_nanosleep",
            "close",
            "connect",
            "copy_file_range",
            "creat",
            "dup",
            "dup2",
            "dup3",
            "epoll_create",
            "epoll_create1",
            "epoll_ctl",
            "epoll_ctl_old",
            "epoll_pwait",
            "epoll_wait",
            "epoll_wait_old",
            "eventfd",
            "eventfd2",
            "execve",
            "execveat",
            "exit",
            "exit_group",
            "faccessat",
            "fadvise64",
            "fadvise64_64",
            "fallocate",
            "fanotify_mark",
            "fchdir",
            "fchmod",
            "fchmodat",
            "fchown",
            "fchown32",
            "fchownat",
            "fcntl",
            "fcntl64",
            "fdatasync",
            "fgetxattr",
            "flistxattr",
            "flock",
            "fork",
            "fremovexattr",
            "fsetxattr",
            "fstat",
            "fstat64",
            "fstatat64",
            "fstatfs",
            "fstatfs64",
            "fsync",
            "ftruncate",
            "ftruncate64",
            "futex",
            "futimesat",
            "getcpu",
            "getcwd",
            "getdents",
            "getdents64",
            "getegid",
            "getegid32",
            "geteuid",
            "geteuid32",
            "getgid",
            "getgid32",
            "getgroups",
            "getgroups32",
            "getitimer",
            "getpeername",
            "getpgid",
            "getpgrp",
            "getpid",
            "getppid",
            "getpriority",
            "getrandom",
            "getresgid",
            "getresgid32",
            "getresuid",
            "getresuid32",
            "getrlimit",
            "get_robust_list",
            "getrusage",
            "getsid",
            "getsockname",
            "getsockopt",
            "get_thread_area",
            "gettid",
            "gettimeofday",
            "getuid",
            "getuid32",
            "getxattr",
            "inotify_add_watch",
            "inotify_init",
            "inotify_init1",
            "inotify_rm_watch",
            "io_cancel",
            "ioctl",
            "io_destroy",
            "io_getevents",
            "ioprio_get",
            "ioprio_set",
            "io_setup",
            "io_submit",
            "ipc",
            "kill",
            "lchown",
            "lchown32",
            "lgetxattr",
            "link",
            "linkat",
            "listen",
            "listxattr",
            "llistxattr",
            "_llseek",
            "lremovexattr",
            "lseek",
            "lsetxattr",
            "lstat",
            "lstat64",
            "madvise",
            "mbind",
            "memfd_create",
            "mincore",
            "mkdir",
            "mkdirat",
            "mknod",
            "mknodat",
            "mlock",
            "mlock2",
            "mlockall",
            "mmap",
            "mmap2",
            "mprotect",
            "mq_getsetattr",
            "mq_notify",
            "mq_open",
            "mq_timedreceive",
            "mq_timedsend",
            "mq_unlink",
            "mremap",
            "msgctl",
            "msgget",
            "msgrcv",
            "msgsnd",
            "msync",
            "munlock",
            "munlockall",
            "munmap",
            "nanosleep",
            "newfstatat",
            "_newselect",
            "open",
            "openat",
            "pause",
            "pipe",
            "pipe2",
            "poll",
            "ppoll",
            "prctl",
            "pread64",
            "preadv",
            "prlimit64",
            "pselect6",
            "pwrite64",
            "pwritev",
            "read",
            "readahead",
            "readlink",
            "readlinkat",
            "readv",
            "recv",
            "recvfrom",
            "recvmmsg",
            "recvmsg",
            "remap_file_pages",
            "removexattr",
            "rename",
            "renameat",
            "renameat2",
            "restart_syscall",
            "rmdir",
            "rt_sigaction",
            "rt_sigpending",
            "rt_sigprocmask",
            "rt_sigqueueinfo",
            "rt_sigreturn",
            "rt_sigsuspend",
            "rt_sigtimedwait",
            "rt_tgsigqueueinfo",
            "sched_getaffinity",
            "sched_getattr",
            "sched_getparam",
            "sched_get_priority_max",
            "sched_get_priority_min",
            "sched_getscheduler",
            "sched_rr_get_interval",
            "sched_setaffinity",
            "sched_setattr",
            "sched_setparam",
            "sched_setscheduler",
            "sched_yield",
            "seccomp",
            "select",
            "semctl",
            "semget",
            "semop",
            "semtimedop",
            "send",
            "sendfile",
            "sendfile64",
            "sendmmsg",
            "sendmsg",
            "sendto",
            "setfsgid",
            "setfsgid32",
            "setfsuid",
            "setfsuid32",
            "setgid",
            "setgid32",
            "setgroups",
            "setgroups32",
            "setitimer",
            "setpgid",
            "setpriority",
            "setregid",
            "setregid32",
            "setresgid",
            "setresgid32",
            "setresuid",
            "setresuid32",
            "setreuid",
            "setreuid32",
            "setrlimit",
            "set_robust_list",
            "setsid",
            "setsockopt",
            "set_thread_area",
            "set_tid_address",
            "setuid",
            "setuid32",
            "setxattr",
            "shmat",
            "shmctl",
            "shmdt",
            "shmget",
            "shutdown",
            "sigaltstack",
            "signalfd",
            "signalfd4",
            "sigreturn",
            "socket",
            "socketcall",
            "socketpair",
            "splice",
            "stat",
            "stat64",
            "statfs",
            "statfs64",
            "symlink",
            "symlinkat",
            "sync",
            "sync_file_range",
            "syncfs",
            "sysinfo",
            "syslog",
            "tee",
            "tgkill",
            "time",
            "timer_create",
            "timer_delete",
            "timerfd_create",
            "timerfd_gettime",
            "timerfd_settime",
            "timer_getoverrun",
            "timer_gettime",
            "timer_settime",
            "times",
            "tkill",
            "truncate",
            "truncate64",
            "ugetrlimit",
            "umask",
            "uname",
            "unlink",
            "unlinkat",
            "utime",
            "utimensat",
            "utimes",
            "vfork",
            "vmsplice",
            "wait4",
            "waitid",
            "waitpid",
            "write",
            "writev",
            "mount",
            "umount2",
            "reboot",
            "name_to_handle_at",
            "unshare"
          ],
          "action": "SCMP_ACT_ALLOW"
        },
        {
          "names": [
            "personality"
          ],
          "action": "SCMP_ACT_ALLOW",
          "args": [
            {
              "index": 0,
              "value": 0,
              "valueTwo": 0,
              "op": "SCMP_CMP_EQ"
            }
          ]
        },
        {
          "names": [
            "personality"
          ],
          "action": "SCMP_ACT_ALLOW",
          "args": [
            {
              "index": 0,
              "value": 8,
              "valueTwo": 0,
              "op": "SCMP_CMP_EQ"
            }
          ]
        },
        {
          "names": [
            "personality"
          ],
          "action": "SCMP_ACT_ALLOW",
          "args": [
            {
              "index": 0,
              "value": 4294967295,
              "valueTwo": 0,
              "op": "SCMP_CMP_EQ"
            }
          ]
        },
        {
          "names": [
            "arch_prctl"
          ],
          "action": "SCMP_ACT_ALLOW"
        },
        {
          "names": [
            "modify_ldt"
          ],
          "action": "SCMP_ACT_ALLOW"
        },
        {
          "names": [
            "clone"
          ],
          "action": "SCMP_ACT_ALLOW",
          "args": [
            {
              "index": 0,
              "value": 2080505856,
              "valueTwo": 0,
              "op": "SCMP_CMP_MASKED_EQ"
            }
          ]
        }
      ]
    }
  }
}
```

## CGroup Metrics

Runc internally uses cgroups to control resources for containers. The CGroup Metrics described at [cgroups_in_storm.md](cgroups_in_storm.md#CGroup-Metrics) still apply except CGroupCpuGuarantee. To get CGroup cpu guarantee, use CGroupCpuGuaranteeByCfsQuota instead.