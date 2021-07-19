/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <sys/utsname.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "utils/user_info.h"
#include "utils/file-utils.h"

#include "worker-launcher.h"
#include "utils/cJSON.h"
#include "utils/file-utils.h"

#include "oci_launch_cmd.h"
#include "oci_write_config.h"

#define RUNC_CONFIG_FILENAME    "config.json"
#define STARTING_JSON_BUFFER_SIZE  (128*1024)


static cJSON* build_runc_config_root(const char* rootfs_path) {
  cJSON* root = cJSON_CreateObject();
  if (cJSON_AddStringToObject(root, "path", rootfs_path) == NULL) {
    goto fail;
  }
  if (cJSON_AddTrueToObject(root, "readonly") == NULL) {
    goto fail;
  }
  return root;

fail:
  cJSON_Delete(root);
  return NULL;
}

static cJSON* build_runc_config_process_user(const char* username) {
  cJSON* user_json = cJSON_CreateObject();
  struct user_info* sui = user_info_alloc();
  if (sui == NULL) {
    return NULL;
  }

  int rc = user_info_fetch(sui, username);
  if (rc != 0) {
    fprintf(ERRORFILE, "Error looking up user %s : %s\n", username,
        strerror(rc));
    goto fail;
  }

  if (cJSON_AddNumberToObject(user_json, "uid", sui->pwd.pw_uid) == NULL) {
    goto fail;
  }
  if (cJSON_AddNumberToObject(user_json, "gid", sui->pwd.pw_gid) == NULL) {
    goto fail;
  }

  rc = user_info_getgroups(sui);
  if (rc != 0) {
    fprintf(ERRORFILE, "Error getting groups for user %s : %s\n", username,
        strerror(rc));
    goto fail;
  }

  if (sui->num_gids > 1) {
    cJSON* garray = cJSON_AddArrayToObject(user_json, "additionalGids");
    if (garray == NULL) {
      goto fail;
    }

    // first gid entry is the primary group which is accounted for above
    int i;
    for (i = 1; i < sui->num_gids; ++i) {
      cJSON* g = cJSON_CreateNumber(sui->gids[i]);
      if (g == NULL) {
        goto fail;
      }
      cJSON_AddItemToArray(garray, g);
    }
  }

  return user_json;

fail:
  user_info_free(sui);
  cJSON_Delete(user_json);
  return NULL;
}

static cJSON* build_runc_config_process(const oci_launch_cmd* olc) {
  cJSON* process = cJSON_CreateObject();
  if (process == NULL) {
    return NULL;
  }

  cJSON_AddItemReferenceToObject(process, "args", olc->config.process.args);
  cJSON_AddItemReferenceToObject(process, "cwd", olc->config.process.cwd);
  cJSON_AddItemReferenceToObject(process, "env", olc->config.process.env);
  if (cJSON_AddTrueToObject(process, "noNewPrivileges") == NULL) {
    goto fail;
  }

  cJSON* user_json = build_runc_config_process_user(olc->username);
  if (user_json == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(process, "user", user_json);

  return process;

fail:
  cJSON_Delete(process);
  return NULL;
}

static bool add_mount_opts(cJSON* mount_json, va_list opts) {
  const char* opt = va_arg(opts, const char*);
  if (opt == NULL) {
    return true;
  }

  cJSON* opts_array = cJSON_AddArrayToObject(mount_json, "options");
  if (opts_array == NULL) {
    return false;
  }

  do {
    cJSON* opt_json = cJSON_CreateString(opt);
    if (opt_json == NULL) {
      return false;
    }
    cJSON_AddItemToArray(opts_array, opt_json);
    opt = va_arg(opts, const char*);
  } while (opt != NULL);

  return true;
}

static bool add_mount_json(cJSON* mounts_array, const char* src,
    const char* dest, const char* fstype, ...) {
  bool result = false;
  cJSON* m = cJSON_CreateObject();
  if (cJSON_AddStringToObject(m, "source", src) == NULL) {
    goto cleanup;
  }
  if (cJSON_AddStringToObject(m, "destination", dest) == NULL) {
    goto cleanup;
  }
  if (cJSON_AddStringToObject(m, "type", fstype) == NULL) {
    goto cleanup;
  }

  va_list vargs;
  va_start(vargs, fstype);
  result = add_mount_opts(m, vargs);
  va_end(vargs);

  if (result) {
    cJSON_AddItemToArray(mounts_array, m);
  }

cleanup:
  if (!result) {
    cJSON_Delete(m);
  }
  return result;
}

static bool add_std_mounts_json(cJSON* mounts_array) {
  bool result = true;
  result &= add_mount_json(mounts_array, "proc", "/proc", "proc", NULL);
  result &= add_mount_json(mounts_array, "tmpfs", "/dev", "tmpfs",
      "nosuid", "strictatime", "mode=755", "size=65536k", NULL);
  result &= add_mount_json(mounts_array, "devpts", "/dev/pts", "devpts",
      "nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5",
       NULL);
  result &= add_mount_json(mounts_array, "shm", "/dev/shm", "tmpfs",
      "nosuid", "noexec", "nodev", "mode=1777", "size=65536k", NULL);
  result &= add_mount_json(mounts_array, "mqueue", "/dev/mqueue", "mqueue",
      "nosuid", "noexec", "nodev", NULL);
  result &= add_mount_json(mounts_array, "sysfs", "/sys", "sysfs",
      "nosuid", "noexec", "nodev", "ro", NULL);
  result &= add_mount_json(mounts_array, "cgroup", "/sys/fs/cgroup", "cgroup",
      "nosuid", "noexec", "nodev", "relatime", "ro", NULL);
  return result;
}

static cJSON* build_runc_config_mounts(const oci_launch_cmd* olc) {
  cJSON* mjson = cJSON_CreateArray();
  if (!add_std_mounts_json(mjson)) {
    goto fail;
  }

  cJSON* e;
  cJSON_ArrayForEach(e, olc->config.mounts) {
    cJSON_AddItemReferenceToArray(mjson, e);
  }

  return mjson;

fail:
  cJSON_Delete(mjson);
  return NULL;
}

static cJSON* get_default_linux_devices_json() {
  cJSON* devs = cJSON_CreateArray();
  if (devs == NULL) {
    return NULL;
  }

  cJSON* o = cJSON_CreateObject();
  if (o == NULL) {
    goto fail;
  }
  cJSON_AddItemToArray(devs, o);

  if (cJSON_AddStringToObject(o, "access", "rwm") == NULL) {
    goto fail;
  }

  if (cJSON_AddFalseToObject(o, "allow") == NULL) {
    goto fail;
  }

  return devs;

fail:
  cJSON_Delete(devs);
  return NULL;
}

static bool add_linux_resources_json(cJSON* ljson, const oci_launch_cmd* olc) {
  cJSON* robj = cJSON_AddObjectToObject(ljson, "resources");
  if (robj == NULL) {
    return false;
  }

  cJSON* devs = get_default_linux_devices_json();
  if (devs == NULL) {
    return false;
  }
  cJSON_AddItemToObjectCS(robj, "devices", devs);

  const cJSON* olc_rsrc = cJSON_GetObjectItemCaseSensitive(
      olc->config.linux_config, "resources");
  cJSON* e;
  cJSON_ArrayForEach(e, olc_rsrc) {
    if (strcmp("devices", e->string) == 0) {
      cJSON* dev_e;
      cJSON_ArrayForEach(dev_e, e) {
        cJSON_AddItemReferenceToArray(devs, dev_e);
      }
    } else {
      cJSON_AddItemReferenceToObject(robj, e->string, e);
    }
  }

  return true;
}

static bool add_linux_namespace_json(cJSON* ljson, const char* ns_type) {
  cJSON* ns = cJSON_CreateObject();
  if (ns == NULL) {
    return false;
  }
  cJSON_AddItemToArray(ljson, ns);
  return (cJSON_AddStringToObject(ns, "type", ns_type) != NULL);
}

static bool add_linux_namespaces_json(cJSON* ljson) {
  cJSON* ns_array = cJSON_AddArrayToObject(ljson, "namespaces");
  if (ns_array == NULL) {
    return false;
  }
  bool result = add_linux_namespace_json(ns_array, "pid");
  result &= add_linux_namespace_json(ns_array, "ipc");
  result &= add_linux_namespace_json(ns_array, "uts");
  result &= add_linux_namespace_json(ns_array, "mount");
  return result;
}

static const char* oci_masked_paths[] = {
  "/proc/kcore",
  "/proc/latency_stats",
  "/proc/timer_list",
  "/proc/timer_stats",
  "/proc/sched_debug",
  "/proc/scsi",
  "/sys/firmware"
};

static bool add_linux_masked_paths_json(cJSON* ljson) {
  size_t num_paths = sizeof(oci_masked_paths) / sizeof(oci_masked_paths[0]);
  cJSON* paths = cJSON_CreateStringArray(oci_masked_paths, num_paths);
  if (paths == NULL) {
    return false;
  }
  cJSON_AddItemToObject(ljson, "maskedPaths", paths);
  return true;
}

static const char* oci_readonly_paths[] = {
  "/proc/asound",
  "/proc/bus",
  "/proc/fs",
  "/proc/irq",
  "/proc/sys",
  "/proc/sysrq-trigger"
};

static bool add_linux_readonly_paths_json(cJSON* ljson) {
  size_t num_paths = sizeof(oci_readonly_paths) / sizeof(oci_readonly_paths[0]);
  cJSON* paths = cJSON_CreateStringArray(oci_readonly_paths, num_paths);
  if (paths == NULL) {
    return false;
  }
  cJSON_AddItemToObject(ljson, "readonlyPaths", paths);
  return true;
}

static bool add_linux_seccomp_json(cJSON* ljson, const oci_launch_cmd* olc) {
  cJSON* sj = cJSON_GetObjectItemCaseSensitive(olc->config.linux_config,
      "seccomp");
  if (sj != NULL) {
    cJSON_AddItemReferenceToObject(ljson, "seccomp", sj);
  }
  return true;
}

static cJSON* build_runc_config_linux(const oci_launch_cmd* olc) {
  cJSON* ljson = cJSON_CreateObject();
  if (ljson == NULL) {
    return NULL;
  }

  cJSON* cgroupsPath = cJSON_GetObjectItemCaseSensitive(
      olc->config.linux_config, "cgroupsPath");
  if (cgroupsPath == NULL) {
    goto fail;
  }
  cJSON_AddItemReferenceToObject(ljson, "cgroupsPath", cgroupsPath);

  if (!add_linux_resources_json(ljson, olc)) {
    goto fail;
  }

  if (!add_linux_namespaces_json(ljson)) {
    goto fail;
  }

  if (!add_linux_masked_paths_json(ljson)) {
    goto fail;
  }

  if (!add_linux_readonly_paths_json(ljson)) {
    goto fail;
  }

  if (!add_linux_seccomp_json(ljson, olc)) {
    goto fail;
  }

  return ljson;

fail:
  cJSON_Delete(ljson);
  return NULL;
}

static char* build_runc_config(const oci_launch_cmd* olc,
    const char* rootfs_path) {
  char* json_data = NULL;
  cJSON* rcj = cJSON_CreateObject();
  if (rcj == NULL) {
    goto fail;
  }

  if (cJSON_AddStringToObject(rcj, "ociVersion", "1.0.0") == NULL) {
    goto fail;
  }

  struct utsname uts;
  uname(&uts);
  if (cJSON_AddStringToObject(rcj, "hostname", uts.nodename) == NULL) {
    goto fail;
  }

  cJSON* item = build_runc_config_root(rootfs_path);
  if (item == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(rcj, "root", item);

  item = build_runc_config_process(olc);
  if (item == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(rcj, "process", item);

  item = build_runc_config_mounts(olc);
  if (item == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(rcj, "mounts", item);

  item = build_runc_config_linux(olc);
  if (item == NULL) {
    goto fail;
  }
  cJSON_AddItemToObjectCS(rcj, "linux", item);

  json_data = cJSON_PrintBuffered(rcj, STARTING_JSON_BUFFER_SIZE, false);

fail:
  cJSON_Delete(rcj);
  return json_data;
}

/**
 * Creates the OCI runtime configuration file for a container.
 *
 * Returns the path to the written configuration file or NULL on error.
 */
char* write_oci_runc_config(const oci_launch_cmd* olc, const char* rootfs_path, const char* working_dir) {
  char* config_data = build_runc_config(olc, rootfs_path);
  if (config_data == NULL) {
    return NULL;
  }

  char* runc_config_path = get_full_path(working_dir, RUNC_CONFIG_FILENAME);
  if (runc_config_path == NULL) {
    fputs("ERROR: Unable to generate runc config path\n", ERRORFILE);
    free(config_data);
    return NULL;
  }

  bool write_ok = write_file_as_wl(runc_config_path, config_data,
      strlen(config_data));
  free(config_data);
  if (!write_ok) {
    free(runc_config_path);
    return NULL;
  }

  return runc_config_path;
}
