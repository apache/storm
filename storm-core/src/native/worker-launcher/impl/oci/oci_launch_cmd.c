/**
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
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "utils/cJSON.h"
#include "utils/file-utils.h"
#include "utils/string-utils.h"

#include "worker-launcher.h"
#include "oci_launch_cmd.h"

#define SQUASHFS_MEDIA_TYPE     "application/vnd.squashfs"

static void free_olc_layers(olc_layer_spec* layers, unsigned int num_layers) {
  unsigned int i;
  for (i = 0; i < num_layers; ++i) {
    free(layers[i].media_type);
    free(layers[i].path);
  }
  free(layers);
}

/**
 * Free an OCI launch command structure and all memory associated with it.
 */
void free_oci_launch_cmd(oci_launch_cmd* olc) {
  if (olc != NULL) {
    free(olc->username);
    free(olc->container_id);
    free(olc->pid_file);
    free(olc->script_path);
    free_olc_layers(olc->layers, olc->num_layers);
    cJSON_Delete(olc->config.hostname);
    cJSON_Delete(olc->config.linux_config);
    cJSON_Delete(olc->config.mounts);
    cJSON_Delete(olc->config.process.args);
    cJSON_Delete(olc->config.process.cwd);
    cJSON_Delete(olc->config.process.env);
    free(olc);
  }
}

static cJSON* parse_json_file(const char* filename) {
  char* data = read_file_to_string_as_wl_user(filename);
  if (data == NULL) {
    fprintf(ERRORFILE, "ERROR: Cannot read command file %s\n", filename);
    return NULL;
  }

  const char* parse_error_location = NULL;
  cJSON* json = cJSON_ParseWithOpts(data, &parse_error_location, 1);
  if (json == NULL) {
    fprintf(ERRORFILE, "ERROR: Error parsing command file %s at byte offset %ld\n",
        filename, parse_error_location - data);
  }

  free(data);
  return json;
}

static bool parse_oci_launch_cmd_layer(olc_layer_spec* layer_out,
    const cJSON* layer_json) {
  if (!cJSON_IsObject(layer_json)) {
    fputs("ERROR: OCI launch command layer is not an object\n", ERRORFILE);
    return false;
  }

  const cJSON* media_type_json = cJSON_GetObjectItemCaseSensitive(layer_json,
      "mediaType");
  if (!cJSON_IsString(media_type_json)) {
    fputs("ERROR: Bad/Missing media type for OCI launch command layer\n", ERRORFILE);
    return false;
  }

  const cJSON* path_json = cJSON_GetObjectItemCaseSensitive(layer_json, "path");
  if (!cJSON_IsString(path_json)) {
    fputs("ERROR: Bad/Missing path for OCI launch command layer\n", ERRORFILE);
    return false;
  }

  layer_out->media_type = strdup(media_type_json->valuestring);
  layer_out->path = strdup(path_json->valuestring);
  return true;
}

static olc_layer_spec* parse_oci_launch_cmd_layers(unsigned int* num_layers_out,
    const cJSON* layers_json) {
  if (!cJSON_IsArray(layers_json)) {
    fputs("ERROR: Bad/Missing OCI launch command layers\n", ERRORFILE);
    return NULL;
  }

  unsigned int num_layers = (unsigned int) cJSON_GetArraySize(layers_json);
  if (num_layers <= 0) {
    return NULL;
  }

  olc_layer_spec* layers = calloc(num_layers, sizeof(*layers));
  if (layers == NULL) {
    fprintf(ERRORFILE, "ERROR: Cannot allocate memory for %d layers\n",
        num_layers + 1);
    return NULL;
  }

  unsigned int layer_index = 0;
  const cJSON* e;
  cJSON_ArrayForEach(e, layers_json) {
    if (layer_index >= num_layers) {
      fputs("ERROR: Iterating past end of layer array\n", ERRORFILE);
      free_olc_layers(layers, layer_index);
      return NULL;
    }

    if (!parse_oci_launch_cmd_layer(&layers[layer_index], e)) {
      free_olc_layers(layers, layer_index);
      return NULL;
    }

    ++layer_index;
  }

  *num_layers_out = layer_index;
  return layers;
}

static int parse_reap_layers_keep(cJSON* json) {
  if (!cJSON_IsNumber(json)) {
    fputs("ERROR: Bad/Missing OCI reap layer keep number\n", ERRORFILE);
    return -1;
  }
  return json->valueint;
}

static void parse_oci_launch_cmd_oci_config(oci_config* oc, cJSON* oc_json) {
  if (!cJSON_IsObject(oc_json)) {
    fputs("ERROR: Bad/Missing OCI runtime config in launch command\n", ERRORFILE);
    return;
  }
  oc->hostname = cJSON_DetachItemFromObjectCaseSensitive(oc_json, "hostname");
  oc->linux_config = cJSON_DetachItemFromObjectCaseSensitive(oc_json, "linux");
  oc->mounts = cJSON_DetachItemFromObjectCaseSensitive(oc_json, "mounts");

  cJSON* process_json = cJSON_GetObjectItemCaseSensitive(oc_json, "process");
  if (!cJSON_IsObject(process_json)) {
    fputs("ERROR: Bad/Missing process section in OCI config\n", ERRORFILE);
    return;
  }
  oc->process.args = cJSON_DetachItemFromObjectCaseSensitive(
      process_json, "args");
  oc->process.cwd = cJSON_DetachItemFromObjectCaseSensitive(
      process_json, "cwd");
  oc->process.env = cJSON_DetachItemFromObjectCaseSensitive(
      process_json, "env");
}

static bool is_valid_layer_media_type(char* media_type) {
  if (media_type == NULL) {
    return false;
  }

  if (strcmp(SQUASHFS_MEDIA_TYPE, media_type)) {
    fprintf(ERRORFILE, "ERROR: Unrecognized layer media type: %s\n", media_type);
    return false;
  }

  return true;
}

static bool is_valid_oci_launch_cmd_layers(olc_layer_spec* layers,
    unsigned int num_layers) {
  if (layers == NULL) {
    return false;
  }
  unsigned int i;
  for (i = 0; i < num_layers; ++i) {
    if (!is_valid_layer_media_type(layers[i].media_type)) {
      return false;
    }
    if (layers[i].path == NULL) {
      return false;
    }
  }

  return true;
}

static bool is_valid_oci_config_linux_resources(const cJSON* oclr) {
  if (!cJSON_IsObject(oclr)) {
    fputs("ERROR: OCI config linux resources missing or not an object\n", ERRORFILE);
    return false;
  }

  bool all_sections_ok = true;
  const cJSON* e;
  cJSON_ArrayForEach(e, oclr) {
    if (strcmp("blockIO", e->string) == 0) {
      // block I/O settings allowed
    } else if (strcmp("cpu", e->string) == 0) {
      // cpu settings allowed
    } else if (strcmp("memory", e->string) == 0) {
      // memory settings allowed. (added for storm. hadoop doesn't allow this)
    } else {
      fprintf(ERRORFILE,
          "ERROR: Unrecognized OCI config linux resources element: %s\n", e->string);
      all_sections_ok = false;
    }
  }

  return all_sections_ok;
}

static bool is_valid_oci_config_linux_seccomp(const cJSON* ocls) {
  // TODO: seccomp validation
  return true;
}

static bool is_valid_oci_config_linux(const cJSON* ocl) {
  if (!cJSON_IsObject(ocl)) {
    fputs("ERROR: OCI config linux section missing or not an object\n", ERRORFILE);
    return false;
  }

  bool has_cgroup_path = false;
  bool all_sections_ok = true;
  const cJSON* e;
  cJSON_ArrayForEach(e, ocl) {
    if (strcmp("cgroupsPath", e->string) == 0) {
      has_cgroup_path = true;
      if (!cJSON_IsString(e)) {
        all_sections_ok = false;
      }
    } else if (strcmp("resources", e->string) == 0) {
      all_sections_ok &= is_valid_oci_config_linux_resources(e);
    } else if (strcmp("seccomp", e->string) == 0) {
      all_sections_ok &= is_valid_oci_config_linux_seccomp(e);
    } else {
      fprintf(ERRORFILE, "ERROR: Unrecognized OCI config linux element: %s\n",
          e->string);
      all_sections_ok = false;
    }
  }

  return has_cgroup_path && all_sections_ok;
}

static bool is_valid_mount_options(const cJSON* mo) {
  if (!cJSON_IsArray(mo)) {
    fputs("ERROR: OCI config mount options not an array\n", ERRORFILE);
    return false;
  }

  bool has_rbind = false;
  bool has_rprivate = false;
  const cJSON* e;
  cJSON_ArrayForEach(e, mo) {
    if (!cJSON_IsString(e)) {
      fputs("ERROR: OCI config mount option is not a string\n", ERRORFILE);
      return false;
    }
    if (strcmp("rbind", e->valuestring) == 0) {
      has_rbind = true;
    } else if (strcmp("rprivate", e->valuestring) == 0) {
      has_rprivate = true;
    }
  }

  if (!has_rbind) {
    fputs("ERROR: OCI config mount options missing rbind\n", ERRORFILE);
    return false;
  }
  if (!has_rprivate) {
    fputs("ERROR: OCI config mount options missing rprivate\n", ERRORFILE);
    return false;
  }

  return true;
}

static bool is_valid_mount(const cJSON* mount) {
  if (!cJSON_IsObject(mount)) {
    fputs("ERROR: OCI config mount entry is not an object\n", ERRORFILE);
    return false;
  }

  bool has_type = false;
  bool has_options = false;
  char* source = NULL;
  char* destination = NULL;
  const cJSON* e;
  cJSON_ArrayForEach(e, mount) {
    if (strcmp("type", e->string) == 0) {
      if (!cJSON_IsString(e) || strcmp("bind", e->valuestring)) {
        fputs("ERROR: OCI config mount is not bind type\n", ERRORFILE);
        return false;
      }
      has_type = true;
    } else if (strcmp("source", e->string) == 0) {
      if (!cJSON_IsString(e)) {
        fputs("ERROR: OCI config mount source is not a string\n", ERRORFILE);
        return false;
      }
      source = e->valuestring;
    } else if (strcmp("destination", e->string) == 0) {
      if (!cJSON_IsString(e)) {
        fputs("ERROR: OCI config mount destination is not a string\n", ERRORFILE);
        return false;
      }
      destination = e->valuestring;
    } else if (strcmp("options", e->string) == 0) {
      if (!is_valid_mount_options(e)) {
        return false;
      }
      has_options = true;
    } else {
      fprintf(ERRORFILE, "ERROR: Unrecognized OCI config mount parameter: %s\n",
          e->string);
      return false;
    }
  }

  if (!has_type) {
    fputs("ERROR: OCI config mount missing mount type\n", ERRORFILE);
    return false;
  }
  if (!has_options) {
    fputs("ERROR: OCI config mount missing mount options\n", ERRORFILE);
    return false;
  }
  if (source == NULL) {
    fputs("ERROR: OCI config mount missing source\n", ERRORFILE);
    return false;
  }
  if (destination == NULL) {
    fputs("ERROR: OCI config mount missing destination\n", ERRORFILE);
    return false;
  }

  // TODO: Need to add mount source/dest whitelist checking here.

  return true;
}

static bool is_valid_oci_config_mounts(const cJSON* ocm) {
  if (ocm == NULL) {
    return true;  // OK to have no extra mounts
  }
  if (!cJSON_IsArray(ocm)) {
    fputs("ERROR: OCI config mounts is not an array\n", ERRORFILE);
    return false;
  }

  bool all_mounts_ok = true;
  const cJSON* e;
  cJSON_ArrayForEach(e, ocm) {
    all_mounts_ok &= is_valid_mount(e);
  }

  return all_mounts_ok;
}

static bool is_valid_oci_config_process(const oci_config_process* ocp) {
  if (ocp == NULL) {
    return false;
  }

  if (!cJSON_IsArray(ocp->args)) {
    fputs("ERROR: OCI config process args is missing or not an array\n", ERRORFILE);
    return false;
  }

  const cJSON* e;
  cJSON_ArrayForEach(e, ocp->args) {
    if (!cJSON_IsString(e)) {
      fputs("ERROR: OCI config process args has a non-string in array\n", ERRORFILE);
      return false;
    }
  }

  if (!cJSON_IsString(ocp->cwd)) {
    fputs("ERROR: Bad/Missing OCI config process cwd\n", ERRORFILE);
    return false;
  }

  if (!cJSON_IsArray(ocp->env)) {
    fputs("ERROR: OCI config process env is missing or not an array\n", ERRORFILE);
    return false;
  }
  cJSON_ArrayForEach(e, ocp->env) {
    if (!cJSON_IsString(e)) {
      fputs("ERROR: OCI config process env has a non-string in array\n", ERRORFILE);
      return false;
    }
  }

  return true;
}

static bool is_valid_oci_config(const oci_config* oc) {
  bool is_valid = true;
  if (oc->hostname != NULL && !cJSON_IsString(oc->hostname)) {
    fputs("ERROR: OCI config hostname is not a string\n", ERRORFILE);
    is_valid = false;
  }
  is_valid &= is_valid_oci_config_linux(oc->linux_config);
  is_valid &= is_valid_oci_config_mounts(oc->mounts);
  is_valid &= is_valid_oci_config_process(&oc->process);
  return is_valid;
}

//we expect input to be NUL-terminated.
static bool all_uuid_digit(const char* input) {
  if (0 == strlen(input)) {
    return false;
  }
  int i;
  for (i = 0; i < strlen(input); ++i) {
    if ((input[i] < '0' || input[i] > '9')
      && (input[i] < 'a' || input[i] > 'f')
      && (input[i] < 'A' || input[i] > 'F')
      && (input[i] != '-')) {
      return false;
    }
  }
  return true;
}

static bool validate_container_id(const char* input) {
  // The container id will be the same as the worker id, with a prefix of "PORTNUM-"
  // Worker id is a type 4 UUID, e.g. 85afb30b-286e-4d32-ab7a-9d5aad89bb88
  // Container id for this worker on port 6702 would be: 6702-85afb30b-286e-4d32-ab7a-9d5aad89bb88
  int min_length = 36;
  int max_length = min_length + 6; // max port number plus dash
  //using strnlen to avoid walking across memory.
  int actual_length = strnlen(input, max_length + 1);
  if (actual_length < min_length) {
    return false;
  }
  if (actual_length > max_length) {
    return false;
  }

  return all_uuid_digit(input);
}

static bool is_valid_oci_launch_cmd(const oci_launch_cmd* olc) {
  if (olc == NULL) {
    return false;
  }

  if (olc->username == NULL) {
    fputs("ERROR: OCI command has bad/missing username\n", ERRORFILE);
    return false;
  }

  if (olc->container_id == NULL) {
    fputs("ERROR: OCI command has bad/missing container ID\n", ERRORFILE);
    return false;
  }

  if (!validate_container_id(olc->container_id)) {
    fprintf(ERRORFILE, "ERROR: Bad container id in OCI command: %s\n",
        olc->container_id);
    return false;
  }

  if (olc->pid_file == NULL) {
    fputs("ERROR: OCI command has bad/missing pid file\n", ERRORFILE);
    return false;
  }
  struct stat statbuf;
  if (stat(olc->pid_file, &statbuf) == 0) {
    fprintf(ERRORFILE, "ERROR: pid file already exists: %s\n", olc->pid_file);
    return false;
  }
  if (errno != ENOENT) {
    fprintf(ERRORFILE, "ERROR: Error accessing %s : %s in is_valid_oci_launch_cmd\n", olc->pid_file,
        strerror(errno));
    return false;
  }

  if (olc->script_path == NULL) {
    fputs("ERROR: OCI command has bad/missing container script path\n", ERRORFILE);
    return false;
  }

  if (!is_valid_oci_launch_cmd_layers(olc->layers, olc->num_layers)) {
    return false;
  }

  if (olc->num_reap_layers_keep < 0) {
    fprintf(ERRORFILE, "ERROR: Bad number of layers to preserve: %d\n",
        olc->num_reap_layers_keep);
    return false;
  }

  return is_valid_oci_config(&olc->config);
}

/**
 * Read, parse, and validate an OCI container launch command.
 *
 * Returns a pointer to the launch command or NULL on error.
 */
oci_launch_cmd* parse_oci_launch_cmd(const char* command_filename) {
  oci_launch_cmd* olc = NULL;
  cJSON* olc_json = NULL;

  olc_json = parse_json_file(command_filename);
  if (olc_json == NULL) {
    goto cleanup;
  }

  olc = calloc(1, sizeof(*olc));
  if (olc == NULL) {
    fprintf(ERRORFILE, "ERROR: Unable to allocate %ld bytes in parse_oci_launch_cmd\n", sizeof(*olc));
    goto cleanup;
  }

  char* username = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      olc_json, "username"));
  if (username != NULL) {
    olc->username = strdup(username);
  }

  char* container_id = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      olc_json, "containerId"));
  if (container_id != NULL) {
    olc->container_id = strdup(container_id);
  }

  char* pid_file = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      olc_json, "pidFile"));
  if (pid_file != NULL) {
    olc->pid_file = strdup(pid_file);
  }

  char* script_path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
      olc_json, "containerScriptPath"));
  if (script_path != NULL) {
    olc->script_path = strdup(script_path);
  }

  olc->layers = parse_oci_launch_cmd_layers(&olc->num_layers,
      cJSON_GetObjectItemCaseSensitive(olc_json,"layers"));
  olc->num_reap_layers_keep = parse_reap_layers_keep(
      cJSON_GetObjectItemCaseSensitive(olc_json, "reapLayerKeepCount"));

  parse_oci_launch_cmd_oci_config(&olc->config,
      cJSON_GetObjectItemCaseSensitive(olc_json, "ociRuntimeConfig"));

cleanup:
  cJSON_Delete(olc_json);
  if (!is_valid_oci_launch_cmd(olc)) {
    free_oci_launch_cmd(olc);
    olc = NULL;
  }
  return olc;
}