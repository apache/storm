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
#ifndef OCI_OCI_LAUNCH_CMD_H
#define OCI_OCI_LAUNCH_CMD_H

#include "utils/cJSON.h"

// NOTE: Update free_oci_launch_cmd when this is changed.
typedef struct oci_launch_cmd_layer_spec {
  char* media_type; // MIME type for layer data
  char* path;       // local filesystem location of layer data
} olc_layer_spec;

// NOTE: Update free_oci_launch_cmd when this is changed.
typedef struct oci_config_process_struct {
  cJSON* args;       // execve-style command and arguments
  cJSON* cwd;        // working dir for container
  cJSON* env;        // execve-style environment
} oci_config_process;

// NOTE: Update free_oci_launch_cmd when this is changed.
typedef struct oci_config_struct {
  cJSON* hostname;            // hostname for the container
  cJSON* linux_config;        // Linux section of the OCI config
  cJSON* mounts;              // bind-mounts for the container
  oci_config_process process; // process config for the container
} oci_config;

// NOTE: Update free_oci_launch_cmd when this is changed.
typedef struct oci_launch_cmd_struct {
  char* username;             // user name of the container user
  char* container_id;         // container ID
  char* pid_file;             // pid file path to create
  char* script_path;          // path to container launch script
  olc_layer_spec* layers;     // array of layers
  unsigned int num_layers;    // number of entries in the layers array
  int num_reap_layers_keep;   // number of total layer mounts to preserve
  oci_config config;          // OCI config for the container
} oci_launch_cmd;


/**
 * Free an OCI launch command structure and all memory associated with it.
 */
void free_oci_launch_cmd(oci_launch_cmd* olc);

/**
 * Read, parse, and validate an OCI container launch command.
 *
 * Returns a pointer to the launch command or NULL on error.
 */
oci_launch_cmd* parse_oci_launch_cmd(const char* command_filename);

#endif /* OCI_OCI_LAUNCH_CMD_H */