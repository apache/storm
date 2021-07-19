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
#ifndef OCI_OCI_H
#define OCI_OCI_H

#include <stdbool.h>

/**
 * Run a container via OCI.
 */
int run_oci_container(const char* command_file, const char* worker_artifacts_dir);

// NOTE: Update init_oci_overlay_desc and destroy_oci_overlay_desc
//       when this is changed.
typedef struct oci_overlay_desc_struct {
  char* top_path;             // top-level directory
  char* mount_path;           // overlay mount point under top_path
  char* upper_path;           // root path of upper layer under top_path
  char* work_path;            // overlay work path under top_path
} oci_overlay_desc;


void init_oci_overlay_desc(oci_overlay_desc* desc);

void destroy_oci_overlay_desc(oci_overlay_desc* desc);

bool init_overlay_descriptor(oci_overlay_desc* desc,
    const char* run_root, const char* container_id);

bool rmdir_recursive(const char* path);

#endif /* OCI_OCI_H */