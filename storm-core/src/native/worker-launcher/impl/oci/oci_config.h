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
#ifndef OCI_OCI_CONFIG_H
#define OCI_OCI_CONFIG_H

// Prefix for all OCI config keys
#define OCI_CONFIG_PREFIX       "worker.launcher.oci."

// Configuration for top-level directory of runtime database
// Ideally this should be configured to a tmpfs or other RAM-based filesystem.
#define OCI_RUN_ROOT_CONFIG_KEY OCI_CONFIG_PREFIX "run-root"
#define DEFAULT_OCI_RUN_ROOT    "/run/worker-launcher"

// Configuration for the path to the OCI runc executable on the host
#define OCI_RUNC_CONFIG_KEY OCI_CONFIG_PREFIX "runc"
#define DEFAULT_OCI_RUNC   "/usr/bin/runc"

#endif /* OCI_OCI_CONFIG_H */
