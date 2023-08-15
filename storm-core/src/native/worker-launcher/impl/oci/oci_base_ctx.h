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
#ifndef OCI_OCI_BASE_CTX_H
#define OCI_OCI_BASE_CTX_H

#include <stdbool.h>

// Length of layer basename, equal to the hexstring length of SHA256
#define LAYER_NAME_LENGTH       64

// NOTE: Update init_oci_base_ctx and destroy_oci_base_ctx when this is changed.
typedef struct oci_base_ctx_struct {
  char* run_root;             // root directory of filesystem database
  int layers_lock_fd;         // file descriptor for layers lock file
  int layers_lock_state;      // lock state: F_RDLCK, F_WRLCK, or F_UNLCK
} oci_base_ctx;


/**
 * Allocates and initializes an OCI base context.
 *
 * Returns a pointer to the allocated and initialized context or NULL on error.
 */
oci_base_ctx* alloc_oci_base_ctx();

/**
 * Free an OCI base context and all memory associated with it.
 */
void free_oci_base_ctx(oci_base_ctx* ctx);

/**
 * Initialize an uninitialized OCI base context.
 */
void init_oci_base_ctx(oci_base_ctx* ctx);

/**
 * Releases the resources underneath an OCI base context but does NOT free the
 * structure itself. This is particularly useful for stack-allocated contexts
 * or structures that embed the context.
 * free_oci_base_ctx should be used for heap-allocated contexts.
 */
void destroy_oci_base_ctx(oci_base_ctx* ctx);

/**
 * Opens the base context for use. This will create the container runtime
 * root directory and layer lock files, if necessary.
 *
 * Returns true on success or false if there was an error.
 */
bool open_oci_base_ctx(oci_base_ctx* cx);

/**
 * Allocates and opens a base context.
 *
 * Returns a pointer to the context or NULL on error.
 */
oci_base_ctx* setup_oci_base_ctx();

/**
 * Acquire the layers read lock.
 *
 * Returns true on success or false on error.
 */
bool acquire_oci_layers_read_lock(oci_base_ctx* ctx);

/**
 * Acquire the layers write lock.
 *
 * Returns true on success or false on error.
 */
bool acquire_oci_layers_write_lock(oci_base_ctx* ctx);

/**
 * Release the layers lock.
 *
 * Returns true on success or false on error.
 */
bool release_oci_layers_lock(oci_base_ctx* ctx);

/**
 * Get the path to the runtime layers directory.
 *
 * Returns the heap-allocated path to the layers directory or NULL on error.
 */
char* get_oci_layers_path(const char* run_root);

/**
 * Get the path to a layer directory.
 *
 * Returns the heap-allocated path to the layer directory or NULL on error.
 */
char* get_oci_layer_path(const char* run_root, const char* layer_name);

/**
 * Get the path to a layer's mountpoint.
 *
 * Returns the heap-allocated path to the layer's mountpoint or NULL on error.
 */
char* get_oci_layer_mount_path(const char* layer_path);

/**
 * Get the layer path from a layer's mountpoint.
 *
 * Returns the heap-allocated path to the layer directory or NULL on error.
 */
char* get_oci_layer_path_from_mount_path(const char* mount_path);

#endif /* OCI_OCI_BASE_CTX_H */
