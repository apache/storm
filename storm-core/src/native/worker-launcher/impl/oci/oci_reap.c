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
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <mntent.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "worker-launcher.h"
#include "configuration.h"
#include "oci.h"
#include "oci_base_ctx.h"
#include "oci_reap.h"
#include "oci_config.h"


#define DEV_LOOP_PREFIX       "/dev/loop"
#define DEV_LOOP_PREFIX_LEN   (sizeof(DEV_LOOP_PREFIX) - 1)
#define DELETED_SUFFIX        " (deleted)\n"
#define DELETED_SUFFIX_LEN    (sizeof(DELETED_SUFFIX) - 1)

#define NUM_ROOTFS_UNMOUNT_ATTEMPTS      40
#define MAX_ROOTFS_UNMOUNT_BACKOFF_MSEC  1000

// The size of the buffer to use when reading the mount table. This should be
// large enough so ideally the mount table is read all at once.
// Otherwise the mount table could change in-between underlying read() calls
// and result in a table with missing or corrupted entries.
#define MOUNT_TABLE_BUFFER_SIZE (1024*1024)

// NOTE: Update destroy_dent_stat when this is updated.
typedef struct dent_stat_struct {
  char* basename;         // basename of directory entry
  struct timespec mtime;  // modification time
} dent_stat;

// NOTE: Update init_dent_stats and destroy_dent_stats when this is changed.
typedef struct dent_stats_array_struct {
  dent_stat* stats;       // array of dent_stat structures
  size_t capacity;        // capacity of the stats array
  size_t length;          // number of valid entries in the stats array
} dent_stats_array;


/**
 * Releases the resources associated with a dent_stat structure but
 * does NOT free the structure itself. This is particularly useful for
 * stack-allocated structures or other structures that embed this structure.
 */
static void destroy_dent_stat(dent_stat* ds) {
  if (ds != NULL) {
    free(ds->basename);
    ds->basename = NULL;
  }
}

/**
 * Initialize an uninitialized dent_stats_array with the specified
 * number of entries as its initial capacity.
 *
 * Returns true on success or false on error.
 */
static bool init_dent_stats(dent_stats_array* dsa, size_t initial_size) {
  memset(dsa, 0, sizeof(*dsa));
  dsa->stats = malloc(sizeof(*dsa->stats) * initial_size);
  if (dsa->stats == NULL) {
    return false;
  }
  dsa->capacity = initial_size;
  dsa->length = 0;
  return true;
}

/**
 * Allocates and initializes a dent_stats_array with the specified
 * number of entries as its initial capacity.
 *
 * Returns a pointer to the dent_stats_array or NULL on error.
 */
static dent_stats_array* alloc_dent_stats(size_t initial_size) {
  dent_stats_array* dsa = malloc(sizeof(*dsa));
  if (dsa != NULL) {
    if (!init_dent_stats(dsa, initial_size)) {
      free(dsa);
      dsa = NULL;
    }
  }
  return dsa;
}

/**
 * Grows the capacity of a dent_stats_array to the new specified number of
 * elements.
 *
 * Returns true on success or false on error.
 */
static bool realloc_dent_stats(dent_stats_array* dsa, size_t new_size) {
  if (new_size < dsa->length) {
    // New capacity would result in a truncation.
    return false;
  }

  dent_stat* new_stats = realloc(dsa->stats, new_size * sizeof(*dsa->stats));
  if (new_stats == NULL) {
    return false;
  }

  dsa->stats = new_stats;
  dsa->capacity = new_size;
  return true;
}

/**
 * Append a new dent_stat entry to a dent_stats_array, reallocating the
 * array if necessary with the specified increase in capacity.
 *
 * Returns true on success or false on error.
 */
static bool append_dent_stat(dent_stats_array* dsa, size_t stats_size_incr,
    const char* basename, const struct timespec* mtime) {
  if (dsa->length == dsa->capacity) {
    if (!realloc_dent_stats(dsa, dsa->capacity + stats_size_incr)) {
      return false;
    }
  }

  char* ds_name = strdup(basename);
  if (ds_name == NULL) {
    return false;
  }

  dent_stat* ds = &dsa->stats[dsa->length++];
  ds->basename = ds_name;
  ds->mtime = *mtime;
  return true;
}

/**
 * Releases the resources associated with a dent_stats_array structure but
 * does NOT free the structure itself. This is particularly useful for
 * stack-allocated contexts or other structures that embed this structure.
 */
static void destroy_dent_stats(dent_stats_array* dsa) {
  if (dsa != NULL ) {
    size_t i;
    for (i = 0; i < dsa->length; ++i) {
      destroy_dent_stat(&dsa->stats[i]);
    }
    free(dsa->stats);
    dsa->capacity = 0;
    dsa->length = 0;
  }
}

/**
 * Frees a dent_stats_array structure and all memory associted with it.
 */
static void free_dent_stats(dent_stats_array* dsa) {
  destroy_dent_stats(dsa);
  free(dsa);
}

/**
 * Get the array of dent_stats for the layers directory.
 * Only directory entries that look like layers will be returned.
 *
 * Returns the array of dent_stats or NULL on error.
 */
static dent_stats_array* get_dent_stats(int layers_fd) {
  DIR* layers_dir = NULL;
  // number of stat buffers to allocate each time we run out
  const size_t stats_size_incr = 8192;
  dent_stats_array* dsa = alloc_dent_stats(stats_size_incr);
  if (dsa == NULL) {
    return NULL;
  }

  int dir_fd = dup(layers_fd);
  if (dir_fd == -1) {
    fprintf(ERRORFILE, "ERROR: Unable to duplicate layer dir fd: %s\n",
        strerror(errno));
    goto fail;
  }

  layers_dir = fdopendir(dir_fd);
  if (layers_dir == NULL) {
    fprintf(ERRORFILE, "ERROR: Cannot open layers directory: %s\n", strerror(errno));
    goto fail;
  }

  struct dirent* de;
  while ((de = readdir(layers_dir)) != NULL) {
    // skip entries that don't look like layers
    if (strlen(de->d_name) != LAYER_NAME_LENGTH) {
      continue;
    }

    struct stat statbuf;
    if (fstatat(layers_fd, de->d_name, &statbuf, AT_SYMLINK_NOFOLLOW) == -1) {
      if (errno == ENOENT) {
        continue;
      }
      fprintf(ERRORFILE, "ERROR: Error getting stats for layer %s : %s\n", de->d_name,
          strerror(errno));
      goto fail;
    }

    if (!append_dent_stat(dsa, stats_size_incr, de->d_name,
        &statbuf.st_mtim)) {
      fputs("ERROR: Unable to allocate memory in get_dent_stats\n", ERRORFILE);
      goto fail;
    }
  }

cleanup:
  if (layers_dir != NULL) {
    closedir(layers_dir);
  }
  return dsa;

fail:
  free_dent_stats(dsa);
  dsa = NULL;
  goto cleanup;
}

/**
 * Umount a layer and remove the directories associated with the layer mount.
 *
 * Returns true on success or false on error.
 */
static bool unmount_layer(const char* layer_dir_path) {
  char* mount_path = get_oci_layer_mount_path(layer_dir_path);
  if (mount_path == NULL) {
    fputs("ERROR: Unable to allocate memory in unmount_layer\n", ERRORFILE);
    return false;
  }

  bool result = false;
  if (umount(mount_path) == -1) {
    if (errno == EBUSY) {
      // Layer is in use by another container.
      goto cleanup;
    } else if (errno != ENOENT && errno != EINVAL) {
      fprintf(ERRORFILE, "ERROR: Error unmounting %s : %s\n", mount_path,
          strerror(errno));
      goto cleanup;
    }
  } else {
    // unmount was successful so report success even if directory removals
    // fail after this.
    result = true;
  }

  if (rmdir(mount_path) == -1 && errno != ENOENT) {
    fprintf(ERRORFILE, "ERROR: Error removing %s : %s in rmdir(mount_path)\n", mount_path,
        strerror(errno));
    goto cleanup;
  }

  if (rmdir(layer_dir_path) == -1 && errno != ENOENT) {
    fprintf(ERRORFILE, "ERROR: Error removing %s : %s in rmdir(layer_dir_path)\n", layer_dir_path,
        strerror(errno));
    goto cleanup;
  }

  result = true;

cleanup:
  free(mount_path);
  return result;
}

/**
 * Order directory entries by increasing modification time.
 */
static int compare_dent_stats_mtime(const void* va, const void* vb) {
  const dent_stat* a = (const dent_stat*)va;
  const dent_stat* b = (const dent_stat*)vb;
  if (a->mtime.tv_sec < b->mtime.tv_sec) {
    return -1;
  } else if (a->mtime.tv_sec > b->mtime.tv_sec) {
    return 1;
  }
  return a->mtime.tv_nsec - b->mtime.tv_nsec;
}

static bool do_reap_layer_mounts_with_lock(oci_base_ctx* ctx,
    int layers_fd, int num_preserve) {
  dent_stats_array* dsa = get_dent_stats(layers_fd);
  if (dsa == NULL) {
    return false;
  }

  qsort(&dsa->stats[0], dsa->length, sizeof(*dsa->stats),
      compare_dent_stats_mtime);

  bool result = false;
  size_t num_remain = dsa->length;
  if (num_remain <= num_preserve) {
    result = true;
    goto cleanup;
  }

  if (!acquire_oci_layers_write_lock(ctx)) {
    fputs("ERROR: Unable to acquire layer write lock\n", ERRORFILE);
    goto cleanup;
  }
  size_t i;
  for (i = 0; i < dsa->length && num_remain > num_preserve; ++i) {
    char* layer_dir_path = get_oci_layer_path(ctx->run_root,
        dsa->stats[i].basename);
    if (layer_dir_path == NULL) {
      fputs("ERROR: Unable to allocate memory in do_reap_layer_mounts_with_lock\n", ERRORFILE);
      goto cleanup;
    }
    if (unmount_layer(layer_dir_path)) {
      --num_remain;
      printf("Unmounted layer %s\n", dsa->stats[i].basename);
    }
    free(layer_dir_path);
  }

  result = true;

cleanup:
  free_dent_stats(dsa);
  return result;
}

/**
 * Determine if the specified loopback device is associated with a file that
 * has been deleted.
 *
 * Returns true if the loopback file is deleted or false otherwise or on error.
 */
bool is_loop_file_deleted(const char* loopdev) {
  bool result = false;
  FILE* f = NULL;
  char* path = NULL;
  char* linebuf = NULL;

  // locate the numeric part of the loop device
  const char* loop_num_str = loopdev + DEV_LOOP_PREFIX_LEN;

  if (asprintf(&path, "/sys/devices/virtual/block/loop%s/loop/backing_file",
      loop_num_str) == -1) {
    return false;
  }

  f = fopen(path, "r");
  if (f == NULL) {
    goto cleanup;
  }

  size_t linebuf_len = 0;
  ssize_t len = getline(&linebuf, &linebuf_len, f);
  if (len <= DELETED_SUFFIX_LEN) {
    goto cleanup;
  }

  result = !strcmp(DELETED_SUFFIX, linebuf + len - DELETED_SUFFIX_LEN);

cleanup:
  if (f != NULL) {
    fclose(f);
  }
  free(linebuf);
  free(path);
  return result;
}

static bool copy_mntent(struct mntent* dest, const struct mntent* src) {
  memset(dest, 0, sizeof(*dest));
  if (src->mnt_fsname != NULL) {
    dest->mnt_fsname = strdup(src->mnt_fsname);
    if (dest->mnt_fsname == NULL) {
      return false;
    }
  }
  if (src->mnt_dir != NULL) {
    dest->mnt_dir = strdup(src->mnt_dir);
    if (dest->mnt_dir == NULL) {
      return false;
    }
  }
  if (src->mnt_type != NULL) {
    dest->mnt_type = strdup(src->mnt_type);
    if (dest->mnt_type == NULL) {
      return false;
    }
  }
  if (src->mnt_opts != NULL) {
    dest->mnt_opts = strdup(src->mnt_opts);
    if (dest->mnt_opts == NULL) {
      return false;
    }
  }
  dest->mnt_freq = src->mnt_freq;
  dest->mnt_passno = src->mnt_passno;
  return true;
}

static void free_mntent_array(struct mntent* entries, size_t num_entries) {
  if (entries != NULL) {
    size_t i;
    for (i = 0; i < num_entries; ++i) {
      struct mntent* me = entries + i;
      free(me->mnt_fsname);
      free(me->mnt_dir);
      free(me->mnt_type);
      free(me->mnt_opts);
    }
    free(entries);
  }
}

/**
 * Get the array of mount table entries that are layer mounts.
 *
 * Returns the heap-allocated array of mount entries or NULL on error.
 * The num_entries argument is updated to the number of elements in the array.
 */
static struct mntent* get_layer_mounts(size_t* num_entries_out,
    const char* layers_path) {
  const size_t layers_path_len = strlen(layers_path);
  char* read_buffer = NULL;
  FILE* f = NULL;
  const size_t num_entries_per_alloc = 8192;
  size_t num_entries = 0;
  size_t entries_capacity = num_entries_per_alloc;
  struct mntent* entries = malloc(sizeof(*entries) * entries_capacity);
  if (entries == NULL) {
    fputs("ERROR: Unable to allocate memory in get_layer_mounts - malloc\n", ERRORFILE);
    goto fail;
  }

  read_buffer = malloc(MOUNT_TABLE_BUFFER_SIZE);
  if (read_buffer == NULL) {
    fprintf(ERRORFILE, "ERROR: Unable to allocate read buffer of %d bytes\n",
        MOUNT_TABLE_BUFFER_SIZE);
    goto fail;
  }

  f = fopen("/proc/mounts", "r");
  if (f == NULL) {
    fprintf(ERRORFILE, "ERROR: Unable to open /proc/mounts : %s\n", strerror(errno));
    goto fail;
  }

  if (setvbuf(f, read_buffer, _IOFBF, MOUNT_TABLE_BUFFER_SIZE) != 0) {
    fprintf(ERRORFILE, "ERROR: Unable to set mount table buffer to %d\n",
        MOUNT_TABLE_BUFFER_SIZE);
    goto fail;
  }

  struct mntent* me;
  while ((me = getmntent(f)) != NULL) {
    // Skip mounts that are not loopback mounts
    if (strncmp(me->mnt_fsname, DEV_LOOP_PREFIX, DEV_LOOP_PREFIX_LEN)) {
      continue;
    }

    // skip destinations that are not under the layers mount area
    if (strncmp(layers_path, me->mnt_dir, layers_path_len)) {
      continue;
    }

    if (num_entries == entries_capacity) {
      entries_capacity += num_entries_per_alloc;
      entries = realloc(entries, sizeof(*entries) * entries_capacity);
      if (entries == NULL) {
        fputs("ERROR: Unable to allocate memory in get_layer_mounts - realloc\n", ERRORFILE);
        goto fail;
      }
    }

    if (!copy_mntent(entries + num_entries, me)) {
      fputs("ERROR: Failed to copy_mntent", ERRORFILE);
      goto fail;
    }
    ++num_entries;
  }

cleanup:
  if (f != NULL) {
    fclose(f);
  }
  free(read_buffer);
  *num_entries_out = num_entries;
  return entries;

fail:
  free_mntent_array(entries, num_entries);
  entries = NULL;
  num_entries = 0;
  goto cleanup;
}

/**
 * Search for layer mounts that correspond with deleted files and unmount them.
 */
static bool reap_deleted_mounts_with_lock(oci_base_ctx* ctx) {
  const char* layers_path = get_oci_layers_path(ctx->run_root);
  if (layers_path == NULL) {
    fputs("ERROR: Unable to allocate memory in reap_deleted_mounts_with_lock\n", ERRORFILE);
    return false;
  }

  bool result = false;
  size_t num_mnt_entries = 0;
  struct mntent* mnt_entries = get_layer_mounts(&num_mnt_entries, layers_path);
  if (mnt_entries == NULL) {
    fputs("ERROR: Error parsing mount table\n", ERRORFILE);
    goto cleanup;
  }

  bool have_write_lock = false;
  size_t i;
  for (i = 0; i < num_mnt_entries; ++i) {
    const struct mntent* me = mnt_entries + i;
    if (is_loop_file_deleted(me->mnt_fsname)) {
      if (!have_write_lock) {
        if (!acquire_oci_layers_write_lock(ctx)) {
          goto cleanup;
        }
        have_write_lock = true;
      }

      char* layer_dir = get_oci_layer_path_from_mount_path(me->mnt_dir);
      if (layer_dir != NULL) {
        if (unmount_layer(layer_dir)) {
          printf("Unmounted layer %s (deleted)\n", basename(layer_dir));
        }
        free(layer_dir);
      }
    }
  }

  result = true;

cleanup:
  free_mntent_array(mnt_entries, num_mnt_entries);
  return result;
}

/**
 * Equivalent to reap_oci_layer_mounts but avoids the need to re-create the
 * OCI base context.
 */
int reap_oci_layer_mounts_with_ctx(oci_base_ctx* ctx, int num_preserve) {
  int rc = ERROR_OCI_REAP_LAYER_MOUNTS_FAILED;
  int layers_fd = -1;
  char* layers_path = get_oci_layers_path(ctx->run_root);
  if (layers_path == NULL) {
    fputs("ERROR: Unable to allocate memory in reap_oci_layer_mounts_with_ctx\n", ERRORFILE);
    rc = OUT_OF_MEMORY;
    goto cleanup;
  }

  layers_fd = open(layers_path, O_RDONLY | O_NOFOLLOW);
  if (layers_fd == -1) {
    fprintf(ERRORFILE, "ERROR: Unable to open layers directory at %s : %s\n",
        layers_path, strerror(errno));
    goto cleanup;
  }

  if (!acquire_oci_layers_read_lock(ctx)) {
    fputs("ERROR: Unable to obtain layer lock\n", ERRORFILE);
    goto cleanup;
  }

  bool reap_deleted_ok = reap_deleted_mounts_with_lock(ctx);
  bool reap_layers_ok = do_reap_layer_mounts_with_lock(ctx, layers_fd,
      num_preserve);
  if (reap_deleted_ok && reap_layers_ok) {
    rc = 0;
  }

  release_oci_layers_lock(ctx);

cleanup:
  if (layers_fd != -1) {
    close(layers_fd);
  }
  free(layers_path);
  return rc;
}

/**
 * Attempt to trim the number of layer mounts to the specified target number to
 * preserve. Layers are unmounted in a least-recently-used fashion. Layers that
 * are still in use by containers are preserved, so the number of layers mounts
 * after trimming may exceed the target number.
 *
 * Returns 0 on success or a non-zero error code on failure.
 */
int reap_oci_layer_mounts(int num_preserve) {
  int rc = ERROR_OCI_REAP_LAYER_MOUNTS_FAILED;
  oci_base_ctx* ctx = setup_oci_base_ctx();
  if (ctx == NULL) {
    fputs("ERROR: Failed to setup_oci_base_ctx", ERRORFILE);
    return rc;
  }

  rc = reap_oci_layer_mounts_with_ctx(ctx, num_preserve);
  free_oci_base_ctx(ctx);
  return rc;
}

int cleanup_oci_container_by_id(const char* container_id, int num_reap_layers_keep) {
  char* run_root = get_value(OCI_RUN_ROOT_CONFIG_KEY);
  int rc = 0;
  if (run_root == NULL) {
    run_root = strdup(DEFAULT_OCI_RUN_ROOT);
    if (run_root == NULL) {
      fputs("ERROR: Unable to allocate memory in cleanup_oci_container_by_id - strdup", ERRORFILE);
      goto mem_fail;
    }
  }

  oci_overlay_desc* desc = malloc(sizeof(oci_overlay_desc));
  if (desc == NULL) {
    fputs("ERROR: Unable to allocate memory in cleanup_oci_container_by_id - malloc", ERRORFILE);
    goto mem_fail;
  }
  init_oci_overlay_desc(desc);
  if (!init_overlay_descriptor(desc, run_root, container_id)) {
      fputs("ERROR: init_overlay_descriptor Failed\n", ERRORFILE);
      goto mem_fail;
  }
  rc = cleanup_oci_container(container_id, desc->mount_path, desc->top_path, NULL, num_reap_layers_keep);

cleanup:
  free(run_root);
  destroy_oci_overlay_desc(desc);
  free(desc);
  return rc;

mem_fail:
  rc = OUT_OF_MEMORY;
  goto cleanup;
}

char* get_next_word(char* line, int* start_index) {
    int temp = *start_index;
    while (line[*start_index] != ' ') {
        *start_index  += 1;
    }
    char* word = malloc(sizeof(char) * (*start_index - temp + 1));
    int i=0;
    while (temp < *(start_index)) {
        word[i++]  = line[temp++];
    }
    word[i] = '\0';

    while (line[*start_index] == ' ') {
            *start_index += 1;
    }

    return word;
}

static bool unmount_and_remove(const char* path) {
  if (umount(path) == -1 && errno != EINVAL && errno != ENOENT) {
    if (errno == EBUSY) {
      // Layer is in use by another container.
      return false;
    }
    fprintf(ERRORFILE, "ERROR: Error unmounting %s : %s in unmount_and_remove \n", path, strerror(errno));
    return false;
  }
  if (rmdir(path) == -1 && errno != ENOENT) {
    fprintf(ERRORFILE, "ERROR: Error removing mount directory %s : %s\n", path,
        strerror(errno));
    return false;
  }
  return true;
}

static bool unmount_and_remove_with_retry(const char* path, int max_attempts,
    long max_backoff_msec) {
  long backoff_msec = 1;
  int i;
  for (i = 0; i < max_attempts - 1; ++i) {
    if (unmount_and_remove(path)) {
      return true;
    }
    struct timespec ts;
    memset(&ts, 0, sizeof(ts));
    ts.tv_sec = backoff_msec / 1000;
    ts.tv_nsec = (backoff_msec % 1000) * 1000 * 1000;
    nanosleep(&ts, NULL);
    backoff_msec *= 2;
    if (backoff_msec > max_backoff_msec) {
      backoff_msec = max_backoff_msec;
    }
  }

  return unmount_and_remove(path);
}

/**
 * Unmounts the container rootfs directory and MAY unmount layers on the host
 * based on the specified number of total layer mounts on the host specified.
 * We need to delete the container using runc since we launch it in detached mode.
 */
int cleanup_oci_container(const char* container_id, const char* mount_path, const char* top_path, 
  oci_base_ctx* base_ctx, int num_reap_layers_keep) {
  unmount_and_remove_with_retry(mount_path,
      NUM_ROOTFS_UNMOUNT_ATTEMPTS, MAX_ROOTFS_UNMOUNT_BACKOFF_MSEC);
  rmdir_recursive(top_path);
  if (base_ctx == NULL) {
    reap_oci_layer_mounts(num_reap_layers_keep);
  } else {
    reap_oci_layer_mounts_with_ctx(base_ctx, num_reap_layers_keep);
  }

  //delete container
  int rc = 0;
  char* runc_path = get_value(OCI_RUNC_CONFIG_KEY);
  if (runc_path == NULL) {
    runc_path = strdup(DEFAULT_OCI_RUNC);
    if (runc_path == NULL) {
      fputs("ERROR: Unable to allocate memory in cleanup_oci_container\n", ERRORFILE);
      rc = OUT_OF_MEMORY;
      goto cleanup;
    }
  }

  char* cmd = NULL;
  if (asprintf(&cmd, "%s delete %s", runc_path, container_id) == -1) {
    rc = 1;
    goto cleanup;
  }

  fprintf(LOGFILE, "oci cleanup container command: %s\n", cmd);
  if (system(cmd) != 0) {
    fprintf(ERRORFILE, "WARN: oci cleanup container command %s failed\n", cmd);
    rc = 1;
  }

cleanup:
  free(runc_path);
  free(cmd);
  return rc;
}