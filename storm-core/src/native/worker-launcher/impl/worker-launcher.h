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
#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>

typedef enum { FALSE, TRUE } boolean;

enum errorcodes {
  INVALID_ARGUMENT_NUMBER = 1,
  INVALID_USER_NAME, //2
  INVALID_COMMAND_PROVIDED, //3
  // SUPER_USER_NOT_ALLOWED_TO_RUN_TASKS (NOT USED) 4
  INVALID_WL_ROOT_DIRS = 5,
  SETUID_OPER_FAILED, //6
  UNABLE_TO_EXECUTE_CONTAINER_SCRIPT, //7
  UNABLE_TO_SIGNAL_CONTAINER, //8
  INVALID_CONTAINER_PID, //9
  // ERROR_RESOLVING_FILE_PATH (NOT_USED) 10
  // RELATIVE_PATH_COMPONENTS_IN_FILE_PATH (NOT USED) 11
  UNABLE_TO_STAT_FILE = 12,
  // FILE_NOT_OWNED_BY_ROOT (NOT USED) 13
  // PREPARE_CONTAINER_DIRECTORIES_FAILED (NOT USED) 14
  // INITIALIZE_CONTAINER_FAILED (NOT USED) 15
  // PREPARE_CONTAINER_LOGS_FAILED (NOT USED) 16
  // INVALID_LOG_DIR (NOT USED) 17
  OUT_OF_MEMORY = 18,
  // INITIALIZE_DISTCACHEFILE_FAILED (NOT USED) 19
  INITIALIZE_USER_FAILED = 20,
  UNABLE_TO_BUILD_PATH, //21
  INVALID_CONTAINER_EXEC_PERMISSIONS, //22
  // PREPARE_JOB_LOGS_FAILED (NOT USED) 23
  INVALID_CONFIG_FILE =  24,
  SETSID_OPER_FAILED = 25,
  WRITE_PIDFILE_FAILED = 26,
  DOCKER_RUN_FAILED=29,
  ERROR_OPENING_FILE = 30,
  ERROR_READING_FILE = 31,
  ERROR_SANITIZING_DOCKER_COMMAND = 39,
  DOCKER_IMAGE_INVALID = 40,
  DOCKER_CONTAINER_NAME_INVALID = 41,
  ERROR_COMPILING_REGEX = 42,
  ERROR_CHANGING_USER = 43,
  ERROR_OCI_RUN_FAILED = 44,
  ERROR_OCI_REAP_LAYER_MOUNTS_FAILED = 45
};

#define LAUNCHER_GROUP_KEY "storm.worker-launcher.group"
#define CONTAINER_SCRIPT "launch_container.sh"
#define MIN_USERID_KEY "min.user.id"
#define BANNED_USERS_KEY "banned.users"
#define DOCKER_BINARY_KEY "docker.binary"
#define NSENTER_BINARY_KEY "nsenter.binary"
#define WORKER_PROFILER_SCRIPT_PATH "worker.profiler.script.path"

/* Macros for min/max. */
#ifndef MIN
#define MIN(a,b) (((a)<(b))?(a):(b))
#endif /* MIN */
#ifndef MAX
#define MAX(a,b) (((a)>(b))?(a):(b))
#endif  /* MAX */

extern struct passwd *user_detail;

// the log file for messages
extern FILE *LOGFILE;
// the log file for error messages
extern FILE *ERRORFILE;

//for debugging purpose: print real, effective and saved userid;
void print_res_uid_gid(char* log_prefix);

int setup_dir_permissions(const char* local_dir, int for_blob_permission, boolean setgid_on_dir);

/**
 * /tmp inside the container is bind mounted to worker-id/tmp directory
 * remove setgid on worker-id/tmp directory so that java profiling can work
 * This is not required for non-container workers. But better to keep them consistent
 */ 
int setup_worker_tmp_permissions(const char *worker_dir);

int exec_as_user(const char * working_dir, const char * args);

int fork_as_user(const char * working_dir, const char * args);

/**
 * delete a directory (or file) recursively. If supervisor_owns_dir,
 * delete the file/dir at path as the supervisor user. (all subdirs/subfiles
 * will be deleted as the current user)
 */
int recursive_delete(const char *path, int supervisor_owns_dir);

// get the executable's filename
char* get_executable();

/**
 * Check the permissions on the worker-launcher to make sure that security is
 * permissible. For this, we need worker-launcher binary to
 *    * be user-owned by root
 *    * be group-owned by a configured special group.
 *    * others do not have any permissions
 *    * be setuid/setgid
 * @param executable_file the file to check
 * @return -1 on error 0 on success.
 */
int check_executor_permissions(char *executable_file);

/**
 * Function used to signal a container launched by the user.
 * The function sends appropriate signal to the process group
 * specified by the pid.
 * @param user the user to send the signal as.
 * @param pid the process id to send the signal to.
 * @param sig the signal to send.
 * @return an errorcode enum value on error, or 0 on success.
 */
int signal_container_as_user(const char *user, int pid, int sig);

// set the uid and gid of the launcher.  This is used when doing some
// priviledged operations for setting the effective uid and gid.
void set_launcher_uid(uid_t user, gid_t group);

/**
 * Is the user a real user account?
 * Checks:
 *   1. Not root
 *   2. UID is above the minimum configured.
 *   3. Not in banned user list
 * Returns NULL on failure
 */
struct passwd* check_user(const char *user);

// set the user
int set_user(const char *user);

// methods to get the directories

char *get_container_launcher_file(const char* work_dir);

/**
 * Ensure that the given path and all of the parent directories are created
 * with the desired permissions.
 */
int mkdirs(const char* path, mode_t perm);

int check_dir(const char* npath, mode_t st_mode, mode_t desired,
   int finalComponent);

int create_validate_dir(const char* npath, mode_t perm, const char* path,
   int finalComponent);

int setup_container_paths(const char* user, const char *container_id,
        const char* work_dir, const char* script_path);

int change_user(uid_t user, gid_t group);

/**
 * Change the effective user to the launcher user
 */
int change_effective_user_to_wl();

/**
 * Change the effective user id to limit damage.
 */
int change_effective_user(uid_t user, gid_t group);

/**
 * Get the docker binary path.
 */
char *get_docker_binary();

/**
 * Run a docker command passing the command file as an argument
 */
int run_docker_cmd(const char * working_dir, const char * command_file);

/**
 * Get the nsenter binary path.
 */
char *get_nsenter_binary();

/**
 * Get the pid of the docker container
 */
int get_docker_container_pid(const char *worker_id);

/**
 * Utility function to concatenate argB to argA using the concat_pattern.
 */
char *concatenate(char *concat_pattern, char *return_path_name, int numArgs, ...);

int profile_oci_container(int container_pid, const char* command_file);