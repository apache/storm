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

#include "configuration.h"
#include "worker-launcher.h"

#include <dirent.h>
#include <fcntl.h>
#include <fts.h>
#include <errno.h>
#include <grp.h>
#include <unistd.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/stat.h>
#include <getopt.h>
#include <regex.h>

static const int DEFAULT_MIN_USERID = 1000;

static const char* DEFAULT_BANNED_USERS[] = {"bin", 0};

static const char* DEFAULT_DOCKER_BINARY_PATH = "/usr/bin/docker";
//For security, we restrict the privileges of docker containers.
//Consequently profiling in the container doesn't work via docker-exec.
//We need to use nsenter instead.
static const char* DEFAULT_NSENTER_BINARY_PATH = "/usr/bin/nsenter";

//struct to store the user details
struct passwd *user_detail = NULL;

//Docker container related constants.
static const char* DOCKER_CLIENT_CONFIG_ARG = "--config=";
static const char* DOCKER_PULL_COMMAND = "pull";

FILE* LOGFILE = NULL;
FILE* ERRORFILE = NULL;

static uid_t launcher_uid = -1;
static gid_t launcher_gid = -1;

void set_launcher_uid(uid_t user, gid_t group) {
  launcher_uid = user;
  launcher_gid = group;
}

/**
 * get the executable filename.
 */
char* get_executable() {
  char buffer[PATH_MAX];
  snprintf(buffer, PATH_MAX, "/proc/%u/exe", getpid());
  char *filename = malloc(PATH_MAX);
  if (NULL == filename) {
    fprintf(ERRORFILE, "malloc failed in get_executable\n");
    exit(-1);
  }
  ssize_t len = readlink(buffer, filename, PATH_MAX);
  if (len == -1) {
    fprintf(ERRORFILE, "Can't get executable name from %s - %s\n", buffer,
            strerror(errno));
    exit(-1);
  } else if (len >= PATH_MAX) {
    fprintf(ERRORFILE, "Executable name %.*s is longer than %d characters.\n",
            PATH_MAX, filename, PATH_MAX);
    exit(-1);
  }
  filename[len] = '\0';
  return filename;
}

int check_executor_permissions(char *executable_file) {
  errno = 0;
  char * resolved_path = realpath(executable_file, NULL);
  if (resolved_path == NULL) {
    fprintf(ERRORFILE,
        "Error resolving the canonical name for the executable : %s!",
        strerror(errno));
    return -1;
  }

  struct stat filestat;
  errno = 0;
  if (stat(resolved_path, &filestat) != 0) {
    fprintf(ERRORFILE, 
            "Could not stat the executable : %s!.\n", strerror(errno));
    return -1;
  }

  uid_t binary_euid = filestat.st_uid; // Binary's user owner
  gid_t binary_gid = filestat.st_gid; // Binary's group owner

  // Effective uid should be root
  if (binary_euid != 0) {
    fprintf(LOGFILE,
        "The worker-launcher binary should be user-owned by root.\n");
    return -1;
  }

  if (binary_gid != getgid()) {
    fprintf(LOGFILE, "ERROR: The configured worker-launcher group %d is different from"
            " the group of the executable %d\n", getgid(), binary_gid);
    return -1;
  }

  // check others do not have read/write/execute permissions
  if ((filestat.st_mode & S_IROTH) == S_IROTH || (filestat.st_mode & S_IWOTH)
      == S_IWOTH || (filestat.st_mode & S_IXOTH) == S_IXOTH) {
    fprintf(LOGFILE,
            "The worker-launcher binary should not have read or write or"
            " execute for others.\n");
    return -1;
  }

  // Binary should be setuid/setgid executable
  if ((filestat.st_mode & S_ISUID) == 0) {
    fprintf(LOGFILE, "The worker-launcher binary should be set setuid.\n");
    return -1;
  }

  return 0;
}

/**
 * Change the effective user id to limit damage.
 */
static int change_effective_user(uid_t user, gid_t group) {
  if (geteuid() == user && getegid() == group) {
    return 0;
  }
  if (seteuid(0) != 0) {
    return -1;
  }
  if (setegid(group) != 0) {
    fprintf(LOGFILE, "Failed to set effective group id %d - %s\n", group,
            strerror(errno));
    return -1;
  }
  if (seteuid(user) != 0) {
    fprintf(LOGFILE, "Failed to set effective user id %d - %s\n", user,
            strerror(errno));
    return -1;
  }
  return 0;
}

/**
 * Change the real and effective user and group to abandon the super user
 * priviledges.
 */
int change_user(uid_t user, gid_t group) {
  if (user == getuid() && user == geteuid() && 
      group == getgid() && group == getegid()) {
    return 0;
  }

  if (seteuid(0) != 0) {
    fprintf(LOGFILE, "unable to reacquire root - %s\n", strerror(errno));
    fprintf(LOGFILE, "Real: %d:%d; Effective: %d:%d\n",
	    getuid(), getgid(), geteuid(), getegid());
    return SETUID_OPER_FAILED;
  }
  if (setgid(group) != 0) {
    fprintf(LOGFILE, "unable to set group to %d - %s\n", group, 
            strerror(errno));
    fprintf(LOGFILE, "Real: %d:%d; Effective: %d:%d\n",
	    getuid(), getgid(), geteuid(), getegid());
    return SETUID_OPER_FAILED;
  }
  if (setuid(user) != 0) {
    fprintf(LOGFILE, "unable to set user to %d - %s\n", user, strerror(errno));
    fprintf(LOGFILE, "Real: %d:%d; Effective: %d:%d\n",
	    getuid(), getgid(), geteuid(), getegid());
    return SETUID_OPER_FAILED;
  }

  return 0;
}

char *get_container_launcher_file(const char* work_dir) {
  char *ret;
  int bytesPrinted = asprintf(&ret, "%s/%s", work_dir, CONTAINER_SCRIPT);
  if (bytesPrinted == -1) {
    //asprintf failed, stop the process
    exit(EXIT_FAILURE);
  }
  return ret;
}

/**
 * Load the user information for a given user name.
 */
static struct passwd* get_user_info(const char* user) {
  int string_size = sysconf(_SC_GETPW_R_SIZE_MAX);
  void* buffer = malloc(string_size + sizeof(struct passwd));
  if (buffer == NULL) {
    fprintf(LOGFILE, "Malloc failed in get_user_info\n");
    return NULL;
  }
  struct passwd *result = NULL;
  if (getpwnam_r(user, buffer, buffer + sizeof(struct passwd), string_size,
		 &result) != 0) {
    free(buffer);
    buffer = NULL;
    fprintf(LOGFILE, "Can't get user information %s - %s\n", user,
	    strerror(errno));
    return NULL;
  }
  return result;
}

/**
 * Is the user a real user account?
 * Checks:
 *   1. Not root
 *   2. UID is above the minimum configured.
 *   3. Not in banned user list
 * Returns NULL on failure
 */
struct passwd* check_user(const char *user) {
  if (strcmp(user, "root") == 0) {
    fprintf(LOGFILE, "Running as root is not allowed\n");
    fflush(LOGFILE);
    return NULL;
  }
  char *min_uid_str = get_value(MIN_USERID_KEY);
  int min_uid = DEFAULT_MIN_USERID;
  if (min_uid_str != NULL) {
    char *end_ptr = NULL;
    min_uid = strtol(min_uid_str, &end_ptr, 10);
    if (min_uid_str == end_ptr || *end_ptr != '\0') {
      fprintf(LOGFILE, "Illegal value of %s for %s in configuration\n", 
	      min_uid_str, MIN_USERID_KEY);
      fflush(LOGFILE);
      free(min_uid_str);
      min_uid_str = NULL;
      return NULL;
    }
    free(min_uid_str);
    min_uid_str = NULL;
  }
  struct passwd *user_info = get_user_info(user);
  if (NULL == user_info) {
    fprintf(LOGFILE, "User %s not found\n", user);
    fflush(LOGFILE);
    return NULL;
  }
  if (user_info->pw_uid < min_uid) {
    fprintf(LOGFILE, "Requested user %s has id %d, which is below the "
	    "minimum allowed %d\n", user, user_info->pw_uid, min_uid);
    fflush(LOGFILE);
    free(user_info);
    user_info = NULL;
    return NULL;
  }
  char **banned_users = get_values(BANNED_USERS_KEY);
  char **banned_user = (banned_users == NULL) ? 
    (char**) DEFAULT_BANNED_USERS : banned_users;
  for(; *banned_user; ++banned_user) {
    if (strcmp(*banned_user, user) == 0) {
      free(user_info);
      user_info = NULL;
      if (banned_users != (char**)DEFAULT_BANNED_USERS) {
        free_values(banned_users);
        banned_users = NULL;
      }
      fprintf(LOGFILE, "Requested user %s is banned\n", user);
      return NULL;
    }
  }
  if (banned_users != NULL && banned_users != (char**)DEFAULT_BANNED_USERS) {
    free_values(banned_users);
    banned_users = NULL;
  }
  return user_info;
}

/**
 * function used to populate and user_details structure.
 */
int set_user(const char *user) {
  // free any old user
  if (user_detail != NULL) {
    free(user_detail);
    user_detail = NULL;
  }
  user_detail = check_user(user);
  if (user_detail == NULL) {
    return -1;
  }

  if (geteuid() == user_detail->pw_uid) {
    return 0;
  }

  if (initgroups(user, user_detail->pw_gid) != 0) {
    fprintf(LOGFILE, "Error setting supplementary groups for user %s: %s\n",
        user, strerror(errno));
    return -1;
  }

  return change_effective_user(user_detail->pw_uid, user_detail->pw_gid);
}

/**
 * Open a file as the worker-launcher user and return a file descriptor for it.
 * Returns -1 on error
 */
static int open_file_as_wl(const char* filename) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(launcher_uid, launcher_gid) != 0) {
    return -1;
  }
  int result = open(filename, O_RDONLY);
  if (result == -1) {
    fprintf(LOGFILE, "Can't open file %s as worker-launcher user - %s\n", filename,
	    strerror(errno));
  }
  if (change_effective_user(user, group)) {
    result = -1;
  }
  return result;
}

/**
 * Copy a file from a fd to a given filename.
 * The input stream is closed.
 * The destination file will be deleted if exists.
 * Return 0 if everything is ok.
 */
static int copy_file(int input, const char* in_filename, 
		     const char* out_filename, mode_t perm) {
  if (remove(out_filename) == -1) {
      fprintf(ERRORFILE, "Can't delete file %s - %s\n", out_filename, strerror(errno));
      return -1;
  }

  const int buffer_size = 128*1024;
  char buffer[buffer_size];
  int out_fd = open(out_filename, O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW, perm);
  if (out_fd == -1) {
    fprintf(LOGFILE, "Can't open %s for output - %s\n", out_filename, 
            strerror(errno));
    return -1;
  }
  ssize_t len = read(input, buffer, buffer_size);
  while (len > 0) {
    ssize_t pos = 0;
    while (pos < len) {
      ssize_t write_result = write(out_fd, buffer + pos, len - pos);
      if (write_result <= 0) {
	fprintf(LOGFILE, "Error writing to %s - %s\n", out_filename,
		strerror(errno));
	close(out_fd);
	return -1;
      }
      pos += write_result;
    }
    len = read(input, buffer, buffer_size);
  }
  if (len < 0) {
    fprintf(LOGFILE, "Failed to read file %s - %s\n", in_filename, 
	    strerror(errno));
    close(out_fd);
    return -1;
  }
  if (close(out_fd) != 0) {
    fprintf(LOGFILE, "Failed to close file %s - %s\n", out_filename, 
	    strerror(errno));
    return -1;
  }
  close(input);
  return 0;
}

/**
 * Sets up permissions for a directory optionally making it user-writable.
 * We set up the permissions r(w)xrws--- so that the file group (should be Storm's user group)
 * has complete access to the directory, and the file user (The topology owner's user)
 * is able to read and execute, and in certain directories, write. The setGID bit is set
 * to make sure any files created under the directory will be accessible to storm's user for
 * cleanup purposes.
 * If setgid_on_dir is FALSE, don't set sticky bit on group permission on the directory.
 */
static int setup_permissions(FTSENT* entry, uid_t euser, int user_write, boolean setgid_on_dir) {
  if (lchown(entry->fts_path, euser, launcher_gid) != 0) {
    fprintf(ERRORFILE, "Failure to exec app initialization process - %s, fts_path=%s\n",
            strerror(errno), entry->fts_path);
     return -1;
  }
  mode_t mode = entry->fts_statp->st_mode;
  // Preserve user read and execute and set group read and write.
  mode_t new_mode = (mode & (S_IRUSR | S_IXUSR)) | S_IRGRP | S_IWGRP;
  if (user_write) {
    new_mode = new_mode | S_IWUSR;
  }
  // If the entry is a directory, Add group execute and setGID bits.
  if ((mode & S_IFDIR) == S_IFDIR) {
    new_mode = new_mode | S_IXGRP;
    if (setgid_on_dir) {
      new_mode = new_mode | S_ISGID;
    }
  }
  if (chmod(entry->fts_path, new_mode) != 0) {
    fprintf(ERRORFILE, "Failure to exec app initialization process - %s, fts_path=%s\n",
            strerror(errno), entry->fts_path);
    return -1;
  }
  return 0;
}


int setup_dir_permissions(const char* local_dir, int user_writable, boolean setgid_on_dir) {
  //This is the same as
  //> chmod g+rwX -R $local_dir
  //> chmod g+s -R $local_dir
  //> if [ $user_writable ]; then chmod u+w;  else u-w; fi
  //> chown -no-dereference -R $user:$supervisor-group $local_dir
  int exit_code = 0;
  uid_t euser = geteuid();

  if (local_dir == NULL) {
    fprintf(ERRORFILE, "Path is null\n");
    exit_code = UNABLE_TO_BUILD_PATH; // may be malloc failed
  } else {
    char *(paths[]) = {strndup(local_dir,PATH_MAX), 0};
    if (paths[0] == NULL) {
      fprintf(ERRORFILE, "Malloc failed in setup_dir_permissions\n");
      return -1;
    }
    // check to make sure the directory exists
    if (access(local_dir, F_OK) != 0) {
      if (errno == ENOENT) {
        fprintf(ERRORFILE, "Path does not exist %s\n", local_dir);
        free(paths[0]);
        paths[0] = NULL;
        return UNABLE_TO_BUILD_PATH;
      }
    }
    FTS* tree = fts_open(paths, FTS_PHYSICAL | FTS_XDEV, NULL);
    FTSENT* entry = NULL;
    int ret = 0;

    if (tree == NULL) {
      fprintf(ERRORFILE,
              "Cannot open file traversal structure for the path %s:%s.\n", 
              local_dir, strerror(errno));
      free(paths[0]);
      paths[0] = NULL;
      return -1;
    }

    if (seteuid(0) != 0) {
      fprintf(ERRORFILE, "Could not become root\n");
      return -1;
    }

    while (((entry = fts_read(tree)) != NULL) && exit_code == 0) {
      switch (entry->fts_info) {

      case FTS_DP:        // A directory being visited in post-order
      case FTS_DOT:       // A dot directory
      case FTS_SL:        // A symbolic link
      case FTS_SLNONE:    // A broken symbolic link
        //NOOP
        fprintf(LOGFILE, "NOOP: %s\n", entry->fts_path); break;
      case FTS_D:         // A directory in pre-order
      case FTS_F:         // A regular file
        if (setup_permissions(entry, euser, user_writable, setgid_on_dir) != 0) {
            exit_code = -1;
        }
        break;
      case FTS_DEFAULT:   // Unknown type of file
      case FTS_DNR:       // Unreadable directory
      case FTS_NS:        // A file with no stat(2) information
      case FTS_DC:        // A directory that causes a cycle
      case FTS_NSOK:      // No stat information requested
      case FTS_ERR:       // Error return
      default:
        fprintf(LOGFILE, "Unexpected...\n");
        exit_code = -1;
        break;
      }
    }
    ret = fts_close(tree);
    if (exit_code == 0 && ret != 0) {
      fprintf(LOGFILE, "Error in fts_close while setting up %s\n", local_dir);
      exit_code = -1;
    }
    free(paths[0]);
    paths[0] = NULL;
  }
  return exit_code;
}


int signal_container_as_user(const char *user, int pid, int sig) {
  if(pid <= 0) {
    return INVALID_CONTAINER_PID;
  }

  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return SETUID_OPER_FAILED;
  }

  //Don't continue if the process-group is not alive anymore.
  int has_group = 1;
  if (kill(-pid,0) < 0) {
    if (kill(pid, 0) < 0) {
      if (errno == ESRCH) {
        return INVALID_CONTAINER_PID;
      }
      fprintf(LOGFILE, "Error signalling container %d with %d - %s\n",
	      pid, sig, strerror(errno));
      return -1;
    } else {
      has_group = 0;
    }
  }

  if (kill((has_group ? -1 : 1) * pid, sig) < 0) {
    if(errno != ESRCH) {
      fprintf(LOGFILE, 
              "Error signalling process group %d with signal %d - %s\n", 
              -pid, sig, strerror(errno));
      fprintf(stderr, 
              "Error signalling process group %d with signal %d - %s\n", 
              -pid, sig, strerror(errno));
      fflush(LOGFILE);
      return UNABLE_TO_SIGNAL_CONTAINER;
    } else {
      return INVALID_CONTAINER_PID;
    }
  }
  fprintf(LOGFILE, "Killing process %s%d with %d\n",
	  (has_group ? "group " :""), pid, sig);
  return 0;
}

/**
 * Delete a final directory as the worker-launcher user.
 */
static int rmdir_as_wl(const char* path) {
  int user_uid = geteuid();
  int user_gid = getegid();
  int ret = change_effective_user(launcher_uid, launcher_gid);
  if (ret == 0) {
    if (rmdir(path) != 0) {
      fprintf(LOGFILE, "rmdir of %s failed - %s\n", path, strerror(errno));
      ret = -1;
    }
  }
  // always change back
  if (change_effective_user(user_uid, user_gid) != 0) {
    ret = -1;
  }
  return ret;
}

static int remove_files_from_dir(const char *path) {
  DIR *dir;
  struct dirent *entry;
  dir = opendir(path);
  if (dir) {
    int exit_code = 0;
    while ((entry = readdir(dir)) != NULL) {
      if(strcmp(entry->d_name, ".") == 0
         || strcmp(entry->d_name, "..") == 0) {
          continue;
      }
      char *newpath;
      int bytesPrinted = asprintf(&newpath, "%s/%s", path, entry->d_name);
      if (bytesPrinted == -1) {
        //asprintf failed, stop the process
        exit(EXIT_FAILURE);
      }
      if(newpath) {
        // Recur on anything in the directory.
        int new_exit = recursive_delete(newpath, 0);
        if(!exit_code) {
          exit_code = new_exit;
        }
      }
      free(newpath);
    }
    closedir(dir);
    return exit_code;
  }
  return -1;
}

int recursive_delete(const char *path, int supervisor_owns_dir) {
  if(path == NULL) {
    fprintf(LOGFILE, "Cannot delete NULL path.\n");
    return UNABLE_TO_BUILD_PATH;
  }

  struct stat file_stat;

  if(access(path, F_OK) != 0) {
    if(errno == ENOENT) {
       if(lstat(path, &file_stat) != 0) {
         fprintf(LOGFILE, "Failed to stat %s: %s", path, strerror(errno));
         return 0;
       }
       // we need to handle symlinks that target missing files.
       if((file_stat.st_mode & S_IFMT) != S_IFLNK) {
         return 0;
       }
     }
   }

  if(lstat(path, &file_stat) != 0) {
    fprintf(LOGFILE, "Failed to delete %s: %s", path, strerror(errno));
    return UNABLE_TO_STAT_FILE;
  }

  switch(file_stat.st_mode & S_IFMT) {
  case S_IFDIR:
    // Make sure we can read the directory.
    if((file_stat.st_mode & 0200) == 0) {
      fprintf(LOGFILE, "Unreadable directory %s, chmoding.\n", path);
      if (chmod(path, 0700) != 0) {
        fprintf(LOGFILE, "Error chmoding %s - %s, continuing\n", path, strerror(errno));
      }
    }

    // Delete all the subfiles.
    remove_files_from_dir(path);

    // Delete the actual directory.
    if (supervisor_owns_dir) {
      return rmdir_as_wl(path);
    }
    else if (rmdir(path) != 0) {
      fprintf(LOGFILE, "Couldn't delete directory %s - %s\n", path, strerror(errno));
      return -1;
    }
    break;

  case S_IFLNK:
    // If it's a link, just unlink it. Don't follow.
    if(unlink(path) != 0) {
      fprintf(LOGFILE, "Couldn't delete symlink %s - %s\n", path, strerror(errno));
      return -1;
    }
    break;

  default: // Just rm all other kinds of files.
    remove(path);
  }
  return 0;
}

int exec_as_user(const char * working_dir, const char * script_file) {
  char *script_file_dest = NULL;
  script_file_dest = get_container_launcher_file(working_dir);
  if (script_file_dest == NULL) {
    return OUT_OF_MEMORY;
  }

  // open launch script
  int script_file_source = open_file_as_wl(script_file);
  if (script_file_source == -1) {
    return -1;
  }

  setsid();

  // give up root privs
  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return SETUID_OPER_FAILED;
  }

  if (copy_file(script_file_source, script_file, script_file_dest, S_IRWXU) != 0) {
    return -1;
  }

  fcloseall();
  umask(0027);
  if (chdir(working_dir) != 0) {
    fprintf(LOGFILE, "Can't change directory to %s -%s\n", working_dir,
	    strerror(errno));
    return -1;
  }

  if (execlp(script_file_dest, script_file_dest, NULL) != 0) {
    fprintf(LOGFILE, "Couldn't execute the container launch file %s - %s", 
            script_file_dest, strerror(errno));
    return UNABLE_TO_EXECUTE_CONTAINER_SCRIPT;
  }
 
  //Unreachable
  return -1;
}

int fork_as_user(const char * working_dir, const char * script_file) {
  char *script_file_dest = NULL;
  script_file_dest = get_container_launcher_file(working_dir);
  if (script_file_dest == NULL) {
    return OUT_OF_MEMORY;
  }

  // open launch script
  int script_file_source = open_file_as_wl(script_file);
  if (script_file_source == -1) {
    return -1;
  }

  setsid();

  // give up root privs
  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return SETUID_OPER_FAILED;
  }

  if (copy_file(script_file_source, script_file, script_file_dest, S_IRWXU) != 0) {
    return -1;
  }

  fcloseall();
  umask(0027);
  if (chdir(working_dir) != 0) {
    fprintf(LOGFILE, "Can't change directory to %s -%s\n", working_dir,
	    strerror(errno));
    return -1;
  }

  int pid = fork();
  if (pid == 0 && execlp(script_file_dest, script_file_dest, NULL) != 0) {
    fprintf(LOGFILE, "Couldn't execute the container launch file %s - %s",
            script_file_dest, strerror(errno));
    return UNABLE_TO_EXECUTE_CONTAINER_SCRIPT;
  } else {
    fprintf(LOGFILE, "Launched the process from the container launch file %s - with pid %d",
            script_file_dest, pid);
    return 0;
  }

  //Unreachable
  return -1;
}

//functions below are docker related.

char *get_docker_binary() {
  char *docker_binary = get_value(DOCKER_BINARY_KEY);
  if (docker_binary == NULL) {
    docker_binary = strdup(DEFAULT_DOCKER_BINARY_PATH);
  }
  return docker_binary;
}

char** tokenize_docker_command(const char *input, int *split_counter) {
  char *line = (char *)calloc(strlen(input) + 1, sizeof(char));
  char **linesplit = (char **) malloc(sizeof(char *));
  char *p = NULL;
  *split_counter = 0;
  strncpy(line, input, strlen(input));

  p = strtok(line, " ");
  while(p != NULL) {
    linesplit[*split_counter] = p;
    (*split_counter)++;
    linesplit = realloc(linesplit, (sizeof(char *) * (*split_counter + 1)));
    if(linesplit == NULL) {
      fprintf(ERRORFILE, "Cannot allocate memory to parse docker command %s",
                 strerror(errno));
      fflush(ERRORFILE);
      exit(OUT_OF_MEMORY);
    }
    p = strtok(NULL, " ");
  }
  linesplit[*split_counter] = NULL;
  return linesplit;
}

int execute_regex_match(const char *regex_str, const char *input) {
  regex_t regex;
  int regex_match;
  if (0 != regcomp(&regex, regex_str, REG_EXTENDED|REG_NOSUB)) {
    fprintf(LOGFILE, "Unable to compile regex.");
    fflush(LOGFILE);
    exit(ERROR_COMPILING_REGEX);
  }
  regex_match = regexec(&regex, input, (size_t) 0, NULL, 0);
  regfree(&regex);
  if(0 == regex_match) {
    return 0;
  }
  return 1;
}

int validate_docker_image_name(const char *image_name) {
  char *regex_str = "^(([a-zA-Z0-9.-]+)(:[0-9]+)?/)?([a-z0-9_./-]+)(:[a-zA-Z0-9_.-]+)?$";
  return execute_regex_match(regex_str, image_name);
}

/**
 * Only allow certain options for any docker commands.
 * Since most options are from docker-run command, we don't have
 * separate checks for other docker-xx (e.g docker-inspect) commands.
 */
char* sanitize_docker_command(const char *line) {
  static struct option long_options[] = {
    {"name", required_argument, 0, 'n' },
    {"user", required_argument, 0, 'u' },
    {"rm", no_argument, 0, 'r' },
    {"workdir", required_argument, 0, 'w' },
    {"net", required_argument, 0, 'e' },
    {"cgroup-parent", required_argument, 0, 'g' },
    {"cap-add", required_argument, 0, 'a' },
    {"cap-drop", required_argument, 0, 'o' },
    {"device", required_argument, 0, 'i' },
    {"detach", required_argument, 0, 't' },
    {"group-add", required_argument, 0, 'x' },
    {"read-only", no_argument, 0, 'R' },
    {"security-opt", required_argument, 0, 'S' },
    {"cpu-shares", required_argument, 0, 'c' },
    {"cpus", required_argument, 0, 'C' },
    {"cidfile", required_argument, 0, 'I' },
    {"format", required_argument, 0, 'f' }, //belongs to docker-inspect command
    {"force", no_argument, 0, 'F' }, //belongs to docker-rm command
    {"time", required_argument, 0, 'T' }, //belongs to docker-stop command
    {0, 0, 0, 0}
  };

  int c = 0;
  int option_index = 0;
  char *output = NULL;
  size_t output_size = 0;
  char **linesplit;
  int split_counter = 0;
  int len = strlen(line);

  linesplit = tokenize_docker_command(line, &split_counter);

  output_size = len * 2;
  output = (char *) calloc(output_size, sizeof(char));
  if(output == NULL) {
    exit(OUT_OF_MEMORY);
  }

  // Handle docker client config option.
  if(0 == strncmp(linesplit[0], DOCKER_CLIENT_CONFIG_ARG, strlen(DOCKER_CLIENT_CONFIG_ARG))) {
    strcat(output, linesplit[0]);
    strcat(output, " ");
    long index = 0;
    while(index < split_counter) {
      linesplit[index] = linesplit[index + 1];
      if (linesplit[index] == NULL) {
        split_counter--;
        break;
      }
      index++;
    }
  }

  // Handle docker pull and image name validation.
  if (0 == strncmp(linesplit[0], DOCKER_PULL_COMMAND, strlen(DOCKER_PULL_COMMAND))) {
    if (0 != validate_docker_image_name(linesplit[1])) {
      fprintf(ERRORFILE, "Invalid Docker image name, exiting.");
      fflush(ERRORFILE);
      exit(DOCKER_IMAGE_INVALID);
    }
    strcat(output, linesplit[0]);
    strcat(output, " ");
    strcat(output, linesplit[1]);
    return output;
  }

  strcat(output, linesplit[0]);
  strcat(output, " ");
  optind = 1;
  while((c=getopt_long(split_counter, linesplit, "dv:", long_options, &option_index)) != -1) {
    switch(c) {
      case 'n':
        strcat(output, "--name=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'w':
        strcat(output, "--workdir=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'u':
        strcat(output, "--user=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'e':
        strcat(output, "--net=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'v':
        strcat(output, "-v ");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'a':
        strcat(output, "--cap-add=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'o':
        strcat(output, "--cap-drop=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'd':
        strcat(output, "-d ");
        break;
      case 'r':
        strcat(output, "--rm ");
        break;
      case 'R':
        strcat(output, "--read-only ");
        break;
      case 'S':
        strcat(output, "--security-opt ");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'g':
        strcat(output, "--cgroup-parent=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'c':
        strcat(output, "--cpu-shares=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'C':
        strcat(output, "--cpus=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'i':
        strcat(output, "--device=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 't':
        strcat(output, "--detach=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'x':
        strcat(output, "--group-add ");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'I':
        strcat(output, "--cidfile=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'f':
        strcat(output, "--format=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      case 'F':
        strcat(output, "--force ");
        break;
      case 'T':
        strcat(output, "--time=");
        strcat(output, optarg);
        strcat(output, " ");
        break;
      default:
        fprintf(LOGFILE, "Unknown option in docker command, character %d %c, optionindex = %d\n", c, c, optind);
        fflush(LOGFILE);
        return NULL;
        break;
    }
  }

  while(optind < split_counter) {
    strcat(output, linesplit[optind++]);
    strcat(output, " ");
  }

  return output;
}

char* parse_docker_command_file(const char* command_file) {

  size_t len = 0;
  char *line = NULL;
  ssize_t read;
  FILE *stream;

  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(launcher_uid, launcher_gid) != 0) {
   fprintf(ERRORFILE, "Cannot change effective user to supervisor user");
   fflush(ERRORFILE);
   exit(ERROR_CHANGING_USER);
  }

  stream = fopen(command_file, "r");
  if (stream == NULL) {
   fprintf(ERRORFILE, "Cannot open file %s - %s",
                 command_file, strerror(errno));
   fflush(ERRORFILE);
   exit(ERROR_OPENING_FILE);
  }
  if ((read = getline(&line, &len, stream)) == -1) {
     fprintf(ERRORFILE, "Error reading command_file %s\n", command_file);
     fflush(ERRORFILE);
     exit(ERROR_READING_FILE);
  }
  fclose(stream);
  if (change_effective_user(user, group)) {
   fprintf(ERRORFILE, "Cannot change effective user from supervisor user back to original");
   fflush(ERRORFILE);
   exit(ERROR_CHANGING_USER);
  }

  char* ret = sanitize_docker_command(line);
  if(ret == NULL) {
    exit(ERROR_SANITIZING_DOCKER_COMMAND);
  }

  return ret;
}

int run_docker_cmd(const char * working_dir, const char * command_file) {
  char* docker_command = parse_docker_command_file(command_file);
  char* docker_binary = get_docker_binary();
  size_t command_size = MIN(sysconf(_SC_ARG_MAX), 128*1024);

  char* docker_command_with_binary = calloc(sizeof(char), command_size);
  snprintf(docker_command_with_binary, command_size, "%s %s", docker_binary, docker_command);

  fprintf(LOGFILE, "Using command: %s\n", docker_command_with_binary);
  fflush(LOGFILE);

  setsid();

  char **args = extract_values_delim(docker_command_with_binary, " ");

  //debug
  int i = 0;
  for (i = 0; args[i] != NULL; i++) {
    fprintf(LOGFILE, "%s\n", args[i]);
    fflush(LOGFILE);
  }

  if (execvp(docker_binary, args) != 0) {
    fprintf(ERRORFILE, "Couldn't execute the container launch with args %s - %s",
              docker_binary, strerror(errno));
    fflush(LOGFILE);
    fflush(ERRORFILE);
    free(docker_binary);
    free(args);
    free(docker_command_with_binary);
    free(docker_command);
    return DOCKER_RUN_FAILED;
  }
  //Unreachable
  return -1;
}

//functions below are nsenter related.
//Used for running profiling inside docker container through nsenter.

char *get_nsenter_binary() {
  char *nsenter_binary = get_value(NSENTER_BINARY_KEY);
  if (nsenter_binary == NULL) {
    nsenter_binary = strdup(DEFAULT_NSENTER_BINARY_PATH);
  }
  return nsenter_binary;
}

int run_nsenter(const char * user, const char * worker_id, const char * working_dir, const char * command_file) {

  size_t command_size = MIN(sysconf(_SC_ARG_MAX), 128*1024);

  //get pid
  char *docker_inspect_command = calloc(sizeof(char), command_size);
  char* docker_binary = get_docker_binary();
  snprintf(docker_inspect_command, command_size, "%s inspect --format {{.State.Pid}} %s", docker_binary, worker_id);

  fprintf(LOGFILE, "Inspecting docker container...\n");
  fflush(LOGFILE);
  FILE* inspect_docker = popen(docker_inspect_command, "r");
  int pid = 0;
  int res = fscanf (inspect_docker, "%d", &pid);
  if (pclose (inspect_docker) != 0 || res <= 0)
  {
    fprintf (ERRORFILE,
     "Could not inspect docker to get pid %s.\n", docker_inspect_command);
    fflush(ERRORFILE);
    free(docker_inspect_command);
    free(docker_binary);
    return UNABLE_TO_EXECUTE_CONTAINER_SCRIPT;
  }

  fprintf(LOGFILE, "The pid is %d.\n", pid);
  fflush(LOGFILE);

  if (pid != 0) {

    size_t len = 0;
    char *line = NULL;
    ssize_t read;
    FILE *stream  = fopen(command_file, "r");
    if (stream == NULL) {
     fprintf(ERRORFILE, "Cannot open file %s - %s",
                   command_file, strerror(errno));
     fflush(ERRORFILE);
     exit(ERROR_OPENING_FILE);
    }
    if ((read = getline(&line, &len, stream)) == -1) {
       fprintf(ERRORFILE, "Error reading command_file %s\n", command_file);
       fflush(ERRORFILE);
       exit(ERROR_READING_FILE);
    }
    fclose(stream);

    //run profiling command
    char* nsenter_binary = get_nsenter_binary();
    char *nsenter_command_with_binary = calloc(sizeof(char), command_size);
    snprintf(nsenter_command_with_binary, command_size, "%s --target %d --mount --pid", nsenter_binary, pid);

    fprintf (LOGFILE, "command is %s\n", nsenter_command_with_binary);
    fflush(LOGFILE);

    if (seteuid(0) != 0) {
      fprintf(ERRORFILE, "Could not become root\n");
      fflush(LOGFILE);
      return -1;
    }

    FILE *fp = popen(nsenter_command_with_binary, "w");
    //TODO need to add some sanity checks for the command for security
    fprintf(fp, "sudo -u %s %s\nexit\n", user, line);
    pclose(fp);

    free(docker_inspect_command);
    free(docker_binary);
    free(nsenter_binary);
    free(nsenter_command_with_binary);
    return 0;

  } else {
     fprintf(ERRORFILE, "docker inspect failed. Got pid=0");
     fflush(ERRORFILE);
  }
  return UNABLE_TO_EXECUTE_CONTAINER_SCRIPT;
}