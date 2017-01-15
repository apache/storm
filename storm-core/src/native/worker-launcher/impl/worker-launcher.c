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
#include <sys/stat.h>

static const int DEFAULT_MIN_USERID = 1000;

static const char* DEFAULT_BANNED_USERS[] = {"bin", 0};

//struct to store the user details
struct passwd *user_detail = NULL;

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
    fprintf(LOGFILE, "The configured nodemanager group %d is different from"
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
  asprintf(&ret, "%s/%s", work_dir, CONTAINER_SCRIPT);
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
 * Open a file as the node manager and return a file descriptor for it.
 * Returns -1 on error
 */
static int open_file_as_nm(const char* filename) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(launcher_uid, launcher_gid) != 0) {
    return -1;
  }
  int result = open(filename, O_RDONLY);
  if (result == -1) {
    fprintf(LOGFILE, "Can't open file %s as node manager - %s\n", filename,
	    strerror(errno));
  }
  if (change_effective_user(user, group)) {
    result = -1;
  }
  return result;
}

/**
 * Copy a file from a fd to a given filename.
 * The new file must not exist and it is created with permissions perm.
 * The input stream is closed.
 * Return 0 if everything is ok.
 */
static int copy_file(int input, const char* in_filename, 
		     const char* out_filename, mode_t perm) {
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
 */
static int setup_permissions(FTSENT* entry, uid_t euser, int user_write) {
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
    new_mode = new_mode | S_IXGRP | S_ISGID;
  }
  if (chmod(entry->fts_path, new_mode) != 0) {
    fprintf(ERRORFILE, "Failure to exec app initialization process - %s, fts_path=%s\n",
            strerror(errno), entry->fts_path);
    return -1;
  }
  return 0;
}


int setup_dir_permissions(const char* local_dir, int user_writable) {
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
        if (setup_permissions(entry, euser, user_writable) != 0) {
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
 * Delete a final directory as the node manager user.
 */
static int rmdir_as_nm(const char* path) {
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
      asprintf(&newpath, "%s/%s", path, entry->d_name);
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

  if(access(path, F_OK) != 0) {
    if(errno == ENOENT) {
      return 0;
    }
    // Can probably return here, but we'll try to lstat anyway.
  }

  struct stat file_stat;
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
      return rmdir_as_nm(path);
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
  int script_file_source = open_file_as_nm(script_file);
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
  int script_file_source = open_file_as_nm(script_file);
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
