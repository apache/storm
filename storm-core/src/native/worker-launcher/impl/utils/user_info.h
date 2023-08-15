/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#ifndef USER_INFO_DOT_H
#define USER_INFO_DOT_H

#include <pwd.h> /* for struct passwd */
#include <unistd.h> /* for size_t */

struct user_info {
  size_t buf_sz;
  struct passwd pwd;
  char *buf;
  gid_t *gids;
  int num_gids;
  int gids_size;
};

/**
 * Allocate a user info context.
 *
 * @return                        NULL on OOM; the context otherwise.
 */
struct user_info *user_info_alloc(void);

/**
 * Free a user info context.
 *
 * @param uinfo                   The user info context to free.
 */
void user_info_free(struct user_info *uinfo);

/**
 * Look up information for a user name.
 *
 * @param uinfo                   The user info context.
 *                                Existing data in this context will be cleared.
 * @param username                The user name to look up.
 *
 * @return                        ENOENT if the user wasn't found;
 *                                0 on success;
 *                                EIO, EMFILE, ENFILE, or ENOMEM if appropriate.
 */
int user_info_fetch(struct user_info *uinfo,
                           const char *username);

/**
 * Look up the groups this user belongs to. 
 *
 * @param uinfo                   The user info context.
 *                                uinfo->gids will be filled in on a successful
 *                                return;
 *
 * @return                        0 on success.
 *                                ENOMEM if we ran out of memory.
 *                                EINVAL if the uinfo was invalid.
 */
int user_info_getgroups(struct user_info *uinfo);

#endif
