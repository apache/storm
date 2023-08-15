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

#include <stdarg.h>
#include <strings.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "string-utils.h"

/* Returns the corresponding hexadecimal character for a nibble. */
static char nibble_to_hex(unsigned char nib) {
  return nib < 10 ? '0' + nib : 'a' + nib - 10;
}

/**
 * Converts a sequence of bytes into a hexadecimal string.
 *
 * Returns a pointer to the allocated string on success or NULL on error.
 */
char* to_hexstring(unsigned char* bytes, unsigned int len) {
  char* hexstr = malloc(len * 2 + 1);
  if (hexstr == NULL) {
    return NULL;
  }
  unsigned char* src = bytes;
  char* dest = hexstr;
  unsigned int i;
  for (i = 0; i < len; ++i) {
    unsigned char val = *src++;
    *dest++ = nibble_to_hex((val >> 4) & 0xF);
    *dest++ = nibble_to_hex(val & 0xF);
  }
  *dest = '\0';
  return hexstr;
}

/**
 * Initialize an uninitialized strbuf with the specified initial capacity.
 *
 * Returns true on success or false if memory could not be allocated.
 */
bool strbuf_init(strbuf* sb, size_t initial_capacity) {
  memset(sb, 0, sizeof(*sb));
  char* new_buffer = malloc(initial_capacity);
  if (new_buffer == NULL) {
    return false;
  }
  sb->buffer = new_buffer;
  sb->capacity = initial_capacity;
  sb->length = 0;
  return true;
}

/**
 * Allocate and initialize a strbuf with the specified initial capacity.
 *
 * Returns a pointer to the allocated and initialized strbuf or NULL on error.
 */
strbuf* strbuf_alloc(size_t initial_capacity) {
  strbuf* sb = malloc(sizeof(*sb));
  if (sb != NULL) {
    if (!strbuf_init(sb, initial_capacity)) {
      free(sb);
      sb = NULL;
    }
  }
  return sb;
}

/**
 * Detach the underlying character buffer from a string buffer.
 *
 * Returns the heap-allocated, NULL-terminated character buffer.
 * NOTE: The caller is responsible for freeing the result.
 */
char* strbuf_detach_buffer(strbuf* sb) {
  char* result = NULL;
  if (sb != NULL) {
    result = sb->buffer;
    sb->buffer = NULL;
    sb->length = 0;
    sb->capacity = 0;
  }
  return result;
}

/**
 * Release memory associated with a strbuf but not the strbuf structure itself.
 * Useful for stack-allocated strbuf objects or structures that embed a strbuf.
 * Use strbuf_free for heap-allocated string buffers.
 */
void strbuf_destroy(strbuf* sb) {
  if (sb != NULL) {
    free(sb->buffer);
    sb->buffer = NULL;
    sb->capacity = 0;
    sb->length = 0;
  }
}

/**
 * Free a strbuf and all memory associated with it.
 */
void strbuf_free(strbuf* sb) {
  if (sb != NULL) {
    strbuf_destroy(sb);
    free(sb);
  }
}

/**
 * Resize a strbuf to the specified new capacity.
 *
 * Returns true on success or false if there was an error.
 */
bool strbuf_realloc(strbuf* sb, size_t new_capacity) {
  if (new_capacity < sb->length + 1) {
    // New capacity would result in a truncation of the existing string.
    return false;
  }

  char* new_buffer = realloc(sb->buffer, new_capacity);
  if (!new_buffer) {
    return false;
  }

  sb->buffer = new_buffer;
  sb->capacity = new_capacity;
  return true;
}

/**
 * Append a formatted string to the current contents of a strbuf.
 *
 * Returns true on success or false if there was an error.
 */
bool strbuf_append_fmt(strbuf* sb, size_t realloc_extra,
    const char* fmt, ...) {
  if (sb->length > sb->capacity) {
    return false;
  }

  if (sb->length == sb->capacity) {
    size_t incr = (realloc_extra == 0) ? 1024 : realloc_extra;
    if (!strbuf_realloc(sb, sb->capacity + incr)) {
      return false;
    }
  }

  size_t remain = sb->capacity - sb->length;
  va_list vargs;
  va_start(vargs, fmt);
  int needed = vsnprintf(sb->buffer + sb->length, remain, fmt, vargs);
  va_end(vargs);
  if (needed == -1) {
    return false;
  }

  needed += 1;  // vsnprintf result does NOT include terminating NUL
  if (needed > remain) {
    // result was truncated so need to realloc and reprint
    size_t new_size = sb->length + needed + realloc_extra;
    if (!strbuf_realloc(sb, new_size)) {
      return false;
    }
    remain = sb->capacity - sb->length;
    va_start(vargs, fmt);
    needed = vsnprintf(sb->buffer + sb->length, remain, fmt, vargs);
    va_end(vargs);
    if (needed == -1) {
      return false;
    }
  }

  sb->length += needed - 1;  // length does not include terminating NUL
  return true;
}
