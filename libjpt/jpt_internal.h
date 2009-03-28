/* Internal header for the jpt library.
   Copyright (C) 2006 Morten Hustveit <morten@rashbox.org>

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public
   License as publiched by the Free Software Foundation; either
   version 3 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTIBILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

#ifndef JPT_INTERNAL_H_
#define JPT_INTERNAL_H_ 1

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/uio.h>

#include "jpt.h"

#define JPT_COL_CREATE  0x0001
#define JPT_COL_NOSAVE  0x0002

#define COLUMN_PREFIX_SIZE 4

#define CELLMETA_TO_COLUMN(cellmeta) ((cellmeta[3] - 1) + (cellmeta[2] - 1) * 255 + (cellmeta[1] - 1) * 65025 + (cellmeta[0] - 1) * 16581375)

#define JPT_DISKTABLE_READ_KEYINFO(disktable, target, keyidx) \
  (disktable->key_infos_mapped ? (memcpy(target, disktable->key_infos + keyidx, sizeof(struct JPT_key_info)), 0) \
                               : JPT_disktable_read_keyinfo(disktable, target, keyidx))

#define JPT_DISKTABLE_WRITE_KEYINFO(disktable, source, keyidx) \
  (disktable->key_infos_mapped ? (memcpy(disktable->key_infos + keyidx, source, sizeof(struct JPT_key_info)), 0) \
                               : JPT_disktable_write_keyinfo(disktable, source, keyidx))

#define JPT_OPERATOR_INSERT         0x0001
#define JPT_OPERATOR_REMOVE         0x0002
#define JPT_OPERATOR_CREATE_COLUMN  0x0003
#define JPT_OPERATOR_REMOVE_COLUMN  0x0004

#define JPT_KEY_REMOVED             0x0001
#define JPT_KEY_NEW_COLUMN          0x0002

extern __thread int JPT_errno;
extern __thread char* JPT_last_error;

struct JPT_node_data
{
  struct JPT_node_data* next;
  void* value;
  size_t value_size;
};

struct JPT_node
{
  uint64_t timestamp;

  char* row;
  uint32_t columnidx;

  struct JPT_node* parent;
  struct JPT_node* left;
  struct JPT_node* right;
  struct JPT_node_data* last;

  struct JPT_node_data data;
};

struct JPT_column
{
  char* name;
  uint32_t index;
};

struct JPT_key_info
{
  uint64_t timestamp;
  uint64_t offset;
  uint32_t size;
  uint32_t flags;
} __attribute__((packed));

struct JPT_info
{
  int flags;

  char* filename;
  int fd;

  int logfd;
  int logfile_empty;
  unsigned char logbuf[256]; /* For log entry headers */
  size_t logbuf_fill;
  int replaying; /* To avoid logging while replaying */

  char* map;
  off_t map_size;
  off_t file_size;

  uint32_t next_column;
  struct JPT_column* columns;
  size_t column_count;

  char* buffer;
  size_t buffer_size;
  size_t buffer_util;

  struct JPT_node* root;
  size_t node_count;
  size_t memtable_key_count;
  size_t memtable_key_size;
  size_t memtable_value_size;

  struct JPT_disktable* first_disktable;
  struct JPT_disktable* last_disktable;
  size_t disktable_count;

#if GLOBAL_LOCKS
  pthread_mutex_t global_lock;
#else
  pthread_rwlock_t rw_lock;
  pthread_rwlock_t splay_lock;
#endif

  pthread_mutex_t column_hash_mutex;

  size_t major_compact_count;
};

struct JPT_disktable
{
  off_t pat_offset;
  struct patricia* pat;
  int pat_mapped;

  off_t key_info_offset;
  struct JPT_key_info* key_infos;
  size_t key_info_count;
  int key_infos_mapped;

  struct JPT_info* info;
  off_t offset;

  uint8_t bloom_filter[4][8192];

  struct JPT_disktable* next;
};

struct JPT_disktable_cursor
{
  uint64_t timestamp;
  struct JPT_disktable* disktable;
  void* buffer;
  void* data;
  size_t data_size;
  size_t data_alloc;
  off_t data_offset;
  size_t keylen;
  off_t offset;
  uint32_t columnidx;
  uint32_t flags;
};

struct JPT_key_info_callback_args
{
  struct JPT_key_info* row_names;
  struct JPT_info* info;
};

void
JPT_memtable_splay(struct JPT_info* info, struct JPT_node* n);

int
JPT_memtable_has_key(struct JPT_info* info, const char* row, uint32_t columnidx);

int
JPT_memtable_insert(struct JPT_info* info, const char* row, uint32_t columnidx,
                    const void* value, size_t value_size, uint64_t* timestamp,
                    int flags);

int
JPT_memtable_get(struct JPT_info* info, const char* row, uint32_t columnidx,
                 void** value, size_t* value_size, size_t* skip, size_t* max_read,
                 uint64_t* timestamp);

void
JPT_memtable_list_all(struct JPT_info* info, struct JPT_node*** nodes);

void
JPT_memtable_list_column(struct JPT_info* info, struct JPT_node*** nodes, uint32_t columnidx);

int
JPT_memtable_remove(struct JPT_info* info, const char* row, uint32_t columnidx);

int
JPT_disktable_read_keyinfo(struct JPT_disktable* disktable, struct JPT_key_info* target, size_t keyidx);

int
JPT_disktable_read(struct JPT_disktable* disktable, void* target, size_t size, size_t offset);

int
JPT_disktable_has_key(struct JPT_disktable* disktable,
                      const char* row, uint32_t columnidx);

int
JPT_disktable_remove(struct JPT_disktable* disktable,
                     const char* row, uint32_t columnidx);

ssize_t
JPT_disktable_overwrite(struct JPT_disktable* disktable,
                        const char* row, uint32_t columnidx,
                        const void* data, size_t amount);

int
JPT_disktable_get(struct JPT_disktable* disktable,
                  const char* row, uint32_t columnidx,
                  void** value, size_t* value_size, size_t* skip, size_t* max_read,
                  uint64_t* timestamp);

int
JPT_disktable_cursor_advance(struct JPT_info* info,
                             struct JPT_disktable_cursor* cursor,
                             size_t columnidx);

int
JPT_disktable_cursor_remap(struct JPT_info* info,
                           struct JPT_disktable_cursor* cursor);

int
JPT_compact(struct JPT_info* info);

int
JPT_get_fixed(struct JPT_info* info, const char* row, const char* column,
              void* value, size_t value_size);

void
JPT_generate_key(char* target, const char* row, uint32_t columnidx);

ssize_t
JPT_read_all(int fd, void* target, size_t size);

ssize_t
JPT_write_all(int fd, const void* target, size_t size);

void*
JPT_fs_malloc(size_t size);

void
JPT_fs_free(void* ptr);

void
JPT_clear_error();

void
JPT_set_error(char* error, int err);

/* jpt_io.c */

int
JPT_write_uint(FILE* f, unsigned int integer);

int
JPT_write_uint64(FILE* f, uint64_t val);

uint64_t
JPT_read_uint(FILE* f);

uint64_t
JPT_read_uint64(FILE* f);

off_t
JPT_lseek(int fd, off_t offset, int whence, off_t filesize);

ssize_t
JPT_writev(int fd, const struct iovec *iov, int iovcnt);

#endif /* !JPT_INTERNAL_H_ */
