/*  General jpt functions.
    Copyright (C) 2007-2008  Morten Hustveit <morten@rashbox.org>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <setjmp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/time.h>

#include "pthread.h"
#include "patricia.h"
#include "jpt.h"

#include "jpt_internal.h"

#define JPT_PARTIAL_WRITE "LBA_"
#define JPT_SIGNATURE     "LBAT"
#define JPT_VERSION       8

/* #define TRACE(x) fprintf x ; fflush(stderr); */

#ifndef TRACE
#define TRACE(x)
#endif

static int
JPT_log_reset(struct JPT_info* info);

static int
JPT_log_replay(struct JPT_info* info);

static int
JPT_log_begin(struct JPT_info* info);

static int
JPT_get(struct JPT_info* info, const char* row, const char* column,
        void** value, size_t* value_size, size_t* skip, size_t* max_read,
        uint64_t* timestamp);

static int
JPT_insert(struct JPT_info* info,
           const char* row, const char* column,
           const void* value, size_t value_size,
           uint64_t* timestamp, int flags);

static int
JPT_remove_column(struct JPT_info* info, const char* column, int flags);

uint64_t
jpt_gettime()
{
  struct timeval tv;

  gettimeofday(&tv, 0);

  return tv.tv_usec + tv.tv_sec * 1000000ULL;
}

#define JPT_ESHORTREAD 1
#define JPT_EVERSION   2

static __thread int JPT_errno = 0;
static __thread char* JPT_last_error = 0;

#define GLOBAL_LOCKS 0

#if GLOBAL_LOCKS
static pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;
#endif

static void
JPT_reader_enter(struct JPT_info* info)
{
#if GLOBAL_LOCKS
  pthread_mutex_lock(&global_lock);
  assert(!info->reader_count && !info->is_writing);
  info->reader_count = 1;
#else
  pthread_mutex_lock(&info->reader_count_mutex);

  while(info->is_writing)
    pthread_cond_wait(&info->read_ready, &info->reader_count_mutex);
  ++info->reader_count;

  pthread_mutex_unlock(&info->reader_count_mutex);

  assert(!info->is_writing);
#endif
}

static void
JPT_reader_leave(struct JPT_info* info)
{
#if GLOBAL_LOCKS
  info->reader_count = 0;
  pthread_mutex_unlock(&global_lock);
#else
  assert(!info->is_writing);
  assert(info->reader_count);

  pthread_mutex_lock(&info->reader_count_mutex);

  if(0 == --info->reader_count)
    pthread_cond_signal(&info->write_ready);

  pthread_mutex_unlock(&info->reader_count_mutex);
#endif
}

static void
JPT_writer_enter(struct JPT_info* info)
{
#if GLOBAL_LOCKS
  pthread_mutex_lock(&global_lock);
  assert(!info->reader_count && !info->is_writing);
  info->is_writing = 1;
#else
  pthread_mutex_lock(&info->reader_count_mutex);

  while(info->reader_count || info->is_writing)
    pthread_cond_wait(&info->write_ready, &info->reader_count_mutex);

  info->is_writing = 1;

  pthread_mutex_unlock(&info->reader_count_mutex);

  assert(!info->reader_count);
#endif
}

static void
JPT_writer_leave(struct JPT_info* info)
{
#if GLOBAL_LOCKS
  info->is_writing = 0;
  pthread_mutex_unlock(&global_lock);
#else
  assert(!info->reader_count);
  assert(info->is_writing);

  pthread_mutex_lock(&info->reader_count_mutex);

  info->is_writing = 0;

  pthread_cond_signal(&info->write_ready);
  pthread_cond_broadcast(&info->read_ready);

  pthread_mutex_unlock(&info->reader_count_mutex);
#endif
}

const char*
jpt_last_error()
{
  TRACE((stderr, "jpt_last_error()\n"));

  return JPT_last_error ? JPT_last_error : strerror(errno);
}

void
JPT_set_error(char* error, int err)
{
  if(JPT_last_error)
  {
    if(error == JPT_last_error)
      return;

    free(JPT_last_error);
  }

  JPT_last_error = error;
  errno = err;
}

ssize_t
JPT_read_all(int fd, void* target, size_t size)
{
  size_t remaining = size;
  char* o = target;

  while(remaining)
  {
    ssize_t res = read(fd, o, remaining);

    if(res < 0)
      return -1;

    if(!res)
    {
      asprintf(&JPT_last_error, "Tried to read %zu bytes, got %zu", size, size - remaining);

      return -1;
    }

    o += res;
    remaining -= res;
  }

  return size;
}

ssize_t
JPT_write_all(int fd, const void* target, size_t size)
{
  size_t remaining = size;
  const char* o = target;

  while(remaining)
  {
    ssize_t res = write(fd, o, remaining);

    if(res < 0)
    {
      asprintf(&JPT_last_error, "Write failed: %s", strerror(errno));

      return -1;
    }

    if(!res)
    {
      asprintf(&JPT_last_error, "Tried to write %zu bytes, terminated after %zu", size, size - remaining);

      return -1;
    }

    o += res;
    remaining -= res;
  }

  return size;
}

off_t
JPT_lseek(int fd, off_t offset, int whence, off_t filesize)
{
  off_t new_pos = lseek64(fd, offset, whence);

  if(new_pos > filesize)
  {
    errno = ERANGE;

    return -1;
  }

  return new_pos;
}

static void
JPT_bloom_filter_indices(int indices[4], const char* key)
{
  uint32_t hash_a, hash_b;

  hash_a = *key;
  hash_b = 0;

  while(*key)
  {
    hash_b += (hash_a >> 27);
    hash_a = (hash_a << 5) - hash_a;
    hash_b = (hash_b << 5) - hash_b;
    hash_a += *key++;
  }

  indices[0] = hash_a & 0xffff;
  indices[1] = hash_a >> 16;
  indices[2] = hash_b & 0xffff;
  indices[3] = hash_b >> 16;
}

void
JPT_bloom_filter_add(unsigned char filter[][8192], const char* key)
{
  int indices[4];

  if(!*key)
    return;

  JPT_bloom_filter_indices(indices, key);

  filter[0][indices[0] >> 3] |= (1 << (indices[0] & 7));
  filter[1][indices[1] >> 3] |= (1 << (indices[1] & 7));
  filter[2][indices[2] >> 3] |= (1 << (indices[2] & 7));
  filter[3][indices[3] >> 3] |= (1 << (indices[3] & 7));
}

#define JPT_BLOOM_FILTER_TEST(filter, indices) \
    ((filter[0][indices[0] >> 3] & (1 << (indices[0] & 7))) \
  && (filter[1][indices[1] >> 3] & (1 << (indices[1] & 7))) \
  && (filter[2][indices[2] >> 3] & (1 << (indices[2] & 7))) \
  && (filter[3][indices[3] >> 3] & (1 << (indices[3] & 7))))

void
JPT_clear_error()
{
  free(JPT_last_error);
  JPT_errno = 0;
  JPT_last_error = 0;
}

static void*
JPT_buffer_alloc(struct JPT_info* info, size_t size)
{
  void* result;

  assert(info->buffer);
  assert(info->is_writing || info->reader_count);

  result = info->buffer + info->buffer_util;
  info->buffer_util = (info->buffer_util + size + 3) & ~3;

  assert(info->buffer_util <= info->buffer_size);

  return result;
}

static uint32_t
JPT_get_column_idx(struct JPT_info* info, const char* column, int flags)
{
  void* data;
  const char* c;
  size_t size, max_read;
  uint32_t hash;
  uint32_t index;
  char* new_name;
  char* old_name;
  char prefix[COLUMN_PREFIX_SIZE + 1];

  assert(info->is_writing || info->reader_count);

  if(column[0] == '_' && column[1] == '_')
  {
    if(!strcmp(column + 2, "META__"))
      return 0;
    else if(!strcmp(column + 2, "COLUMNS__"))
      return 1;
    else if(!strcmp(column + 2, "REV_COLUMNS__"))
      return 2;
    else if(!strcmp(column + 2, "COUNTERS__"))
      return 3;
  }

  hash = 0;
  c = column;

  while(*c)
  {
    hash = (hash << 5) - hash + *c;
    ++c;
  }

  hash %= info->column_count;
  hash &= ~1;

  pthread_mutex_lock(&info->column_hash_mutex);

  if(info->columns[hash].name
  && !strcmp(info->columns[hash].name, column))
  {
    size_t result = info->columns[hash].index;

    pthread_mutex_unlock(&info->column_hash_mutex);

    return result;
  }

  if(info->columns[hash + 1].name
  && !strcmp(info->columns[hash + 1].name, column))
  {
    size_t result = info->columns[hash + 1].index;

    pthread_mutex_unlock(&info->column_hash_mutex);

    return result;
  }

  pthread_mutex_unlock(&info->column_hash_mutex);

  data = &index;
  max_read = sizeof(uint32_t);

  uint64_t timestamp = jpt_gettime();

  if(-1 == JPT_get(info, column, "__COLUMNS__", &data, &size, 0, &max_read, 0))
  {
    if(errno == ENOENT && (flags & JPT_COL_CREATE))
    {
      if(info->next_column == 0xffffffff)
      {
        errno = ENOSPC;

        return (uint32_t) ~0;
      }

      index = info->next_column++;
      JPT_generate_key(prefix, "", index);

      if(-1 == JPT_insert(info, column, "__COLUMNS__", &index, sizeof(uint32_t), &timestamp, JPT_REPLACE))
        return (uint32_t) ~0;

      if(-1 == JPT_insert(info, prefix, "__REV_COLUMNS__", column, strlen(column) + 1, &timestamp, JPT_REPLACE))
        return (uint32_t) ~0;

      if(-1 == JPT_insert(info, "next-column", "__META__", &info->next_column, sizeof(uint32_t), &timestamp, JPT_REPLACE))
        return (uint32_t) ~0;
    }
    else
      return (uint32_t) ~0;
  }
  else if(size != sizeof(uint32_t))
  {
    errno = EILSEQ;

    return (uint32_t) ~0;
  }

  new_name = strdup(column);

  if(info->columns[hash].name)
  {
    if(!info->columns[hash].name)
      ++hash;
    else if(rand() & 1)
      ++hash;
  }

  pthread_mutex_lock(&info->column_hash_mutex);

  old_name = info->columns[hash].name;
  info->columns[hash].name = new_name;
  info->columns[hash].index = index;

  pthread_mutex_unlock(&info->column_hash_mutex);

  free(old_name);

  return index;
}

static const char*
JPT_get_column_name(struct JPT_info* info, uint32_t columnidx)
{
  static __thread char* result = 0;
  static __thread uint32_t last = (uint32_t) ~0;

  char prefix[COLUMN_PREFIX_SIZE + 1];
  size_t result_size;

  assert(info->is_writing || info->reader_count);

  if(columnidx == last)
    return result;

  if(columnidx < 100)
  {
    switch(columnidx)
    {
    case 0: return "__META__";
    case 1: return "__COLUMNS__";
    case 2: return "__REV_COLUMNS__";
    case 3: return "__COUNTERS__";
    }
  }

  last = columnidx;

  free(result);
  result = 0;

  JPT_generate_key(prefix, "", columnidx);

  if(-1 == JPT_get(info, prefix, "__REV_COLUMNS__", (void*) &result, &result_size, 0, 0, 0))
    return 0;

  return result;
}

static struct JPT_node*
JPT_create_node(struct JPT_info* info,
                const void* row, uint32_t columnidx,
                const void* value, size_t value_size,
                int must_compact)
{
  struct JPT_node* result;

  assert(info->is_writing || info->reader_count);

  result = JPT_buffer_alloc(info, sizeof(struct JPT_node));
  result->row = JPT_buffer_alloc(info, strlen(row) + 1);
  result->value_size = value_size;
  result->parent = 0;
  result->left = 0;
  result->right = 0;
  result->next = 0;
  result->last = 0;
  result->columnidx = columnidx;

  strcpy(result->row, row);

  if(must_compact)
    result->value = (char*) value;
  else
  {
    result->value = JPT_buffer_alloc(info, value_size);
    memcpy(result->value, value, value_size);
  }

  return result;
}

void
JPT_splay(struct JPT_info* info, struct JPT_node* n)
{
  assert(info->is_writing || info->reader_count);

  while(n->parent)
  {
    struct JPT_node* parent = n->parent;
    int isleft = (n == parent->left);

    assert(isleft ^ (n == parent->right));

    if(!parent->parent)
    {
      assert(parent == info->root);

      if(isleft)
      {
        info->root = n;

        parent->left = n->right;
        if(parent->left)
          parent->left->parent = parent;

        n->right = parent;
        parent->parent = n;

        n->parent = 0;
      }
      else
      {
        info->root = n;

        parent->right = n->left;
        if(parent->right)
          parent->right->parent = parent;

        n->left = parent;
        parent->parent = n;

        n->parent = 0;
      }

      break;
    }
    else
    {
      struct JPT_node* gparent = parent->parent;
      int parent_isleft = (parent == gparent->left);

      if(gparent->parent)
      {
        if(gparent->parent->left == gparent)
        {
          gparent->parent->left = n;
        }
        else
        {
          assert(gparent->parent->right == gparent);

          gparent->parent->right = n;
        }

        n->parent = gparent->parent;
      }
      else
      {
        info->root = n;

        n->parent = 0;
      }

      if(isleft && parent_isleft)
      {
        gparent->left = parent->right;
        if(gparent->left)
          gparent->left->parent = gparent;

        parent->left = n->right;
        if(parent->left)
          parent->left->parent = parent;

        parent->right = gparent;
        if(parent->right)
          parent->right->parent = parent;

        n->right = parent;
        n->right->parent = n;
      }
      else if(!isleft && !parent_isleft)
      {
        gparent->right = parent->left;
        if(gparent->right)
          gparent->right->parent = gparent;

        parent->right = n->left;
        if(parent->right)
          parent->right->parent = parent;

        parent->left = gparent;
        if(parent->left)
          parent->left->parent = parent;

        n->left = parent;
        n->left->parent = n;
      }
      else if(!isleft && parent_isleft)
      {
        gparent->left = n->right;
        if(gparent->left)
          gparent->left->parent = gparent;

        parent->right = n->left;
        if(parent->right)
          parent->right->parent = parent;

        n->left = parent;
        n->left->parent = n;

        n->right = gparent;
        n->right->parent = n;
      }
      else
      {
        assert(isleft && !parent_isleft);

        gparent->right = n->left;
        if(gparent->right)
          gparent->right->parent = gparent;

        parent->left = n->right;
        if(parent->left)
          parent->left->parent = parent;

        n->right = parent;
        n->right->parent = n;

        n->left = gparent;
        n->left->parent = n;
      }
    }
  }
}

void
JPT_generate_key(char* target, const char* row, uint32_t columnidx)
{
  target[0] = (columnidx / 16581375 % 255) + 1;
  target[1] = (columnidx / 65025 % 255) + 1;
  target[2] = (columnidx / 255 % 255) + 1;
  target[3] = (columnidx % 255) + 1;
  strcpy(target + COLUMN_PREFIX_SIZE, row);
}

static const char*
JPT_node_key_callback(unsigned int idx, void* arg)
{
  struct JPT_node** nodes = arg;

  /* XXX: Need to free key_buf on thread exit */
  static __thread size_t key_buf_size = 0;
  static __thread char* key_buf = 0;

  if(strlen(nodes[idx]->row) + COLUMN_PREFIX_SIZE + 1 > key_buf_size)
  {
    key_buf_size = strlen(nodes[idx]->row) + 32;
    free(key_buf);
    key_buf = malloc(key_buf_size);
  }

  JPT_generate_key(key_buf, nodes[idx]->row, nodes[idx]->columnidx);

  return key_buf;
}

void
JPT_update_map(struct JPT_info* info)
{
  assert(info->is_writing || info->reader_count);

  info->file_size = lseek64(info->fd, 0, SEEK_END);

  if(!info->file_size)
    return;

  if(info->file_size > (size_t) -1)
    return;

  if(info->map_size)
  {
    void* new_map;

    new_map = mremap(info->map, info->map_size, info->file_size, MREMAP_MAYMOVE);

    if(new_map != MAP_FAILED)
    {
      info->map = new_map;
      info->map_size = info->file_size;
    }
    else
    {
      munmap(info->map, info->map_size);
      info->map_size = 0;
    }
  }
  else
  {
    info->map = mmap(0, info->file_size, PROT_READ | PROT_WRITE, MAP_SHARED, info->fd, 0);

    if(info->map != MAP_FAILED)
      info->map_size = info->file_size;
  }

  if(info->map_size)
  {
    struct JPT_disktable* disktable;

    disktable = info->first_disktable;

    while(disktable)
    {
      patricia_remap(disktable->pat, info->map + disktable->pat_offset);
      disktable->pat_mapped = 1;

      disktable->key_infos = (struct JPT_key_info*) (info->map + disktable->key_info_offset);
      disktable->key_infos_mapped = 1;

      disktable = disktable->next;
    }
  }
  else
  {
    struct JPT_disktable* disktable;

    disktable = info->first_disktable;

    while(disktable)
    {
      if(disktable->pat_mapped)
      {
        lseek64(info->fd, disktable->pat_offset, SEEK_SET);
        patricia_read(disktable->pat, info->fd);
        disktable->pat_mapped = 0;
      }

      disktable->key_infos = 0;
      disktable->key_infos_mapped = 0;

      disktable = disktable->next;
    }
  }
}

struct JPT_info*
jpt_init(const char* filename, size_t buffer_size, int flags)
{
  jmp_buf io_error;
  struct JPT_info* info = 0;
  struct JPT_disktable* disktable;
  char signature[4];
  uint32_t version;
  uint32_t row_count;
  uint32_t data_size;
  int res;
  off_t offset;
  char* logname;

  assert(sizeof(off_t) == 8);

  TRACE((stderr, "jpt_init(%s, %zu)\n", filename, buffer_size));

  JPT_clear_error();

  info = malloc(sizeof(struct JPT_info));

  if(!info)
    return 0;

  memset(info, 0, sizeof(struct JPT_info));

  info->flags = flags;
  info->fd = open(filename, O_RDWR | O_CREAT, 0600);

  if(info->fd == -1)
    goto fail;

  if(-1 == lockf(info->fd, F_TLOCK, INT_MAX))
    goto fail;

  if(-1 == asprintf(&logname, "%s.log", filename))
    goto fail;

  info->logfile = fopen(logname, "a+");

  if(!info->logfile)
    goto fail;

  free(logname);

  pthread_mutex_init(&info->reader_count_mutex, 0);
  pthread_cond_init(&info->write_ready, 0);
  pthread_cond_init(&info->read_ready, 0);
  pthread_mutex_init(&info->column_hash_mutex, 0);
  pthread_mutex_init(&info->memtable_mutex, 0);

  info->is_writing = 1;

  JPT_update_map(info);

  lseek64(info->fd, 0, SEEK_SET);

  for(;;)
  {
    offset = lseek64(info->fd, 0, SEEK_CUR);

    if(setjmp(io_error))
    {
      if(flags & JPT_RECOVER)
      {
        if(-1 == JPT_lseek(info->fd, offset, SEEK_SET, info->file_size))
          goto fail;

        if(-1 == ftruncate(info->fd, offset))
          goto fail;

        break;
      }

      char* prev_error = strdupa(jpt_last_error());
      free(JPT_last_error);
      asprintf(&JPT_last_error, "%s.  Run `jpt-control %s recover' to truncate offending data", prev_error, filename);

      free(info);

      return 0;
    }

    version = 0;

    res = read(info->fd, signature, 4);

    if(res < sizeof(uint32_t))
    {
      if(res == -1)
        longjmp(io_error, 1);

      break;
    }

    if(!memcmp(signature, JPT_PARTIAL_WRITE, 4))
    {
      flags |= JPT_RECOVER;

      longjmp(io_error, 1);
    }

    if(memcmp(signature, JPT_SIGNATURE, 4))
    {
      if(flags & JPT_RECOVER)
        longjmp(io_error, 1);
      else
      {
        asprintf(&JPT_last_error, "Database corrupt at offset 0x%llx (found %.*s, expected %s).  Run `jpt-control %s recover'", (long long) offset, 4, signature, JPT_SIGNATURE, filename);

        goto fail;
      }
    }

    if(sizeof(uint32_t) != JPT_read_all(info->fd, &version, sizeof(uint32_t)))
      longjmp(io_error, 1);

    if(version > JPT_VERSION)
    {
      JPT_errno = JPT_EVERSION;
      asprintf(&JPT_last_error, "Table version %u is not supported (maximum is %u)", version, JPT_VERSION);

      goto fail;
    }

    if(-1 == JPT_read_all(info->fd, &row_count, sizeof(uint32_t)))
      longjmp(io_error, 1);

    if(-1 == JPT_read_all(info->fd, &data_size, sizeof(uint32_t)))
      longjmp(io_error, 1);

    if(version < 8)
    {
      JPT_errno = JPT_EVERSION;
      asprintf(&JPT_last_error, "Table version %u is too old.  Use jpt-control backup/restore", version);

      goto fail;
    }

    disktable = malloc(sizeof(struct JPT_disktable));

    if(-1 == JPT_read_all(info->fd, disktable->bloom_filter, sizeof(disktable->bloom_filter)))
      longjmp(io_error, 1);

    disktable->pat = patricia_create(0, 0);

    disktable->pat_offset = lseek64(info->fd, 0, SEEK_CUR);

    if(!info->map_size)
    {
      patricia_read(disktable->pat, info->fd);
    }
    else
    {
      size_t pat_size;

      pat_size = patricia_remap(disktable->pat, info->map + disktable->pat_offset);

      if(-1 == JPT_lseek(info->fd, pat_size, SEEK_CUR, info->file_size))
        longjmp(io_error, 1);
    }

    disktable->key_info_offset = lseek64(info->fd, 0, SEEK_CUR);
    disktable->key_info_count = row_count;

    if(!info->map_size)
      disktable->key_infos_mapped = 0;
    else
    {
      disktable->key_infos = (struct JPT_key_info*) (info->map + disktable->key_info_offset);
      disktable->key_infos_mapped = 1;
    }

    if(-1 == JPT_lseek(info->fd, row_count * sizeof(struct JPT_key_info), SEEK_CUR, info->file_size))
      longjmp(io_error, 1);

    disktable->offset = lseek64(info->fd, 0, SEEK_CUR);

    if(-1 == JPT_lseek(info->fd, disktable->offset + data_size, SEEK_SET, info->file_size))
      longjmp(io_error, 1);

    disktable->info = info;
    disktable->next = 0;

    if(!info->first_disktable)
    {
      info->first_disktable = disktable;
      info->last_disktable = disktable;
    }
    else
    {
      info->last_disktable->next = disktable;
      info->last_disktable = disktable;
    }

    ++info->disktable_count;
  }

  info->buffer_size = buffer_size;
  info->buffer = malloc(buffer_size);
  info->filename = strdup(filename);

  info->column_count = 128;
  info->columns = malloc(info->column_count * sizeof(struct JPT_column));
  memset(info->columns, 0, info->column_count * sizeof(struct JPT_column));

  info->is_writing = 0;

  if(sizeof(uint32_t) != jpt_get_fixed(info, "next-column", "__META__", &info->next_column, sizeof(uint32_t)))
    info->next_column = 100;

  info->is_writing = 1;

  if(-1 == JPT_log_replay(info))
    goto fail;

  info->is_writing = 0;

  return info;

fail:

  if(info->logfile)
    fclose(info->logfile);

  if(info->fd != -1)
    close(info->fd);

  free(info);

  return 0;
}

static void
JPT_free_disktables(struct JPT_info* info)
{
  struct JPT_disktable* dt;

  assert(info->is_writing || info->reader_count);

  dt = info->first_disktable;

  while(dt)
  {
    struct JPT_disktable* tmp = dt;
    dt = dt->next;

    patricia_destroy(tmp->pat);

    free(tmp);
  }
}

int
JPT_compact(struct JPT_info* info)
{
  struct JPT_node** nodes;
  struct JPT_node** iterator;
  struct patricia* pat;
  size_t i, j, amount;

  off_t offset = 0;
  off_t data_start = 0;
  uint32_t row_count = 0;
  struct JPT_key_info* key_infos;

  jmp_buf io_error;
  off_t old_eof;

  assert(info->is_writing && !info->reader_count);

  if(!info->memtable_key_count)
    return JPT_log_reset(info);

  key_infos = malloc(sizeof(struct JPT_key_info) * info->memtable_key_count);
  nodes = malloc(sizeof(struct JPT_node*) * info->node_count);
  iterator = nodes;

  JPT_memtable_list_all(info->root, &iterator);

  pat = patricia_create(JPT_node_key_callback, nodes);

  size_t key_buf_size = 256;
  char* key_buf = malloc(key_buf_size);

  size_t sum_key_size = 0;
  size_t sum_value_size = 0;
  size_t sum_key_count = 0;

  struct JPT_disktable* disktable = malloc(sizeof(struct JPT_disktable));

  memset(disktable->bloom_filter, 0, sizeof(disktable->bloom_filter));
  disktable->key_infos_mapped = 0;

  for(i = 0; i < info->node_count; ++i)
  {
    if(strlen(nodes[i]->row) + 3 > key_buf_size)
    {
      key_buf_size = strlen(nodes[i]->row) + 32;
      free(key_buf);
      key_buf = malloc(key_buf_size);
    }

    sum_key_size += strlen(nodes[i]->row) + 1;
    sum_value_size += nodes[i]->value_size;
    ++sum_key_count;

    JPT_generate_key(key_buf, nodes[i]->row, nodes[i]->columnidx);

    j = patricia_define(pat, key_buf);

    assert(j == row_count);

    JPT_bloom_filter_add(disktable->bloom_filter, key_buf);

    key_infos[row_count].timestamp = nodes[i]->timestamp;
    key_infos[row_count].offset = offset;
    key_infos[row_count].size = strlen(key_buf) + 1 + nodes[i]->value_size;
    key_infos[row_count].flags = 0;

    struct JPT_node_data* d = nodes[i]->next;

    while(d)
    {
      key_infos[row_count].size += d->value_size;
      sum_value_size += d->value_size;
      d = d->next;
    }

    offset += key_infos[row_count].size;
    ++row_count;
  }

  assert(offset == info->memtable_key_size + info->memtable_key_count * COLUMN_PREFIX_SIZE + info->memtable_value_size);
  assert(row_count == info->memtable_key_count);

  old_eof = lseek64(info->fd, 0, SEEK_END);

  if(setjmp(io_error))
  {
    ftruncate(info->fd, old_eof);
    free(key_buf);
    free(key_infos);
    free(nodes);
    free(disktable);

    return -1;
  }

  uint32_t version = JPT_VERSION;
  uint32_t data_size = key_infos[row_count - 1].offset + key_infos[row_count - 1].size;

  if(-1 == JPT_write_all(info->fd, JPT_PARTIAL_WRITE, 4))
    longjmp(io_error, 1);

  if(-1 == JPT_write_all(info->fd, &version, sizeof(uint32_t)))
    longjmp(io_error, 1);

  if(-1 == JPT_write_all(info->fd, &row_count, sizeof(uint32_t)))
    longjmp(io_error, 1);

  if(-1 == JPT_write_all(info->fd, &data_size, sizeof(uint32_t)))
    longjmp(io_error, 1);

  if(-1 == JPT_write_all(info->fd, disktable->bloom_filter, sizeof(disktable->bloom_filter)))
    longjmp(io_error, 1);

  disktable->pat_offset = lseek64(info->fd, 0, SEEK_CUR);

  if(-1 == patricia_write(pat, info->fd))
  {
    asprintf(&JPT_last_error, "Failed to write PATRICIA trie failed: %s", strerror(errno));

    longjmp(io_error, 1);
  }

  amount = row_count * sizeof(struct JPT_key_info);

  disktable->key_info_offset = lseek64(info->fd, 0, SEEK_CUR);

  if(amount != JPT_write_all(info->fd, key_infos, amount))
    longjmp(io_error, 1);

  data_start = lseek64(info->fd, 0, SEEK_CUR);
  offset = data_start;

  if(-1 == ftruncate(info->fd, data_start + data_size))
  {
    asprintf(&JPT_last_error, "Failed to resize file to %llu bytes: %s", (long long) (data_start + data_size), strerror(errno));

    longjmp(io_error, 1);
  }

  JPT_update_map(info);

  lseek64(info->fd, data_start, SEEK_SET);

  for(i = 0; i < info->node_count; ++i)
  {
    struct JPT_node_data* d = nodes[i]->next;
    size_t keylen;

    JPT_generate_key(key_buf, nodes[i]->row, nodes[i]->columnidx);

    keylen = strlen(key_buf) + 1;

    if(info->map_size)
    {
      memcpy(info->map + offset, key_buf, keylen);
      offset += keylen;
      memcpy(info->map + offset, nodes[i]->value, nodes[i]->value_size);
      offset += nodes[i]->value_size;

      while(d)
      {
        memcpy(info->map + offset, d->value, d->value_size);
        offset += d->value_size;
        d = d->next;
      }
    }
    else
    {
      if(-1 == JPT_write_all(info->fd, key_buf, keylen))
        longjmp(io_error, 1);

      if(-1 == JPT_write_all(info->fd, nodes[i]->value, nodes[i]->value_size))
        longjmp(io_error, 1);

      while(d)
      {
        if(-1 == JPT_write_all(info->fd, d->value, d->value_size))
          longjmp(io_error, 1);

        d = d->next;
      }
    }
  }

  if(-1 == lseek64(info->fd, old_eof, SEEK_SET))
    longjmp(io_error, 1);

  if(-1 == JPT_write_all(info->fd, JPT_SIGNATURE, 4))
    longjmp(io_error, 1);

  fdatasync(info->fd);

  if(-1 == JPT_log_reset(info))
    longjmp(io_error, 1);

  free(key_buf);
  free(nodes);
  free(key_infos);

  info->buffer_util = 0;
  info->root = 0;
  info->node_count = 0;
  info->memtable_key_count = 0;
  info->memtable_key_size = 0;
  info->memtable_value_size = 0;

  disktable->pat = pat;
  disktable->key_infos = 0;
  disktable->key_info_count = row_count;
  disktable->info = info;
  disktable->offset = data_start;
  disktable->next = 0;

  if(!info->first_disktable)
  {
    info->first_disktable = disktable;
    info->last_disktable = disktable;
  }
  else
  {
    info->last_disktable->next = disktable;
    info->last_disktable = disktable;
  }

  JPT_update_map(info);

  ++info->disktable_count;

  return 0;
}

int
jpt_compact(struct JPT_info* info)
{
  int result;

  TRACE((stderr, "jpt_compact(%p)\n", info));

  JPT_clear_error();

  JPT_writer_enter(info);

  result = JPT_compact(info);

  JPT_writer_leave(info);

  return result;
}

const char*
JPT_key_info_callback(unsigned int idx, void* arg)
{
  struct JPT_key_info_callback_args* args = arg;

  static __thread size_t key_buf_size = 0;
  static __thread char* key_buf = 0;

  if(args->row_names[idx].size + 1 > key_buf_size)
  {
    key_buf_size = args->row_names[idx].size + 32;
    free(key_buf);
    key_buf = malloc(key_buf_size);
  }

  if(args->info->map_size)
  {
    memcpy(key_buf, args->info->map + args->row_names[idx].offset,
           args->row_names[idx].size);
  }
  else
  {
    pread64(args->info->fd, key_buf, args->row_names[idx].size,
            args->row_names[idx].offset);
  }

  key_buf[args->row_names[idx].size] = 0;

  return key_buf;
}

int
jpt_major_compact(struct JPT_info* info)
{
  char* newname;
  struct JPT_disktable* n;
  struct JPT_disktable_cursor* cursors;
  struct patricia* pat;
  size_t i, j;
  int outfd;
  int ok = 0;

  uint32_t row_count = 0;
  off_t offset = 0;
  struct JPT_key_info* row_names;
  struct JPT_key_info* key_infos;
  struct JPT_key_info_callback_args callback_args;

  TRACE((stderr, "jpt_major_compact(%p)\n", info));

  JPT_clear_error();

  JPT_writer_enter(info);

  if(-1 == JPT_compact(info))
  {
    JPT_writer_leave(info);

    return -1;
  }

  if(info->disktable_count < 2)
  {
    JPT_write_leave(info);

    return 0;
  }


  newname = alloca(strlen(info->filename) + 8);
  strcpy(newname, info->filename);
  strcat(newname, ".XXXXXX");
  outfd = mkstemp(newname);

  struct JPT_disktable* disktable = malloc(sizeof(struct JPT_disktable));

  memset(disktable->bloom_filter, 0, sizeof(disktable->bloom_filter));
  disktable->key_infos_mapped = 0;

  cursors = calloc(info->disktable_count, sizeof(struct JPT_disktable_cursor));

  n = info->first_disktable;
  i = 0;

  while(n)
  {
    cursors[i++].disktable = n;
    row_count += n->key_info_count;

    n = n->next;
  }

  row_names = malloc(sizeof(struct JPT_key_info) * row_count);

  if(!row_names)
  {
    asprintf(&JPT_last_error, "malloc failed while allocating %zu bytes", sizeof(struct JPT_key_info) * row_count);

    return -1;
  }

  key_infos = malloc(sizeof(struct JPT_key_info) * row_count);

  if(!key_infos)
  {
    asprintf(&JPT_last_error, "malloc failed while allocating %zu bytes", sizeof(struct JPT_key_info) * row_count);

    free(row_names);

    return -1;
  }

  callback_args.row_names = row_names;
  callback_args.info = info;

  pat = patricia_create(JPT_key_info_callback, &callback_args);

  row_count = 0;

  for(;;)
  {
    for(i = 0; i < info->disktable_count; ++i)
    {
      if(!cursors[i].data_size && cursors[i].offset < cursors[i].disktable->key_info_count)
      {
        if(-1 == JPT_disktable_cursor_advance(info, &cursors[i], (size_t) -1))
          goto fail;
      }
    }

    const char* min = 0;
    size_t minidx = 0;

    for(i = 0; i < info->disktable_count; ++i)
    {
      if(!cursors[i].data_size)
        continue;

      if(!min || strcmp(cursors[i].data, min) < 0)
      {
        min = cursors[i].data;
        minidx = i;
      }
    }

    if(!min)
      break;

    j = patricia_define(pat, min);

    if(j == row_count)
    {
      JPT_bloom_filter_add(disktable->bloom_filter, min);

      row_names[j].offset = cursors[minidx].data_offset;
      row_names[j].size = strlen(cursors[minidx].data);

      key_infos[j].timestamp = cursors[minidx].timestamp;
      key_infos[j].offset = offset;
      key_infos[j].size = cursors[minidx].data_size;
      key_infos[j].flags = cursors[minidx].flags;

      offset += cursors[minidx].data_size;

      ++row_count;
    }
    else
    {
      assert(j == row_count - 1);

      key_infos[j].size += cursors[minidx].data_size - cursors[minidx].keylen;
      offset += cursors[minidx].data_size - cursors[minidx].keylen;
    }

    cursors[minidx].data_size = 0;
  }

  uint32_t version = JPT_VERSION;
  off_t data_size = offset;

  if(-1 == JPT_write_all(outfd, JPT_PARTIAL_WRITE, 4))
    goto fail;

  if(-1 == JPT_write_all(outfd, &version, sizeof(uint32_t)))
    goto fail;

  if(-1 == JPT_write_all(outfd, &row_count, sizeof(uint32_t)))
    goto fail;

  if(-1 == JPT_write_all(outfd, &data_size, sizeof(uint32_t)))
    goto fail;

  if(-1 == JPT_write_all(outfd, disktable->bloom_filter, sizeof(disktable->bloom_filter)))
    goto fail;

  disktable->pat_offset = lseek64(outfd, 0, SEEK_CUR);
  if(-1 == patricia_write(pat, outfd))
    goto fail;

  disktable->key_info_offset = lseek64(outfd, 0, SEEK_CUR);
  if(-1 == JPT_write_all(outfd, key_infos, row_count * sizeof(struct JPT_key_info)))
    goto fail;

  offset = lseek64(outfd, 0, SEEK_CUR);

  row_count = 0;

  for(i = 0; i < info->disktable_count; ++i)
  {
    assert(!cursors[i].data_size);
    cursors[i].offset = 0;
  }

  for(;;)
  {
    for(i = 0; i < info->disktable_count; ++i)
    {
      if(!cursors[i].data_size && cursors[i].offset < cursors[i].disktable->key_info_count)
      {
        if(-1 == JPT_disktable_cursor_advance(info, &cursors[i], (size_t) -1))
          goto fail;
      }
    }

    const char* min = 0;
    size_t minidx = 0;

    for(i = 0; i < info->disktable_count; ++i)
    {
      if(!cursors[i].data_size)
        continue;

      if(!min || strcmp(cursors[i].data, min) < 0)
      {
        min = cursors[i].data;
        minidx = i;
      }
    }

    if(!min)
      break;

    j = patricia_lookup(pat, min);

    if(j == row_count)
    {
      if(cursors[minidx].data_size != JPT_write_all(outfd, cursors[minidx].data, cursors[minidx].data_size))
        goto fail;

      ++row_count;
    }
    else
    {
      size_t amount;

      assert(j == row_count - 1);

      amount = cursors[minidx].data_size - cursors[minidx].keylen;

      if(amount != JPT_write_all(outfd, cursors[minidx].data + cursors[minidx].keylen, amount))
        goto fail;
    }

    cursors[minidx].data_size = 0;
  }

  if(-1 == lseek64(outfd, 0, SEEK_SET))
    goto fail;

  if(-1 == JPT_write_all(outfd, JPT_SIGNATURE, 4))
    goto fail;

  if(info->map_size)
  {
    munmap(info->map, info->map_size);
    info->map_size = 0;
  }

  if(-1 == rename(newname, info->filename))
    goto fail;

  JPT_free_disktables(info);

  close(info->fd);
  info->fd = outfd;

  disktable->pat = pat;
  disktable->key_infos = 0;
  disktable->key_info_count = row_count;
  disktable->info = info;
  disktable->offset = offset;
  disktable->next = 0;

  info->first_disktable = disktable;
  info->last_disktable = disktable;
  info->disktable_count = 1;

  JPT_update_map(info);

  ok = 1;

fail:

  if(!info->map_size)
  {
    for(i = 0; i < info->disktable_count; ++i)
      free(cursors[i].buffer);
  }

  free(cursors);
  free(row_names);
  free(key_infos);

  ++info->major_compact_count;

  JPT_writer_leave(info);

  if(!ok)
  {
    free(disktable);
    close(outfd);
    unlink(newname);

    return -1;
  }

  return 0;
}

static int
JPT_insert(struct JPT_info* info,
           const char* row, const char* column,
           const void* value, size_t value_size,
           uint64_t* timestamp, int flags)
{
  int bloom_indices[4];
  const char* cvalue = value;
  struct JPT_node* n;
  int cmp;
  uint32_t columnidx;
  size_t space_needed;
  size_t row_size = strlen(row) + 1;
  size_t column_size = strlen(column) + 1;
  char* key = alloca(strlen(row) + COLUMN_PREFIX_SIZE + 1);
  int must_compact = 0;
  int written = 0;

  assert(info->is_writing && !info->reader_count);

  if(row_size + COLUMN_PREFIX_SIZE - 1 > PATRICIA_MAX_KEYLENGTH)
  {
    errno = EINVAL;

    return -1;
  }

  columnidx = JPT_get_column_idx(info, column, JPT_COL_CREATE);

  if(columnidx == (uint32_t) ~0)
    return -1;

  JPT_generate_key(key, row, columnidx);
  JPT_bloom_filter_indices(bloom_indices, key);

  if(flags & JPT_REPLACE)
  {
    struct JPT_disktable* d = info->first_disktable;

    while(d)
    {
      if(JPT_BLOOM_FILTER_TEST(d->bloom_filter, bloom_indices))
      {
        if(value_size)
        {
          ssize_t result;

          result = JPT_disktable_overwrite(d, row, columnidx, cvalue, value_size);

          if(result == -1 && errno != ENOENT)
            return -1;

          if(result > 0)
          {
            cvalue += result;
            value_size -= result;
            written = 1;
          }
        }
        else
        {
          if(-1 == JPT_disktable_remove(d, row, columnidx) && errno != ENOENT)
            return -1;
        }
      }

      d = d->next;
    }
  }
  else if(!(flags & JPT_APPEND))
  {
    struct JPT_disktable* d = info->first_disktable;

    while(d)
    {
      if(0 == JPT_disktable_has_key(d, row, columnidx))
      {
        errno = EEXIST;

        return -1;
      }

      d = d->next;
    }
  }

  space_needed = ((row_size + 3) & ~3)
               + ((column_size + 3) & ~3)
               + ((sizeof(struct JPT_node) + 3) & ~3);

  if(info->buffer_util + space_needed > info->buffer_size)
  {
    if(-1 == JPT_compact(info))
      return -1;
  }

  assert(info->buffer_util + space_needed <= info->buffer_size);

  space_needed += ((value_size + 3) & ~3);

  if(info->buffer_util + space_needed > info->buffer_size)
    must_compact = 1;

  if(!info->root)
  {
    if((flags & JPT_REPLACE) && written && !value_size)
      return 0;

    info->root = JPT_create_node(info, row, columnidx, cvalue, value_size, must_compact);
    info->root->timestamp = *timestamp;
    ++info->node_count;
    ++info->memtable_key_count;
    info->memtable_key_size += strlen(row) + 1;
    info->memtable_value_size += value_size;

    goto done;
  }

  n = info->root;

  for(;;)
  {
    if(columnidx != n->columnidx)
      cmp = columnidx - n->columnidx;
    else
      cmp = strcmp(row, n->row);

    if(!cmp)
    {
      if(n->value == (void*) -1)
      {
        if(must_compact)
          n->value = (char*) cvalue;
        else
        {
          n->value = JPT_buffer_alloc(info, value_size);
          memcpy(n->value, cvalue, value_size);
        }

        n->timestamp = *timestamp;
        n->value_size = value_size;
        n->next = 0;
        n->last = 0;

        JPT_splay(info, n);

        info->memtable_value_size += value_size;
        info->memtable_key_size += strlen(row) + 1;
        ++info->node_count;
        ++info->memtable_key_count;

        goto done;
      }
      else if(flags & JPT_APPEND)
      {
        struct JPT_node_data* d = JPT_buffer_alloc(info, sizeof(struct JPT_node_data));

        if(!n->last)
        {
          n->next = d;
          n->last = d;
        }
        else
        {
          assert(!n->last->next);
          n->last->next = d;
          n->last = d;
        }

        if(must_compact)
          d->value = (char*) cvalue;
        else
        {
          d->value = JPT_buffer_alloc(info, value_size);
          memcpy(d->value, cvalue, value_size);
        }

        n->timestamp = *timestamp;
        d->value_size = value_size;
        d->next = 0;

        JPT_splay(info, n);

        info->memtable_value_size += value_size;

        goto done;
      }
      else if(flags & JPT_REPLACE)
      {
        if(n->value_size >= value_size)
        {
          info->memtable_value_size -= n->value_size;
          info->memtable_value_size += value_size;

          memcpy(n->value, cvalue, value_size);
          n->value_size = value_size;

          struct JPT_node_data* d = n->next;

          while(d)
          {
            info->memtable_value_size -= d->value_size;

            d = d->next;
          }

          n->next = 0;
          n->last = 0;
        }
        else
        {
          memcpy(n->value, cvalue, n->value_size);
          cvalue += n->value_size;
          value_size -= n->value_size;

          struct JPT_node_data* d = n->next;
          n->last = 0;

          while(d && value_size)
          {
            if(d->value_size >= value_size)
            {
              info->memtable_value_size -= d->value_size;
              info->memtable_value_size += value_size;
              d->value_size = value_size;
            }

            memcpy(d->value, cvalue, d->value_size);

            cvalue += d->value_size;
            value_size -= d->value_size;

            n->last = d;

            d = d->next;
          }

          while(d)
          {
            info->memtable_value_size -= d->value_size;

            d = d->next;
          }

          if(value_size)
          {
            d = JPT_buffer_alloc(info, sizeof(struct JPT_node_data));

            if(must_compact)
              d->value = (char*) cvalue;
            else
            {
              d->value = JPT_buffer_alloc(info, value_size);
              memcpy(d->value, cvalue, value_size);
            }

            d->value_size = value_size;
            d->next = 0;

            info->memtable_value_size += value_size;

            if(n->next)
            {
              n->last->next = d;
              n->last = d;
            }
            else
            {
              n->next = d;
              n->last = d;
            }
          }
          else if(n->last)
            n->last->next = 0;
        }

        n->timestamp = *timestamp;

        JPT_splay(info, n);

        goto done;
      }
      else
      {
        JPT_splay(info, n);

        errno = EEXIST;

        return -1;
      }
    }

    if(cmp < 0)
    {
      if(!n->left)
      {
        if((flags & JPT_REPLACE) && written && !value_size)
          goto done;

        n->left = JPT_create_node(info, row, columnidx, cvalue, value_size, must_compact);
        n->left->timestamp = *timestamp;
        n->left->parent = n;
        ++info->node_count;
        ++info->memtable_key_count;
        info->memtable_key_size += strlen(row) + 1;
        info->memtable_value_size += value_size;

        JPT_splay(info, n->left);

        goto done;
      }
      else
        n = n->left;
    }
    else
    {
      if(!n->right)
      {
        if((flags & JPT_REPLACE) && written && !value_size)
          goto done;

        n->right = JPT_create_node(info, row, columnidx, cvalue, value_size, must_compact);
        n->right->timestamp = *timestamp;
        n->right->parent = n;
        ++info->node_count;
        ++info->memtable_key_count;
        info->memtable_key_size += strlen(row) + 1;
        info->memtable_value_size += value_size;

        JPT_splay(info, n->right);

        goto done;
      }
      else
        n = n->right;
    }
  }

done:

  if(must_compact)
  {
    if(-1 == JPT_compact(info))
      return -1;
  }

  return 0;
}

int
jpt_insert_timestamp(struct JPT_info* info,
           const char* row, const char* column,
           const void* value, size_t value_size,
           uint64_t* timestamp, int flags)
{
  int res;

  TRACE((stderr, "jpt_insert_timestamp(%p, \"%s\", \"%s\", \"%.*s\", %zu, 0x%04x)\n", info, row, column, (int) value_size, (const char*) value, value_size, flags));

  JPT_clear_error();

  JPT_writer_enter(info);

  res = JPT_insert(info, row, column, value, value_size, timestamp, flags);

  JPT_writer_leave(info);

  if(res != -1)
  {
    int rowlen = strlen(row);
    int collen = strlen(column);

    if(-1 == JPT_log_begin(info)
    || -1 == JPT_write_uint(info->logfile, JPT_OPERATOR_INSERT)
    || -1 == JPT_write_uint(info->logfile, flags)
    || -1 == JPT_write_uint(info->logfile, rowlen)
    || -1 == JPT_write_uint(info->logfile, collen)
    || -1 == JPT_write_uint(info->logfile, value_size)
    || -1 == JPT_write_uint64(info->logfile, *timestamp)
    || rowlen != fwrite(row, 1, rowlen, info->logfile)
    || collen != fwrite(column, 1, collen, info->logfile)
    || value_size != fwrite(value, 1, value_size, info->logfile)
    || -1 == fflush(info->logfile))
      return -1;

    if(info->flags & JPT_SYNC)
      fdatasync(fileno(info->logfile));
  }

  return res;
}

int
jpt_insert(struct JPT_info* info,
           const char* row, const char* column,
           const void* value, size_t value_size, int flags)
{
  uint64_t timestamp = jpt_gettime();

  return jpt_insert_timestamp(info, row, column, value, value_size, &timestamp, flags);
}

static int
JPT_remove(struct JPT_info* info, const char* row, const char* column)
{
  int bloom_indices[4];
  struct JPT_node* n;
  struct JPT_disktable* disktable = info->first_disktable;
  char* key = alloca(strlen(row) + COLUMN_PREFIX_SIZE + 1);
  uint32_t columnidx;
  int cmp;
  int found = 0;

  assert(info->is_writing && !info->reader_count);

  columnidx = JPT_get_column_idx(info, column, 0);

  if(columnidx == (uint32_t) ~0)
  {
    errno = ENOENT;

    return -1;
  }

  JPT_generate_key(key, row, columnidx);
  JPT_bloom_filter_indices(bloom_indices, key);

  while(disktable)
  {
    if(JPT_BLOOM_FILTER_TEST(disktable->bloom_filter, bloom_indices))
    {
      if(0 == JPT_disktable_remove(disktable, row, columnidx))
        found = 1;
    }

    disktable = disktable->next;
  }

  n = info->root;

  while(n)
  {
    if(columnidx != n->columnidx)
      cmp = columnidx - n->columnidx;
    else
      cmp = strcmp(row, n->row);

    if(!cmp)
    {
      if(n->value != (void*) -1)
      {
        struct JPT_node_data* d = n->next;

        while(d)
        {
          info->memtable_value_size -= d->value_size;

          d = d->next;
        }

        info->memtable_value_size -= n->value_size;
        info->memtable_key_size -= strlen(row) + 1;
        --info->memtable_key_count;
        --info->node_count;

        n->value = (void*) -1;
        n->next = 0;

        found = 1;
      }

      break;
    }

    if(cmp < 0)
      n = n->left;
    else
      n = n->right;
  }

  if(!found)
  {
    errno = ENOENT;

    return -1;
  }

  return 0;
}

static int
JPT_log_replay(struct JPT_info* info)
{
  long size;
  uint64_t old_size;
  char* row = 0;
  char* col = 0;
  char* value = 0;
  int result = -1;
  off_t last_valid = 0;

  if(-1 == fseek(info->logfile, 0, SEEK_END))
    return -1;

  size = ftell(info->logfile);

  if(-1 == fseek(info->logfile, 0, SEEK_SET))
    return -1;

  if(size == 0)
  {
    info->logfile_empty = 1;

    return 0;
  }

  if(size < sizeof(uint64_t))
    goto truncate;

  old_size = JPT_read_uint64(info->logfile);

  if(info->file_size < old_size)
  {
    asprintf(&JPT_last_error, "log file's record of database size (%llu) is larger than actual size (%llu)",
             (unsigned long long) old_size, (unsigned long long) info->file_size);
    errno = EINVAL;

    return -1;
  }

  if(size == sizeof(uint64_t))
    return 0;

  if(-1 == ftruncate(info->fd, old_size))
  {
    asprintf(&JPT_last_error, "ftruncate(fd, %llu) failed during log replay: %s", (unsigned long long) old_size, strerror(errno));

    return -1;
  }

  while(!feof(info->logfile))
  {
    int command;

    command = JPT_read_uint(info->logfile);

    if(feof(info->logfile))
      break;

    if(command == JPT_OPERATOR_INSERT)
    {
      int flags, rowlen, collen, value_size;
      uint64_t timestamp;

      flags = JPT_read_uint(info->logfile);
      rowlen = JPT_read_uint(info->logfile);
      collen = JPT_read_uint(info->logfile);
      value_size = JPT_read_uint(info->logfile);
      timestamp = JPT_read_uint64(info->logfile);
      row = malloc(rowlen + 1);
      col = malloc(collen + 1);
      value = malloc(value_size);

      if(!row || !col || !value)
      {
        asprintf(&JPT_last_error, "malloc failed during log replay: %s", strerror(errno));

        goto fail;
      }

      if(rowlen != fread(row, 1, rowlen, info->logfile)
      || collen != fread(col, 1, collen, info->logfile)
      || value_size != fread(value, 1, value_size, info->logfile))
        break;

      row[rowlen] = 0;
      col[collen] = 0;

      if(-1 == JPT_insert(info, row, col, value, value_size, &timestamp, flags) && errno != EEXIST)
      {
        asprintf(&JPT_last_error, "insert failed during log replay: %s", strerror(errno));

        goto fail;
      }

      free(row); row = 0;
      free(col); col = 0;
      free(value); value = 0;
    }
    else if(command == JPT_OPERATOR_REMOVE)
    {
      int rowlen, collen;

      rowlen = JPT_read_uint(info->logfile);
      collen = JPT_read_uint(info->logfile);
      row = malloc(rowlen + 1);
      col = malloc(collen + 1);

      if(!row || !col)
        goto fail;

      if(rowlen != fread(row, 1, rowlen, info->logfile)
      || collen != fread(col, 1, collen, info->logfile))
        break;

      row[rowlen] = 0;
      col[collen] = 0;

      if(-1 == JPT_remove(info, row, col) && errno != ENOENT)
        goto fail;

      free(row); row = 0;
      free(col); col = 0;
    }
    else if(command == JPT_OPERATOR_CREATE_COLUMN)
    {
      int flags, collen;

      flags = JPT_read_uint(info->logfile);
      collen = JPT_read_uint(info->logfile);
      col = malloc(collen + 1);

      if(!col)
        goto fail;

      if(collen != fread(col, 1, collen, info->logfile))
        break;

      col[collen] = 0;

      if(JPT_get_column_idx(info, col, JPT_COL_CREATE) == (uint32_t) ~0)
        goto fail;

      free(col); col = 0;
    }
    else if(command == JPT_OPERATOR_REMOVE_COLUMN)
    {
      int flags, collen;

      flags = JPT_read_uint(info->logfile);
      collen = JPT_read_uint(info->logfile);
      col = malloc(collen + 1);

      if(!col)
        goto fail;

      if(collen != fread(col, 1, collen, info->logfile))
        break;

      col[collen] = 0;

      if(-1 == JPT_remove_column(info, col, flags) && errno != ENOENT)
        goto fail;

      free(col); col = 0;
    }
    else
    {
      asprintf(&JPT_last_error, "unexpected command %d in log file", command);

      goto fail;
    }

    last_valid = ftell(info->logfile);
  }

truncate:

  ftruncate(fileno(info->logfile), last_valid);
  rewind(info->logfile);

  result = 0;

fail:

  free(row);
  free(col);
  free(value);

  return result;
}

static int
JPT_log_reset(struct JPT_info* info)
{
  int fd;

  rewind(info->logfile);
  fd = fileno(info->logfile);

  if(-1 == ftruncate(fd, 0))
    return -1;

  fdatasync(fd);
  info->logfile_empty = 1;

  return 0;
}

static int
JPT_log_begin(struct JPT_info* info)
{
  if(!info->logfile_empty)
    return 0;

  if(-1 == JPT_write_uint64(info->logfile, info->file_size)
  || -1 == fflush(info->logfile))
    return -1;

  if(info->flags & JPT_SYNC)
    fdatasync(fileno(info->logfile));
  info->logfile_empty = 0;

  return 0;
}

int
jpt_remove(struct JPT_info* info, const char* row, const char* column)
{
  int res;

  TRACE((stderr, "jpt_remove(%p, \"%s\", \"%s\")\n", info, row, column));

  JPT_clear_error();

  JPT_writer_enter(info);

  res = JPT_remove(info, row, column);

  JPT_writer_leave(info);

  if(res != -1)
  {
    int rowlen = strlen(row);
    int collen = strlen(column);

    if(-1 == JPT_log_begin(info)
    || -1 == JPT_write_uint(info->logfile, JPT_OPERATOR_REMOVE)
    || -1 == JPT_write_uint(info->logfile, rowlen)
    || -1 == JPT_write_uint(info->logfile, collen)
    || rowlen != fwrite(row, 1, rowlen, info->logfile)
    || collen != fwrite(column, 1, collen, info->logfile)
    || -1 == fflush(info->logfile))
      return -1;

    if(info->flags & JPT_SYNC)
      fdatasync(fileno(info->logfile));
  }

  return res;
}

static int
JPT_remove_column(struct JPT_info* info, const char* column, int flags)
{
  struct JPT_disktable_cursor cursor;
  struct JPT_node** nodes = 0;
  struct JPT_node** iterator = 0;
  struct JPT_disktable* n;
  const char* c;
  uint32_t columnidx, hash;
  size_t i;
  char prefix[COLUMN_PREFIX_SIZE + 1];

  columnidx = JPT_get_column_idx(info, column, 0);

  if(columnidx == (uint32_t) ~0)
    return 0;

  JPT_generate_key(prefix, "", columnidx);

  if(info->root)
  {
    nodes = malloc(sizeof(struct JPT_node*) * info->node_count);
    iterator = nodes;

    JPT_memtable_list_column(info->root, &iterator, columnidx);

    if(iterator != nodes)
    {
      if(flags & JPT_REMOVE_IF_EMPTY)
      {
        errno = ENOTEMPTY;

        return -1;
      }

      for(i = 0; i < iterator - nodes; ++i)
      {
        struct JPT_node* n = nodes[i];
        struct JPT_node_data* d = n->next;

        while(d)
        {
          info->memtable_value_size -= d->value_size;

          d = d->next;
        }

        info->memtable_value_size -= n->value_size;
        info->memtable_key_size -= strlen(n->row) + 1;
        --info->memtable_key_count;
        --info->node_count;

        n->value = (void*) -1;
        n->next = 0;
      }
    }

    free(nodes);
  }

  n = info->first_disktable;

  memset(&cursor, 0, sizeof(cursor));

  while(n)
  {
    struct JPT_key_info key_info;
    size_t first, len, half, middle;
    size_t this_columnidx;
    unsigned char cellmeta[4];

    cursor.disktable = n;

    first = patricia_lookup_prefix(n->pat, prefix);
    len = cursor.disktable->key_info_count - first;

    if(len > 0)
    {
      if(-1 == JPT_DISKTABLE_READ_KEYINFO(cursor.disktable, &key_info, first))
        return -1;

      if(-1 == JPT_disktable_read(cursor.disktable, cellmeta, 4, key_info.offset))
        return -1;

      this_columnidx = CELLMETA_TO_COLUMN(cellmeta);

      if(this_columnidx >= columnidx)
        len = 0;

      while(len > 0)
      {
        half = len >> 1;
        middle = first + half;

        if(-1 == JPT_DISKTABLE_READ_KEYINFO(cursor.disktable, &key_info, middle))
          return -1;

        if(-1 == JPT_disktable_read(cursor.disktable, cellmeta, 4, key_info.offset))
          return -1;

        this_columnidx = CELLMETA_TO_COLUMN(cellmeta);

        if(this_columnidx < columnidx)
        {
          first = middle + 1;
          len -= half + 1;
        }
        else
          len = half;
      }
    }

    cursor.offset = first;

    while(cursor.offset < cursor.disktable->key_info_count)
    {
      if(-1 == JPT_disktable_cursor_advance(info, &cursor, (size_t) -1))
        return -1;

      if(cursor.columnidx != columnidx)
      {
        cursor.data_size = 0;

        continue;
      }

      if(cursor.columnidx > columnidx)
        break;

      if(flags & JPT_REMOVE_IF_EMPTY)
      {
        errno = ENOTEMPTY;

        return -1;
      }

      if(info->map_size)
      {
        info->map[cursor.data_offset + COLUMN_PREFIX_SIZE] = 0;
      }
      else
      {
        char zero = 0;

        if(1 != pwrite64(info->fd, &zero, 1, cursor.data_offset + COLUMN_PREFIX_SIZE))
          return -1;
      }
    }

    n = n->next;
  }

  free(cursor.buffer);

  if(-1 == JPT_remove(info, column, "__COLUMNS__"))
    return -1;

  if(-1 == JPT_remove(info, prefix, "__REV_COLUMNS__"))
    return -1;

  hash = 0;
  c = column;

  while(*c)
  {
    hash = (hash << 5) - hash + *c;
    ++c;
  }

  hash %= info->column_count;
  hash &= ~1;

  pthread_mutex_lock(&info->column_hash_mutex);

  if(info->columns[hash].name
  && !strcmp(info->columns[hash].name, column))
  {
    free(info->columns[hash].name);
    info->columns[hash].name = 0;
  }

  if(info->columns[hash + 1].name
  && !strcmp(info->columns[hash + 1].name, column))
  {
    free(info->columns[hash + 1].name);
    info->columns[hash + 1].name = 0;
  }

  pthread_mutex_unlock(&info->column_hash_mutex);

  return 0;
}

int
jpt_remove_column(struct JPT_info* info, const char* column, int flags)
{
  int result;

  TRACE((stderr, "jpt_remove_column(%p, \"%s\")\n", info, column));

  JPT_clear_error();

  JPT_writer_enter(info);

  result = JPT_remove_column(info, column, flags);

  JPT_writer_leave(info);

  if(result != -1)
  {
    int collen = strlen(column);

    if(-1 == JPT_log_begin(info)
    || -1 == JPT_write_uint(info->logfile, JPT_OPERATOR_REMOVE_COLUMN)
    || -1 == JPT_write_uint(info->logfile, flags)
    || -1 == JPT_write_uint(info->logfile, collen)
    || collen != fwrite(column, 1, collen, info->logfile)
    || -1 == fflush(info->logfile))
      return -1;

    if(info->flags & JPT_SYNC)
      fdatasync(fileno(info->logfile));
  }

  return result;
}

int
jpt_create_column(struct JPT_info* info, const char* column, int flags)
{
  JPT_writer_enter(info);

  if(JPT_get_column_idx(info, column, JPT_COL_CREATE) == (uint32_t) ~0)
  {
    JPT_writer_leave(info);

    return -1;
  }
  else
  {
    JPT_writer_leave(info);

    int collen = strlen(column);

    if(-1 == JPT_log_begin(info)
    || -1 == JPT_write_uint(info->logfile, JPT_OPERATOR_CREATE_COLUMN)
    || -1 == JPT_write_uint(info->logfile, flags)
    || -1 == JPT_write_uint(info->logfile, collen)
    || collen != fwrite(column, 1, collen, info->logfile)
    || -1 == fflush(info->logfile))
      return -1;

    if(info->flags & JPT_SYNC)
      fdatasync(fileno(info->logfile));
  }

  return 0;
}

int
jpt_has_key(struct JPT_info* info, const char* row, const char* column)
{
  int bloom_indices[4];
  struct JPT_disktable* n;
  char* key = alloca(strlen(row) + COLUMN_PREFIX_SIZE + 1);
  uint32_t columnidx;
  int result;

  TRACE((stderr, "jpt_has_key(%p, \"%s\", \"%s\")\n", info, row, column));

  JPT_reader_enter(info);

  columnidx = JPT_get_column_idx(info, column, 0);

  if(columnidx == (uint32_t) ~0)
  {
    JPT_reader_leave(info);

    return -1;
  }

  JPT_generate_key(key, row, columnidx);
  JPT_bloom_filter_indices(bloom_indices, key);

  n = info->first_disktable;

  while(n)
  {
    if(JPT_BLOOM_FILTER_TEST(n->bloom_filter, bloom_indices))
    {
      if(0 == JPT_disktable_has_key(n, row, columnidx))
      {
        JPT_reader_leave(info);

        return 0;
      }
    }

    n = n->next;
  }

  result = JPT_memtable_has_key(info, row, columnidx);

  JPT_reader_leave(info);

  return result;
}

int
jpt_has_column(struct JPT_info* info, const char* column)
{
  JPT_reader_enter(info);

  if(JPT_get_column_idx(info, column, 0) == (uint32_t) ~0)
  {
    JPT_reader_leave(info);

    return -1;
  }

  JPT_reader_leave(info);

  return 0;
}

static int
JPT_get(struct JPT_info* info, const char* row, const char* column,
        void** value, size_t* value_size, size_t* skip, size_t* max_read,
        uint64_t* timestamp)
{
  int bloom_indices[4];
  struct JPT_disktable* d;
  uint32_t columnidx;
  char* key;
  int res = -1;
  /* XXX: Improve error handling */

  assert(info->is_writing || info->reader_count);

  key = alloca(strlen(row) + COLUMN_PREFIX_SIZE + 1);

  JPT_clear_error();

  columnidx = JPT_get_column_idx(info, column, 0);

  if(columnidx == (uint32_t) ~0)
  {
    errno = ENOENT;

    return -1;
  }

  d = info->first_disktable;

  if(!max_read)
    *value = 0;

  *value_size = 0;

  JPT_generate_key(key, row, columnidx);
  JPT_bloom_filter_indices(bloom_indices, key);

  while(d)
  {
    if(JPT_BLOOM_FILTER_TEST(d->bloom_filter, bloom_indices))
    {
      if(0 == JPT_disktable_get(d, row, columnidx, value, value_size, skip, max_read, timestamp))
        res = 0;
    }

    d = d->next;
  }

  pthread_mutex_lock(&info->memtable_mutex);

  if(0 == JPT_memtable_get(info, row, columnidx, value, value_size, skip, max_read, timestamp))
    res = 0;

  pthread_mutex_unlock(&info->memtable_mutex);

  if(res == -1)
    errno = ENOENT;

  if(!max_read && *value)
    ((char*) *value)[*value_size] = 0;

  TRACE((stderr, " = \"%.*s\" (%zu bytes)\n", (int) *value_size, (const char*) *value, *value_size));

  return res;
}

int
jpt_get(struct JPT_info* info,
        const char* row, const char* column,
        void** value, size_t* value_size)
{
  int res;

  JPT_reader_enter(info);

  res = JPT_get(info, row, column, value, value_size, 0, 0, 0);

  JPT_reader_leave(info);

  return res;
}

int
jpt_get_timestamp(struct JPT_info* info,
                  const char* row, const char* column,
                  void** value, size_t* value_size, uint64_t* timestamp)
{
  int res;

  JPT_reader_enter(info);

  res = JPT_get(info, row, column, value, value_size, 0, 0, timestamp);

  JPT_reader_leave(info);

  return res;
}

int
jpt_get_fixed(struct JPT_info* info, const char* row, const char* column,
              void* value, size_t value_size)
{
  size_t size;
  size_t max_read = value_size;

  JPT_reader_enter(info);

  if(-1 == JPT_get(info, row, column, &value, &size, 0, &max_read, 0))
  {
    JPT_reader_leave(info);

    return -1;
  }

  JPT_reader_leave(info);

  return size;
}

int
jpt_scan(struct JPT_info* info, jpt_cell_callback callback, void* arg)
{
  struct JPT_node** nodes = 0;
  struct JPT_node** iterator = 0;
  struct JPT_node** nodes_end = 0;
  struct JPT_disktable* n;
  struct JPT_disktable_cursor* cursors;
  char* cat_buffer = 0;
  size_t cat_buffer_size = 0;
  size_t i;
  int ok = 0, cmp, res;

  JPT_reader_enter(info);

  TRACE((stderr, "jpt_scan(%p, %p, %p)\n", info, callback, arg));

  JPT_clear_error();

  if(info->root)
  {
    nodes = malloc(sizeof(struct JPT_node*) * info->node_count);
    iterator = nodes;

    pthread_mutex_lock(&info->memtable_mutex);

    JPT_memtable_list_all(info->root, &iterator);

    pthread_mutex_unlock(&info->memtable_mutex);

    nodes_end = iterator;
    iterator = nodes;

    while(iterator != nodes_end && (*iterator)->columnidx < 100)
      ++iterator;
  }

  cursors = calloc(info->disktable_count, sizeof(struct JPT_disktable_cursor));
  i = 0;
  n = info->first_disktable;

  while(n)
  {
    cursors[i++].disktable = n;

    n = n->next;
  }

  for(;;)
  {
    for(i = 0; i < info->disktable_count; ++i)
    {
      while(!cursors[i].data_size && cursors[i].offset < cursors[i].disktable->key_info_count)
      {
        if(-1 == JPT_disktable_cursor_advance(info, &cursors[i], (size_t) -1))
          goto fail;

        if(cursors[i].columnidx < 100)
          cursors[i].data_size = 0;
      }
    }

    const char* minrow = 0;
    size_t mincol = 0;
    size_t minidx = 0;

    /* When the same value exists in several tables, the values are
     * concatenated before they are returned.
     */
    size_t equal_count = 1;
    size_t equal_size = 0;

    for(i = 0; i < info->disktable_count; ++i)
    {
      if(!cursors[i].data_size)
        continue;

      if(!minrow)
      {
        mincol = cursors[i].columnidx;
        minrow = (char*) cursors[i].data + COLUMN_PREFIX_SIZE;
        minidx = i;
        equal_size = cursors[i].data_size - cursors[i].keylen;
      }
      else
      {
        if(cursors[i].columnidx != mincol)
          cmp = cursors[i].columnidx - mincol;
        else
          cmp = strcmp(cursors[i].data + COLUMN_PREFIX_SIZE, minrow);

        if(cmp < 0)
        {
          mincol = cursors[i].columnidx;
          minrow = (char*) cursors[i].data + COLUMN_PREFIX_SIZE;
          minidx = i;
          equal_count = 1;
          equal_size = cursors[i].data_size - cursors[i].keylen;
        }
        else if(cmp == 0)
        {
          ++equal_count;
          equal_size += cursors[i].data_size - cursors[i].keylen;
        }
      }
    }

    if(iterator != nodes_end)
    {
      if(!minrow)
      {
        mincol = (*iterator)->columnidx;
        minrow = (*iterator)->row;
        minidx = (uint32_t) ~0;
        /* No need to update equal_size since we're last and know count = 1 */
      }
      else
      {
        if((*iterator)->columnidx != mincol)
          cmp = (*iterator)->columnidx - mincol;
        else
          cmp = strcmp((*iterator)->row, minrow);

        if(cmp < 0)
        {
          mincol = (*iterator)->columnidx;
          minrow = (*iterator)->row;
          minidx = (uint32_t) ~0;
          equal_count = 1;
          /* No need to update equal_size since we're last and know count = 1 */
        }
        else if(cmp == 0)
        {
          ++equal_count;
          equal_size += (*iterator)->value_size;
        }
      }
    }

    if(!minrow)
      break;

    if(equal_count > 1)
    {
      char* o;

      if(equal_size > cat_buffer_size)
      {
        cat_buffer_size = equal_size;
        cat_buffer = realloc(cat_buffer, cat_buffer_size);
      }

      o = cat_buffer;

      for(i = 0; i < info->disktable_count && equal_count; ++i)
      {
        if(i == minidx || (!strcmp(cursors[i].data + COLUMN_PREFIX_SIZE, minrow) && cursors[i].columnidx == mincol))
        {
          memcpy(o, cursors[i].data + cursors[i].keylen, cursors[i].data_size - cursors[i].keylen);
          o += cursors[i].data_size - cursors[i].keylen;
          cursors[i].data_size = 0;
          --equal_count;
        }
      }

      if(equal_count)
      {
        assert(equal_count == 1);
        assert((*iterator)->columnidx == mincol);
        assert(!strcmp((*iterator)->row, minrow));

        memcpy(o, (*iterator)->value, (*iterator)->value_size);
        ++iterator;
      }

      res = callback(cursors[minidx].data + COLUMN_PREFIX_SIZE, JPT_get_column_name(info, mincol), cat_buffer, equal_size, &cursors[minidx].timestamp, arg);
    }
    else
    {
      if(minidx != (uint32_t) ~0)
      {
        res = callback(cursors[minidx].data + COLUMN_PREFIX_SIZE,
                       JPT_get_column_name(info, mincol),
                       cursors[minidx].data + cursors[minidx].keylen,
                       cursors[minidx].data_size - cursors[minidx].keylen,
                       &cursors[minidx].timestamp, arg);

        cursors[minidx].data_size = 0;
      }
      else
      {
        res = callback((*iterator)->row, JPT_get_column_name(info, mincol),
                       (*iterator)->value, (*iterator)->value_size,
                       &(*iterator)->timestamp, arg);

        ++iterator;
      }
    }

    switch(res)
    {
      case 1: ok = 1;
      case -1: goto fail;
    }
  }

  ok = 1;

fail:

  JPT_reader_leave(info);

  for(i = 0; i < info->disktable_count; ++i)
    free(cursors[i].buffer);

  free(cursors);
  free(cat_buffer);
  free(nodes);

  return ok ? 0 : -1;
}

int
jpt_column_scan(struct JPT_info* info, const char* column,
                jpt_cell_callback callback, void* arg)
{
  struct JPT_node** nodes = 0;
  struct JPT_node** iterator = 0;
  struct JPT_node** nodes_end = 0;
  struct JPT_disktable* n;
  struct JPT_disktable_cursor* cursors;
  char* row = 0;
  char* start_row = 0;
  char* last_row = 0;
  char* cat_buffer = 0;
  size_t cat_buffer_size = 0;
  uint32_t columnidx;
  size_t i;
  int ok = 0, cmp, res = 0;
  size_t cursor_count = 0;
  char prefix[COLUMN_PREFIX_SIZE + 1];
  size_t major_compact_count, disktable_count;

  TRACE((stderr, "jpt_column_scan(%p, %s, %p, %p)\n", info, column, callback, arg));

  JPT_clear_error();

  JPT_reader_enter(info);

  columnidx = JPT_get_column_idx(info, column, 0);

  if(columnidx == (uint32_t) -1)
  {
    errno = ENOENT;

    JPT_reader_leave(info);

    return -1;
  }

  JPT_generate_key(prefix, "", columnidx);

restart:

  disktable_count = info->disktable_count;
  major_compact_count = info->major_compact_count;

  if(info->root)
  {
    nodes = malloc(sizeof(struct JPT_node*) * info->node_count);
    iterator = nodes;

    pthread_mutex_lock(&info->memtable_mutex);

    JPT_memtable_list_column(info->root, &iterator, columnidx);

    pthread_mutex_unlock(&info->memtable_mutex);

    nodes_end = iterator;
    iterator = nodes;
  }

  cursor_count = info->disktable_count;
  cursors = calloc(cursor_count, sizeof(struct JPT_disktable_cursor));
  i = 0;
  n = info->first_disktable;

  while(n)
  {
    struct JPT_key_info last;
    char last_column[COLUMN_PREFIX_SIZE];

    if(n->key_infos_mapped)
    {
      last = n->key_infos[n->key_info_count - 1];
    }
    else
    {
      if(sizeof(struct JPT_key_info) != pread64(info->fd, &last, sizeof(struct JPT_key_info), n->key_info_offset + (n->key_info_count - 1) * sizeof(struct JPT_key_info)))
        goto fail;
    }

    if(info->map_size)
    {
      memcpy(last_column, info->map + last.offset + n->offset, COLUMN_PREFIX_SIZE);
    }
    else
    {
      if(COLUMN_PREFIX_SIZE != pread64(info->fd, last_column, COLUMN_PREFIX_SIZE, last.offset + n->offset))
        goto fail;
    }

    if(memcmp(prefix, last_column, COLUMN_PREFIX_SIZE) <= 0)
    {
      struct JPT_key_info key_info;
      struct JPT_disktable_cursor* cursor;
      size_t first, len, half, middle;
      size_t this_columnidx;
      unsigned char cellmeta[4];

      cursor = &cursors[i];

      cursor->disktable = n;

      first = patricia_lookup_prefix(n->pat, prefix);
      len = cursor->disktable->key_info_count - first;

      if(len > 0)
      {
        if(-1 == JPT_DISKTABLE_READ_KEYINFO(cursor->disktable, &key_info, first))
          goto fail;

        if(-1 == JPT_disktable_read(cursor->disktable, cellmeta, 4, key_info.offset))
          goto fail;

        this_columnidx = CELLMETA_TO_COLUMN(cellmeta);

        if(this_columnidx >= columnidx)
        {
          len = 0;

          if(this_columnidx > columnidx)
            first = cursor->disktable->key_info_count;
        }

        while(len > 0)
        {
          half = len >> 1;
          middle = first + half;

          if(-1 == JPT_DISKTABLE_READ_KEYINFO(cursor->disktable, &key_info, middle))
            goto fail;

          if(-1 == JPT_disktable_read(cursor->disktable, cellmeta, 4, key_info.offset))
            goto fail;

          this_columnidx = CELLMETA_TO_COLUMN(cellmeta);

          if(this_columnidx < columnidx)
          {
            first = middle + 1;
            len -= half + 1;
          }
          else
            len = half;
        }
      }

      cursor->offset = first;

      if(cursor->offset < cursor->disktable->key_info_count)
        ++i;
    }

    n = n->next;
  }

  cursor_count = i;

  for(;;)
  {
    if(disktable_count != info->disktable_count
    || major_compact_count != info->major_compact_count)
    {
      if(!start_row)
        start_row = strdup(last_row);

      for(i = 0; i < cursor_count; ++i)
        free(cursors[i].buffer);

      free(cursors);
      free(nodes);

      cursors = 0;
      cursor_count = 0;
      nodes = 0;
      iterator = 0;
      nodes_end = 0;

      goto restart;
    }

    i = 0;

    while(i < cursor_count)
    {
      struct JPT_disktable_cursor* cursor;

      cursor = &cursors[i];

      if(!cursor->data_size)
      {
        if(cursor->offset < cursor->disktable->key_info_count)
        {
          if(-1 == JPT_disktable_cursor_advance(info, cursor, columnidx))
            goto fail;
        }

        if(!cursor->data_size && cursor->offset == cursor->disktable->key_info_count)
        {
          free(cursor->buffer);

          memmove(cursors + i, cursors + i + 1, sizeof(struct JPT_disktable_cursor) * (cursor_count - i - 1));
          --cursor_count;

          continue;
        }
      }

      ++i;
    }

    const char* min = 0;
    size_t minidx = 0;

    /* When the same value exists in several tables, the values are
     * concatenated before they are returned.
     */
    size_t equal_count = 1;
    size_t equal_size = 0;
    size_t keylen = 0;

    for(i = 0; i < cursor_count; ++i)
    {
      if(!min)
      {
        min = cursors[i].data + COLUMN_PREFIX_SIZE;
        minidx = i;
        equal_size = cursors[i].data_size - cursors[i].keylen;
        keylen = cursors[i].keylen - COLUMN_PREFIX_SIZE;
      }
      else
      {
        cmp = strcmp(cursors[i].data + COLUMN_PREFIX_SIZE, min);

        if(cmp < 0)
        {
          min = cursors[i].data + COLUMN_PREFIX_SIZE;
          minidx = i;
          equal_count = 1;
          equal_size = cursors[i].data_size - cursors[i].keylen;
          keylen = cursors[i].keylen - COLUMN_PREFIX_SIZE;
        }
        else if(cmp == 0)
        {
          ++equal_count;
          equal_size += cursors[i].data_size - cursors[i].keylen;
        }
      }
    }

    if(iterator != nodes_end)
    {
      if(!min)
      {
        min = (*iterator)->row;
        minidx = (uint32_t) ~0;
        keylen = strlen((*iterator)->row) + 1;
        equal_size = (*iterator)->value_size;
      }
      else
      {
        cmp = strcmp((*iterator)->row, min);

        if(cmp < 0)
        {
          min = (*iterator)->row;
          minidx = (uint32_t) ~0;
          equal_count = 1;
          keylen = strlen((*iterator)->row) + 1;
          equal_size = (*iterator)->value_size;
        }
        else if(cmp == 0)
        {
          ++equal_count;
          equal_size += (*iterator)->value_size;
        }
      }
    }

    if(!min)
      break;

    if(equal_size + keylen > cat_buffer_size)
    {
      cat_buffer_size = equal_size + keylen;
      cat_buffer = realloc(cat_buffer, cat_buffer_size);
    }

    if(equal_count > 1)
    {
      char* o;

      o = cat_buffer;

      for(i = 0; i < cursor_count && equal_count; ++i)
      {
        if(i == minidx || (cursors[i].data_size && !strcmp(cursors[i].data + COLUMN_PREFIX_SIZE, min)))
        {
          assert(cursors[i].data_size >= cursors[i].keylen);

          memcpy(o, cursors[i].data + cursors[i].keylen, cursors[i].data_size - cursors[i].keylen);
          o += cursors[i].data_size - cursors[i].keylen;
          cursors[i].data_size = 0;
          --equal_count;
        }
      }

      if(equal_count)
      {
        assert(equal_count == 1);
        assert(!strcmp((*iterator)->row, min));

        memcpy(o, (*iterator)->value, (*iterator)->value_size);
        o += (*iterator)->value_size;
        ++iterator;
      }

      row = o;
      strcpy(row, cursors[minidx].data + COLUMN_PREFIX_SIZE);

      JPT_reader_leave(info);

      if(start_row)
      {
        if(0 > strcmp(start_row, row))
        {
          free(start_row);

          start_row = 0;
        }
      }

      if(!start_row)
        res = callback(last_row = row, column, cat_buffer, equal_size, &cursors[minidx].timestamp, arg);

      JPT_reader_enter(info);
    }
    else
    {
      if(minidx != (uint32_t) ~0)
      {
        memcpy(cat_buffer, cursors[minidx].data + cursors[minidx].keylen, equal_size);
        row = cat_buffer + equal_size;
        strcpy(row, cursors[minidx].data + COLUMN_PREFIX_SIZE);

        JPT_reader_leave(info);

        if(start_row)
        {
          if(0 > strcmp(start_row, row))
          {
            free(start_row);

            start_row = 0;
          }
        }

        if(!start_row)
        {
          res = callback(last_row = row, column, cat_buffer,
                         cursors[minidx].data_size - cursors[minidx].keylen,
                         &cursors[minidx].timestamp, arg);
        }

        JPT_reader_enter(info);

        cursors[minidx].data_size = 0;
      }
      else
      {
        uint64_t timestamp;
        size_t size;

        size = (*iterator)->value_size;

        memcpy(cat_buffer, (*iterator)->value, size);
        memcpy(cat_buffer + size, (*iterator)->row, keylen);
        timestamp = (*iterator)->timestamp;

        row = cat_buffer + size;

        JPT_reader_leave(info);

        if(start_row)
        {
          if(0 > strcmp(start_row, row))
          {
            free(start_row);

            start_row = 0;
          }
        }

        if(!start_row)
          res = callback(last_row = row, column, cat_buffer, size, &timestamp, arg);

        JPT_reader_enter(info);

        ++iterator;
      }
    }

    switch(res)
    {
      case 1: ok = 1;
      case -1: goto fail;
    }
  }

  ok = 1;

fail:

  JPT_reader_leave(info);

  for(i = 0; i < cursor_count; ++i)
    free(cursors[i].buffer);

  free(cursors);
  free(cat_buffer);
  free(nodes);
  free(start_row);

  return ok ? 0 : -1;
}

uint64_t
jpt_get_counter(struct JPT_info* info, const char* name)
{
  uint64_t result;
  uint64_t next;
  int first = 0;
  unsigned char buf[8];

  if(-1 == jpt_get_fixed(info, name, "__COUNTERS__", buf, 8))
  {
    if(errno != ENOENT)
      return (uint64_t) ~0ULL;

    memset(buf, 0, sizeof(buf));
    first = 1;
  }

  result = ((uint64_t) buf[0] << 56ULL) | ((uint64_t) buf[1] << 48ULL)
         | ((uint64_t) buf[2] << 40ULL) | ((uint64_t) buf[3] << 32ULL)
         | ((uint64_t) buf[4] << 24ULL) | ((uint64_t) buf[5] << 16ULL)
         | ((uint64_t) buf[6] << 8ULL)  | (uint64_t) buf[7];

  next = result + 1;

  buf[0] = next >> 56ULL; buf[1] = next >> 48ULL;
  buf[2] = next >> 40ULL; buf[3] = next >> 32ULL;
  buf[4] = next >> 24ULL; buf[5] = next >> 16ULL;
  buf[6] = next >> 8ULL; buf[7] = next;

  if(-1 == jpt_insert(info, name, "__COUNTERS__", buf, 8, JPT_REPLACE))
    return (uint64_t) ~0ULL;

  return result;
}

void
jpt_close(struct JPT_info* info)
{
  size_t i;

  TRACE((stderr, "jpt_close(%p)\n", info));

  JPT_clear_error();

  JPT_writer_enter(info);

  JPT_free_disktables(info);

  close(info->fd);
  fclose(info->logfile);

  if(info->map_size)
    munmap(info->map, info->map_size);

  for(i = 0; i < info->column_count; ++i)
    free(info->columns[i].name);

  free(info->columns);
  free(info->buffer);
  free(info->filename);
  free(info);
}
