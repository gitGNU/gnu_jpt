/*  Functions for jpt's on-disk tables.
    Copyright (C) 2007, 2008, 2009  Morten Hustveit <morten@rashbox.org>

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

#include <alloca.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

#include "patricia.h"

#include "jpt_internal.h"

int
JPT_disktable_read_keyinfo(struct JPT_disktable* disktable, struct JPT_key_info* target, size_t keyidx)
{
  ssize_t res;

  if(disktable->key_infos_mapped)
  {
    memcpy(target, disktable->key_infos + keyidx, sizeof(struct JPT_key_info));

    return 0;
  }

  res = pread64(disktable->info->fd, target, sizeof(struct JPT_key_info),
                disktable->key_info_offset + keyidx * sizeof(struct JPT_key_info));

  if(res == -1)
    return -1;

  if(res != sizeof(struct JPT_key_info))
  {
    asprintf(&JPT_last_error, "Short read: Got %zu bytes, expected %zu", res, sizeof(struct JPT_key_info));

    return -1;
  }

  return 0;
}

int
JPT_disktable_write_keyinfo(struct JPT_disktable* disktable, const struct JPT_key_info* source, size_t keyidx)
{
  ssize_t res;

  if(disktable->key_infos_mapped)
  {
    memcpy(disktable->key_infos + keyidx, source, sizeof(struct JPT_key_info));

    return 0;
  }

  res = pwrite64(disktable->info->fd, source, sizeof(struct JPT_key_info),
                 disktable->key_info_offset + keyidx * sizeof(struct JPT_key_info));

  if(res == -1)
    return -1;

  if(res != sizeof(struct JPT_key_info))
  {
    asprintf(&JPT_last_error, "Short write: Wrote %zu bytes, wanted to write %zu", res, sizeof(struct JPT_key_info));

    return -1;
  }

  return 0;
}


int
JPT_disktable_read(struct JPT_disktable* disktable, void* target, size_t size, size_t offset)
{
  size_t fdoffset = offset + disktable->offset;
  int res;

  if(disktable->info->map_size)
  {
    memcpy(target, disktable->info->map + fdoffset, 4);

    return 0;
  }

  res = pread64(disktable->info->fd, target, size, fdoffset);

  if(res != size)
    return -1;

  return 0;
}

int
JPT_disktable_has_key(struct JPT_disktable* disktable,
                      const char* row, uint32_t columnidx)
{
  struct JPT_key_info key_info;
  char* key_buf;
  size_t key_size;
  unsigned int idx;

  key_size = strlen(row) + COLUMN_PREFIX_SIZE + 1;
  key_buf = alloca(key_size);

  JPT_generate_key(key_buf, row, columnidx);

  idx = patricia_lookup(disktable->pat, key_buf);

  if(idx >= disktable->key_info_count)
    return -1;

  if(-1 == JPT_DISKTABLE_READ_KEYINFO(disktable, &key_info, idx))
    return -1;

  if(key_info.size < key_size || (key_info.flags & JPT_KEY_REMOVED))
    return -1;

  if(disktable->info->map_size)
  {
    if(!memcmp(disktable->info->map + disktable->offset + key_info.offset, key_buf, key_size))
      return 0;

    return -1;
  }
  else
  {
    char* cmp_buf;
    cmp_buf = alloca(key_size);

    if(key_size != pread64(disktable->info->fd, cmp_buf, key_size, disktable->offset + key_info.offset))
      return -1;

    if(!memcmp(cmp_buf, key_buf, key_size))
      return 0;
  }

  return -1;
}

int
JPT_disktable_remove(struct JPT_disktable* disktable,
                     const char* row, uint32_t columnidx)
{
  struct JPT_key_info key_info;
  struct JPT_info* info = disktable->info;
  char* key_buf;
  char* cmp_buf;
  size_t key_size;
  unsigned int idx;

  key_size = strlen(row) + COLUMN_PREFIX_SIZE + 1;
  key_buf = alloca(key_size);
  cmp_buf = alloca(key_size);

  JPT_generate_key(key_buf, row, columnidx);

  idx = patricia_lookup(disktable->pat, key_buf);

  if(idx >= disktable->key_info_count)
  {
    errno = ENOENT;

    return -1;
  }

  if(-1 == JPT_DISKTABLE_READ_KEYINFO(disktable, &key_info, idx))
    return -1;

  if((key_info.flags & JPT_KEY_REMOVED)
  || key_info.size < key_size)
  {
    errno = ENOENT;

    return -1;
  }

  if(info->map_size)
  {
    if(memcmp(info->map + disktable->offset + key_info.offset, key_buf, key_size))
    {
      errno = ENOENT;

      return -1;
    }
  }
  else
  {
    if(key_size != pread64(info->fd, cmp_buf, key_size, disktable->offset + key_info.offset))
      return -1;

    if(memcmp(cmp_buf, key_buf, key_size))
    {
      errno = ENOENT;

      return -1;
    }
  }

  key_info.flags |= JPT_KEY_REMOVED;

  if(-1 == JPT_DISKTABLE_WRITE_KEYINFO(disktable, &key_info, idx))
    return -1;

  return 0;
}

ssize_t
JPT_disktable_overwrite(struct JPT_disktable* disktable,
                        const char* row, uint32_t columnidx,
                        const void* value, size_t amount)
{
  struct JPT_key_info key_info;
  struct JPT_info* info = disktable->info;
  char* key_buf;
  char* cmp_buf;
  size_t key_size;
  unsigned int idx;
  size_t size;
  off_t offset;

  key_size = strlen(row) + COLUMN_PREFIX_SIZE + 1;
  key_buf = alloca(key_size);
  cmp_buf = alloca(key_size);

  JPT_generate_key(key_buf, row, columnidx);

  idx = patricia_lookup(disktable->pat, key_buf);

  if(idx >= disktable->key_info_count)
  {
    errno = ENOENT;

    return -1;
  }

  if(-1 == JPT_DISKTABLE_READ_KEYINFO(disktable, &key_info, idx))
    return -1;

  size = key_info.size;
  offset = key_info.offset;

  if(size < key_size)
  {
    errno = ENOENT;

    return -1;
  }

  size -= key_size;

  if(info->map_size)
  {
    if(memcmp(info->map + disktable->offset + offset, key_buf, key_size))
    {
      errno = ENOENT;

      return -1;
    }

    if(size > amount)
    {
      size = amount;

      key_info.size = key_size + size;
    }

    memcpy(info->map + disktable->offset + offset + key_size,
           value, size);
  }
  else
  {
    if(key_size != pread64(info->fd, cmp_buf, key_size, disktable->offset + offset))
      return -1;

    if(memcmp(cmp_buf, key_buf, key_size))
    {
      errno = ENOENT;

      return -1;
    }

    if(size > amount)
    {
      size = amount;
      key_info.size = key_size + size;
    }

    if(-1 == pwrite64(info->fd, value, size, disktable->offset + offset + key_size))
      return -1;
  }

  key_info.flags &= ~JPT_KEY_REMOVED;

  if(-1 == JPT_DISKTABLE_WRITE_KEYINFO(disktable, &key_info, idx))
      return -1;

  return size;
}

int
JPT_disktable_get(struct JPT_disktable* disktable,
                  const char* row, uint32_t columnidx,
                  void** value, size_t* value_size,
                  size_t* skip, size_t* max_read,
                  uint64_t* timestamp)
{
  struct JPT_key_info key_info;
  struct JPT_info* info = disktable->info;
  char* key_buf;
  char* cmp_buf;
  size_t key_size;
  size_t size;
  unsigned int idx;

  key_size = strlen(row) + COLUMN_PREFIX_SIZE + 1;
  key_buf = alloca(key_size);

  JPT_generate_key(key_buf, row, columnidx);

  idx = patricia_lookup(disktable->pat, key_buf);

  if(idx >= disktable->key_info_count)
    return -1;

  if(-1 == JPT_DISKTABLE_READ_KEYINFO(disktable, &key_info, idx))
    return -1;

  size = key_info.size;

  if(size < key_size || (key_info.flags & JPT_KEY_REMOVED))
  {
    errno = ENOENT;

    return -1;
  }

  if(info->map_size)
  {
    cmp_buf = info->map + disktable->offset + key_info.offset;
  }
  else
  {
    cmp_buf = alloca(key_size);

    if(key_size != pread64(info->fd, cmp_buf, key_size, disktable->offset + key_info.offset))
      return -1;
  }

  if(!memcmp(cmp_buf, key_buf, key_size))
  {
    size_t old_size = *value_size;
    size -= key_size;

    *value_size += size;

    if(max_read)
    {
      if(*value_size > *max_read)
        *value_size = *max_read;
    }
    else
      *value = realloc(*value, *value_size + 1);

    if(info->map_size)
    {
      memcpy(*value + old_size, info->map + disktable->offset + key_info.offset + key_size, size);
    }
    else
    {
      if(size != pread64(info->fd, *value + old_size, size, disktable->offset + key_info.offset + key_size))
      {
        *value_size = old_size;

        return -1;
      }
    }

    if(timestamp)
      *timestamp = key_info.timestamp;

    return 0;
  }

  errno = ENOENT;

  return -1;
}

int
JPT_disktable_cursor_advance(struct JPT_info* info,
                             struct JPT_disktable_cursor* cursor,
                             uint32_t columnidx)
{
  struct JPT_key_info key_info;
  unsigned char* cellmeta;

  do
  {
repeat:

    if(cursor->offset == cursor->disktable->key_info_count)
    {
      cursor->data_size = 0;

      return 0;
    }

    if(-1 == JPT_DISKTABLE_READ_KEYINFO(cursor->disktable, &key_info, cursor->offset++))
      return -1;

    if((key_info.flags & JPT_KEY_REMOVED) && !(key_info.flags & JPT_KEY_NEW_COLUMN))
      goto repeat;

    cursor->data_offset = key_info.offset + cursor->disktable->offset;
    cursor->data_size = key_info.size;

    if(info->map_size)
    {
      cursor->data = info->map + cursor->data_offset;
    }
    else
    {
      if(cursor->data_alloc < key_info.size)
      {
        cursor->data_alloc = (key_info.size + 1023) & ~1023;
        cursor->buffer = realloc(cursor->data, cursor->data_alloc);
      }

      cursor->data = cursor->buffer;

      if(key_info.size != pread64(info->fd, cursor->data, key_info.size, cursor->data_offset))
        return -1;
    }

    cellmeta = (unsigned char*) cursor->data;
    cursor->columnidx = CELLMETA_TO_COLUMN(cellmeta);

    if(columnidx != JPT_INVALID_COLUMN && cursor->columnidx != columnidx)
    {
      if(cursor->columnidx > columnidx)
      {
        cursor->offset = cursor->disktable->key_info_count;
        cursor->data_size = 0;
        cursor->data_offset = 0;

        return 0;
      }

      goto repeat;
    }

    cursor->timestamp = key_info.timestamp;
    cursor->keylen = strlen(cursor->data) + 1;
    cursor->flags = key_info.flags;
  }
  while(!cellmeta[COLUMN_PREFIX_SIZE] || (key_info.flags & JPT_KEY_REMOVED));

  return 0;
}
