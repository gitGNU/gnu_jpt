/*  I/O functions for jpt.
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

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include "jpt_internal.h"

int
JPT_write_uint(FILE* f, unsigned int integer)
{
  if(integer > 0xfffffff)
  {
    if(EOF == fputc(0x80 | ((integer >> 28) & 0x7f), f))
      return -1;
  }

  if(integer > 0x1fffff)
  {
    if(EOF == fputc(0x80 | ((integer >> 21) & 0x7f), f))
      return -1;
  }

  if(integer > 0x3fff)
  {
    if(EOF == fputc(0x80 | ((integer >> 14) & 0x7f), f))
      return -1;
  }

  if(integer > 0x7f)
  {
    if(EOF == fputc(0x80 | ((integer >> 7) & 0x7f), f))
      return -1;
  }

  if(EOF == fputc(integer & 0x7f, f))
    return -1;

  return 0;
}

int
JPT_write_uint64(FILE* f, uint64_t val)
{
  int i;

  for(i = 0; i < 8; ++i)
  {
    if(EOF == fputc(val >> ((7 - i) * 8), f))
      return -1;
  }

  return 0;
}

uint64_t
JPT_read_uint(FILE* f)
{
  uint64_t result = 0;
  int c;

  while(EOF != (c = fgetc(f)))
  {
    result <<= 7;
    result |= (c & 0x7f);

    if(!(c & 0x80))
      break;
  }

  return result;
}

uint64_t
JPT_read_uint64(FILE* f)
{
  uint64_t result = 0;
  int i;

  for(i = 0; i < 8; ++i)
  {
    result <<= 8ULL;
    result |= (uint64_t) fgetc(f);
  }

  return result;
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
    ssize_t res;

    res = write(fd, o, remaining);

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

ssize_t
JPT_writev(int fd, const struct iovec *iov, int iovcnt)
{
  size_t size = 0;
  ssize_t ret;
  int i;

  for(i = 0; i < iovcnt; ++i)
    size += iov[i].iov_len;

  ret = writev(fd, iov, iovcnt);

  if(ret == -1)
  {
    asprintf(&JPT_last_error, "Vector write failed: %s", strerror(errno));

    return -1;
  }

  if(ret < size)
  {
    asprintf(&JPT_last_error, "Tried to write %zu bytes, but writev returned after %zu", size, ret);

    return -1;
  }

  return 0;
}
