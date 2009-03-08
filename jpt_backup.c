/*  Backup functions for jpt.
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "jpt_internal.h"

struct JPT_backup_arg
{
  uint64_t mintime;
  FILE* f;
};

static int
write_callback(const char* row, const char* column, const void* data, size_t data_size, uint64_t* timestamp, void* varg)
{
  struct JPT_backup_arg* arg = varg;
  FILE* f = arg->f;
  size_t rowlen = strlen(row);
  size_t collen = strlen(column);

  if(*timestamp < arg->mintime)
    return 0;

  if(-1 == JPT_write_uint(f, rowlen))
    return -1;

  if(-1 == JPT_write_uint(f, collen))
    return -1;

  if(-1 == JPT_write_uint(f, data_size))
    return -1;

  if(-1 == JPT_write_uint64(f, *timestamp))
    return -1;

  if(rowlen != fwrite(row, 1, rowlen, f))
    return -1;

  if(collen != fwrite(column, 1, collen, f))
    return -1;

  if(data_size != fwrite(data, 1, data_size, f))
    return -1;

  return 0;
}

int
jpt_backup(struct JPT_info* info, const char* filename, const char* column, uint64_t mintime)
{
  struct JPT_backup_arg arg;
  FILE* f;
  const char signature[11] = { 0, 0, 0, 'J', 'P', 'T', 'B', '0', '0', '0', '0' };

  if(!strcmp(filename, "-"))
  {
    f = stdout;
  }
  else
  {
    f = fopen(filename, "w");

    if(!f)
      return -1;
  }

  arg.mintime = mintime;
  arg.f = f;

  if(sizeof(signature) != fwrite(signature, 1, sizeof(signature), f))
    return -1;

  if(!column)
  {
    if(-1 == jpt_scan(info, write_callback, &arg))
    {
      fclose(f);
      unlink(filename);

      return -1;
    }
  }
  else
  {
    if(-1 == jpt_column_scan(info, column, write_callback, &arg))
    {
      fclose(f);
      unlink(filename);

      return -1;
    }
  }

  fclose(f);

  return 0;
}

int
jpt_restore(struct JPT_info* info, const char* filename, int flags)
{
  uint64_t timestamp;
  FILE* f;
  char* row = 0;
  char* column = 0;
  char* value = 0;
  size_t row_alloc = 0;
  size_t column_alloc = 0;
  size_t value_alloc = 0;
  size_t row_size = 0;
  size_t column_size = 0;
  size_t value_size = 0;
  int ok = 0;
  int version = -1;

  timestamp = jpt_gettime();

  if(!strcmp(filename, "-"))
  {
    f = stdin;
  }
  else
  {
    f = fopen(filename, "r");

    if(!f)
      return -1;
  }

  while(!feof(f))
  {
    row_size = JPT_read_uint(f);
    column_size = JPT_read_uint(f);
    value_size = JPT_read_uint(f);

    if(!row_size || !column_size)
    {
      if(!row_size)
      {
        char tmp[8];
        fread(tmp, 1, 8, f);

        if(!memcmp(tmp, "JPTB0000", 8))
        {
          version = 0;

          continue;
        }
      }

      break;
    }

    if(row_size > row_alloc)
    {
      row_alloc = (row_size + 4095) & ~4095;
      free(row);
      row = malloc(row_alloc);

      if(!row)
        goto fail;
    }

    if(column_size > column_alloc)
    {
      column_alloc = (column_size + 4095) & ~4095;
      free(column);
      column = malloc(column_alloc);

      if(!column)
        goto fail;
    }

    if(value_size > value_alloc)
    {
      value_alloc = (value_size + 4095) & ~4095;
      free(value);
      value = malloc(value_alloc);

      if(!value)
        goto fail;
    }

    if(version >= 0)
      timestamp = JPT_read_uint64(f);

    if(row_size != fread(row, 1, row_size, f))
      goto fail;

    if(column_size != fread(column, 1, column_size, f))
      goto fail;

    if(value_size != fread(value, 1, value_size, f))
      goto fail;

    row[row_size] = 0;
    column[column_size] = 0;

    if(-1 == jpt_insert_timestamp(info, row, column, value, value_size, &timestamp, flags))
      goto fail;
  }

  ok = 1;

fail:

  free(row);
  free(column);
  free(value);

  if(f != stdin)
    fclose(f);

  return ok ? 0 : -1;
}
