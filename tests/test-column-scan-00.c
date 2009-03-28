/*  Test-case for column scan in jpt.
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

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "jpt.h"

#include "common.h"

static struct JPT_info* db;
static size_t count;

static int
cell_callback(const char* row, const char* column, const void* data,
              size_t data_size, uint64_t* timestamp, void* arg)
{
  char buf[64];

  WANT_TRUE(!strcmp(column, "column"));
  WANT_TRUE(data_size == strlen(row));
  WANT_TRUE(0 == memcmp(row, data, data_size));

  sprintf(buf, "%08zu", count++);

  WANT_TRUE(0 == strcmp(row, buf));

  if(count == 1000)
    jpt_major_compact(db);

  return 0;
}

int
main(int argc, char** argv)
{
  void* ret;
  size_t retsize;
  char buf[64];
  size_t i;

  WANT_TRUE(0 == unlink("test-db.backup") || errno == ENOENT);
  WANT_TRUE(0 == unlink("test-db.tab") || errno == ENOENT);
  WANT_TRUE(0 == unlink("test-db.tab.log") || errno == ENOENT);

  WANT_POINTER(db = jpt_init("test-db.tab", 128 * 1024, 0));

  for(i = 0; i <= 0x7FFF; ++i)
  {
    sprintf(buf, "%08zu", i ^ 0x5AAA);

    WANT_SUCCESS(jpt_insert(db, buf, "column", buf, strlen(buf), 0));
  }

  WANT_SUCCESS(jpt_column_scan(db, "column", cell_callback, 0));

  WANT_TRUE(count == i);

  WANT_SUCCESS(unlink("test-db.tab"));
  WANT_SUCCESS(unlink("test-db.tab.log"));

  fprintf(stderr, "* passed all %zu test%s\n", test_count, (test_count != 1) ? "s" : "");

  return EXIT_SUCCESS;
}
