/*  General tests for jpt.
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

int
main(int argc, char** argv)
{
  struct JPT_info* db;
  void* ret;
  size_t retsize;

  WANT_TRUE(0 == unlink("test-db.tab") || errno == ENOENT);
  WANT_TRUE(0 == unlink("test-db.tab.log") || errno == ENOENT);

  WANT_POINTER(db = jpt_init("test-db.tab", 1024 * 1024, 0))
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", "1234567890", 10, 0));
  WANT_SUCCESS(jpt_compact(db));
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", "abcdefghijklmnopqrst", 20, JPT_REPLACE));
  WANT_SUCCESS(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_TRUE(retsize == 20);
  free(ret);
  jpt_close(db);

  WANT_TRUE(0 == unlink("test-db.tab") || errno == ENOENT);
  WANT_TRUE(0 == unlink("test-db.tab.log") || errno == ENOENT);

  WANT_POINTER(db = jpt_init("test-db.tab", 1024 * 1024, 0))
  WANT_SUCCESS(jpt_insert(db, "MFXKDBSQMOXZBCBBLQHRCWD", "SGSNNIZFVUBQKPXKLCPHHOZRTIH", "ZQGKX", 5, JPT_APPEND));
  WANT_SUCCESS(jpt_compact(db));
  WANT_SUCCESS(jpt_insert(db, "MFXKDBSQMOXZBCBBLQHRCWD", "SGSNNIZFVUBQKPXKLCPHHOZRTIH", "UGJLBY", 6, JPT_REPLACE));
  WANT_SUCCESS(jpt_compact(db));
  WANT_SUCCESS(jpt_get(db, "MFXKDBSQMOXZBCBBLQHRCWD", "SGSNNIZFVUBQKPXKLCPHHOZRTIH", &ret, &retsize));
  WANT_TRUE(retsize == 6);
  free(ret);
  jpt_close(db);

  WANT_TRUE(0 == unlink("test-db.tab") || errno == ENOENT);
  WANT_TRUE(0 == unlink("test-db.tab.log") || errno == ENOENT);

  WANT_POINTER(db = jpt_init("test-db.tab", 1024 * 1024, 0));
  WANT_SUCCESS(jpt_insert(db, "eple", "eple", "eple", 4, JPT_REPLACE));
  WANT_SUCCESS(jpt_insert(db, "eple", "eple", "hest", 4, JPT_APPEND));
  WANT_SUCCESS(jpt_insert(db, "eple", "eple", "hest", 4, JPT_REPLACE));
  WANT_SUCCESS(jpt_compact(db));
  WANT_FAILURE(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_FAILURE(jpt_get(db, "!", "!", &ret, &retsize));
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", "a", 1, JPT_APPEND));
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", "b", 1, JPT_APPEND));
  WANT_FAILURE(jpt_insert(db, "row1", "col1", "x", 1, 0));
  WANT_SUCCESS(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_TRUE(retsize == 2);
  WANT_TRUE(!memcmp(ret, "ab", 1));
  free(ret);
  WANT_SUCCESS(jpt_get(db, "row1", "col1", &ret, &retsize));
  free(ret);
  WANT_SUCCESS(jpt_remove(db, "row1", "col1"));
  WANT_FAILURE(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", "c", 1, JPT_APPEND));
  WANT_SUCCESS(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_TRUE(retsize == 1);
  WANT_TRUE(!memcmp(ret, "c", 1));
  free(ret);
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", "d", 1, JPT_APPEND));
  WANT_SUCCESS(jpt_insert(db, "row2", "col2", "e", 1, JPT_APPEND));
  WANT_SUCCESS(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_TRUE(retsize == 2);
  WANT_TRUE(!memcmp(ret, "cd", 1));
  free(ret);
  WANT_SUCCESS(jpt_compact(db));
  WANT_FAILURE(jpt_insert(db, "row1", "col1", "x", 1, 0));
  WANT_SUCCESS(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_TRUE(retsize == 2);
  WANT_TRUE(!memcmp(ret, "cd", 1));
  free(ret);
  WANT_SUCCESS(jpt_remove(db, "row1", "col1"));
  WANT_FAILURE(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_SUCCESS(jpt_compact(db));
  WANT_FAILURE(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_SUCCESS(jpt_major_compact(db));
  jpt_close(db);

  WANT_POINTER(db = jpt_init("test-db.tab", 1024 * 1024, 0))
  WANT_FAILURE(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_SUCCESS(jpt_get(db, "row2", "col2", &ret, &retsize));
  WANT_TRUE(retsize == 1);
  WANT_TRUE(!memcmp(ret, "e", 1));
  free(ret);
  jpt_close(db);

  WANT_SUCCESS(unlink("test-db.tab"));
  WANT_SUCCESS(unlink("test-db.tab.log"));

  fprintf(stderr, "* passed all %zu test%s\n", test_count, (test_count != 1) ? "s" : "");

  return EXIT_SUCCESS;
}
