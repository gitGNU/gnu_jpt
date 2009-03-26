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

  /* Create a table, insert a value, backup, remove table, restore */

  WANT_TRUE(0 == unlink("test-db.backup") || errno == ENOENT);
  WANT_TRUE(0 == unlink("test-db.tab") || errno == ENOENT);
  WANT_TRUE(0 == unlink("test-db.tab.log") || errno == ENOENT);

  WANT_POINTER(db = jpt_init("test-db.tab", 1024 * 1024, 0))
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", "1234567890", 10, 0));
  WANT_SUCCESS(jpt_backup(db, "test-db.backup", 0, 0));
  jpt_close(db);

  WANT_SUCCESS(unlink("test-db.tab"));
  WANT_SUCCESS(unlink("test-db.tab.log"));

  WANT_POINTER(db = jpt_init("test-db.tab", 1024 * 1024, 0))
  WANT_SUCCESS(jpt_restore(db, "test-db.backup", 0));
  WANT_SUCCESS(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_TRUE(retsize == 10);
  WANT_TRUE(!memcmp(ret, "1234567890", 1));
  free(ret);

  WANT_SUCCESS(unlink("test-db.tab"));
  WANT_SUCCESS(unlink("test-db.tab.log"));
  WANT_SUCCESS(unlink("test-db.backup"));

  fprintf(stderr, "* passed all %zu test%s\n", test_count, (test_count != 1) ? "s" : "");

  return EXIT_SUCCESS;
}
