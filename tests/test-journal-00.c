#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "jpt.h"
#include "jpt_internal.h"

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
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", "abcde", 5, JPT_REPLACE));

  /* Close file descriptors without giving library a chance to record data */
  close(db->fd);
  close(db->logfd);

  WANT_POINTER(db = jpt_init("test-db.tab", 1024 * 1024, 0))
  WANT_SUCCESS(jpt_get(db, "row1", "col1", &ret, &retsize));
  WANT_TRUE(retsize == 5);
  free(ret);
  jpt_close(db);

  WANT_SUCCESS(unlink("test-db.tab"));
  WANT_SUCCESS(unlink("test-db.tab.log"));

  fprintf(stderr, "* passed all %zu test%s\n", test_count, (test_count != 1) ? "s" : "");

  return EXIT_SUCCESS;
}
