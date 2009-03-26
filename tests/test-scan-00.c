#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "jpt.h"

#include "common.h"

static size_t count;

static int
cell_callback(const char* row, const char* column, const void* data,
              size_t data_size, uint64_t* timestamp, void* arg)
{
  char buf[64];

  WANT_TRUE(data_size == strlen(column));
  WANT_TRUE(0 == memcmp(column, data, data_size));
  WANT_TRUE(0 == memcmp(row, data, data_size));

  ++count;

  return 0;
}

int
main(int argc, char** argv)
{
  struct JPT_info* db;
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

    WANT_SUCCESS(jpt_insert(db, buf, buf, buf, strlen(buf), 0));
  }

  WANT_SUCCESS(jpt_scan(db, cell_callback, 0));

  WANT_TRUE(count == i);

  WANT_SUCCESS(unlink("test-db.tab"));
  WANT_SUCCESS(unlink("test-db.tab.log"));

  fprintf(stderr, "* passed all %zu test%s\n", test_count, (test_count != 1) ? "s" : "");

  return EXIT_SUCCESS;
}
