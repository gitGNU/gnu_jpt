#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "jpt.h"

#include "common.h"

static char desired_value[256];

static struct JPT_info* db;

static const char* values[] = {
  "ABCDE",
  "abcdefghij",
  "12345678901234567890"
};

void replace_large_value()
{
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", values[2], strlen(values[2]), JPT_REPLACE));
  strcpy(desired_value, values[2]);
}

void append_large_value()
{
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", values[2], strlen(values[2]), JPT_APPEND));
  strcat(desired_value, values[2]);
}

void replace_medium_value()
{
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", values[1], strlen(values[1]), JPT_REPLACE));
  strcpy(desired_value, values[1]);
}

void append_medium_value()
{
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", values[1], strlen(values[1]), JPT_APPEND));
  strcat(desired_value, values[1]);
}

void replace_small_value()
{
  WANT_SUCCESS(jpt_insert(db, "row1", "col1", values[0], strlen(values[0]), JPT_REPLACE));
  strcpy(desired_value, values[0]);
}

void remove_value()
{
  if(desired_value[0])
  {
    WANT_SUCCESS(jpt_remove(db, "row1", "col1"));
  }
  else
  {
    WANT_FAILURE(jpt_remove(db, "row1", "col1"));
    WANT_TRUE(errno == ENOENT);
  }

  desired_value[0] = 0;
}

void do_op(int n)
{
  void* ret;
  size_t retsize;

  switch(n)
  {
  case 0: replace_large_value(); break;
  case 1: append_large_value(); break;
  case 2: replace_medium_value(); break;
  case 3: append_medium_value(); break;
  case 4: replace_small_value(); break;
  case 5: remove_value(); break;
  case 6: jpt_compact(db); break;
  }

  if(desired_value[0])
  {
    WANT_SUCCESS(jpt_get(db, "row1", "col1", &ret, &retsize));
    WANT_TRUE(retsize == strlen(desired_value));
    WANT_TRUE(!memcmp(desired_value, ret, retsize));
    free(ret);
  }
  else
  {
    WANT_FAILURE(jpt_get(db, "row1", "col1", &ret, &retsize));
  }
}

void
reverse(int* __first, int* __last)
{
  if (__first == __last)
    return;
  --__last;
  while (__first < __last)
  {
    int tmp;

    tmp = *__first;
    *__first = *__last;
    *__last = tmp;

    ++__first;
    --__last;
  }
}

int
next_permutation(int* __first, int* __last)
{
  if (__first == __last)
    return -1;

  int* __i = __first;
  ++__i;

  if (__i == __last)
    return -1;

  __i = __last;
  --__i;

  for(;;)
  {
    int* __ii = __i;
    --__i;
    if (*__i < *__ii)
    {
      int* __j = __last;
      int tmp;

      while (!(*__i < *--__j))
      {}

      tmp = *__i;
      *__i = *__j;
      *__j = tmp;

      reverse(__ii, __last);
      return 0;
    }
    if (__i == __first)
    {
      reverse(__first, __last);
      return -1;
    }
  }
}

int
main(int argc, char** argv)
{
  void* ret;
  size_t retsize;
  int n = 0, i, commands[7];

  WANT_TRUE(0 == unlink("test-db.tab") || errno == ENOENT);
  WANT_TRUE(0 == unlink("test-db.tab.log") || errno == ENOENT);

  for(i = 0; i < 7; ++i)
    commands[i] = i;

  do
  {
    fprintf(stderr, "Iter %d of 5040...\n", n++);

    desired_value[0] = 0;

    WANT_POINTER(db = jpt_init("test-db.tab", 1024 * 1024, 0));

    WANT_SUCCESS(jpt_insert(db, "row1", "col1", "xxx", 3, 0));
    strcpy(desired_value, "xxx");

    for(i = 0; i < 7; ++i)
      do_op(commands[i]);

    jpt_close(db);

    WANT_SUCCESS(unlink("test-db.tab"));
    WANT_SUCCESS(unlink("test-db.tab.log"));
  }
  while(0 == next_permutation(commands, commands + 7));

  fprintf(stderr, "* passed all %zu test%s\n", test_count, (test_count != 1) ? "s" : "");

  return EXIT_SUCCESS;
}
