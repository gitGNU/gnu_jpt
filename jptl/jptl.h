#ifndef JPTL_H_
#define JPTL_H_ 1

#include "jpt.h"

struct JPTL_context;

struct JPTL_value
{
  const char* data;
  size_t size;
};

struct JPTL_cons
{
  struct JPTL_value car_value;
  struct JPTL_cons* car;
  struct JPTL_cons* cdr;
};

typedef int (*jptl_callback)(struct JPTL_cons* data, void* arg);

/**
 * Evaluates a JPT Lisp program and send the result to a callback function.
 */
int
jptl_eval_string(struct JPT_info* info, const char* query, jptl_callback callback, void* arg);

#endif /* !JPTL_H_ */
