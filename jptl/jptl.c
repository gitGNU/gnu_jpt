#include <ctype.h>
#include <errno.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#include "jptl.h"
#include "jpt_internal.h"

#define JPT_POOL_SIZE (64 * 1024)
#define SKIP_SPACE(p) do { while(isspace(*p)) *p++ = 0; } while(0)

struct JPTL_variable
{
  const char* name;
  struct JPTL_cons* value;

  struct JPTL_variable* next;
};

struct JPTL_pool
{
  char* data;
  size_t size;
  size_t data_used;

  struct JPTL_pool* next;
};

struct JPTL_context
{
  struct JPTL_program* p;
  struct JPTL_variable* locals;
};

struct JPTL_program
{
  struct JPTL_cons* head;
  char* data;

  jmp_buf error_handler;
  char* error;

  struct JPTL_pool* first_pool;
};

static struct JPTL_cons*
JPTL_display(struct JPT_info* info, struct JPTL_context* c,
             struct JPTL_cons* cons);

static struct JPTL_cons*
JPTL_eval(struct JPT_info* info, struct JPTL_context* c,
          struct JPTL_cons* cons);

static struct JPTL_cons*
JPTL_flatten(struct JPT_info* info, struct JPTL_context* c,
             struct JPTL_cons* cons);

static struct JPTL_cons*
JPTL_filter(struct JPT_info* info, struct JPTL_context* c,
            struct JPTL_cons* cons);

static struct JPTL_cons*
JPTL_nintersection(struct JPT_info* info, struct JPTL_context* c,
                   struct JPTL_cons* cons);

static struct JPTL_cons*
JPTL_let(struct JPT_info* info, struct JPTL_context* c,
         struct JPTL_cons* cons);

static struct JPTL_cons*
JPTL_lookup(struct JPT_info* info, struct JPTL_context* c,
            struct JPTL_cons* cons);

static struct JPTL_cons*
JPTL_lookupf(struct JPT_info* info, struct JPTL_context* c,
             struct JPTL_cons* cons);

static struct JPTL_cons*
JPTL_zipf(struct JPT_info* info, struct JPTL_context* c,
          struct JPTL_cons* cons);

static const struct
{
  const char* name;
  struct JPTL_cons* (*fun)(struct JPT_info* info, struct JPTL_context* c, struct JPTL_cons* cons);
} functions[] =
{
  { "display",       JPTL_display },
  { "eval",          JPTL_eval },
  { "flatten",       JPTL_flatten },
  { "filter",        JPTL_filter },
  { "nintersection", JPTL_nintersection },
  { "let",           JPTL_let },
  { "lookup",        JPTL_lookup },
  { "lookupf",       JPTL_lookupf },
  { "zipf",          JPTL_zipf },
};


static void*
JPTL_alloc(struct JPTL_program* program, size_t size)
{
  struct JPTL_pool* pool;
  void* result;

  if(program->first_pool
  && program->first_pool->data_used + size <= program->first_pool->size)
  {
    pool = program->first_pool;
  }
  else
  {
    pool = malloc(sizeof(struct JPTL_pool));
    pool->data = malloc(JPT_POOL_SIZE);
    pool->size = JPT_POOL_SIZE;
    pool->data_used = 0;
    pool->next = program->first_pool;
    program->first_pool = pool;
  }

  result = pool->data + pool->data_used;
  pool->data_used += size;

  memset(result, 0, size);

  return result;
}

static struct JPTL_cons*
JPTL_recursive_parse(struct JPTL_program* p, char** pc)
{
  struct JPTL_cons* result;
  char* c;

  result = JPTL_alloc(p, sizeof(struct JPTL_cons));
  c = *pc;

  if(!*c)
    return 0;

  SKIP_SPACE(c);

  if(*c == '\'')
  {
    *c++ = 0;

    result->car = JPTL_alloc(p, sizeof(struct JPTL_cons));
    result->car->car_value.data = "quote";
    result->car->car_value.size = 5;

    SKIP_SPACE(c);

    if(*c == '(')
    {
      *c++ = 0;

      result->car->cdr = JPTL_recursive_parse(p, &c);

      SKIP_SPACE(c);
    }
    else
    {
      result->car->cdr = JPTL_alloc(p, sizeof(struct JPTL_cons));
      result->car->cdr->car_value.data = c;

      while(*c && !isspace(*c) && *c != ')')
        ++c;

      result->car->cdr->car_value.size = c - result->car->cdr->car_value.data;

      SKIP_SPACE(c);
    }
  }
  else if(*c == '(')
  {
    *c++ = 0;

    result->car = JPTL_recursive_parse(p, &c);

    SKIP_SPACE(c);
  }
  else if(*c != ')')
  {
    if(*c == '"')
    {
      *c++ = 0;

      result->car_value.data = c;

      while(*c && *c != '"')
        ++c;

      result->car_value.size = c - result->car_value.data;

      if(*c)
        *c++ = 0;
    }
    else
    {
      result->car_value.data = c;
      result->car = 0;

      while(*c && !isspace(*c) && *c != ')')
        ++c;

      result->car_value.size = c - result->car_value.data;
    }

    SKIP_SPACE(c);
  }
  else
  {
    if(isgraph(*c))
      asprintf(&p->error, "Unexpected character '%c' at offset %u", *c, (unsigned int) (c - p->data));
    else
      asprintf(&p->error, "Unexpected character %d at offset %u", *c, (unsigned int) (c - p->data));

    longjmp(p->error_handler, 1);
  }

  if(*c == ')')
  {
    *c++ = 0;

    result->cdr = 0;
  }
  else
  {
    result->cdr = JPTL_recursive_parse(p, &c);
  }

  *pc = c;

  return result;
}

static void
JPTL_display_recursive(struct JPT_info* info, struct JPTL_context* c,
                       struct JPTL_cons* cons)
{
  if(!cons)
  {
    printf("nil");

    return;
  }

  printf("(");

  if(cons->car_value.data)
    printf("%s", cons->car_value.data);
  else
    JPTL_display_recursive(info, c, cons->car);

  while(cons->cdr)
  {
    cons = cons->cdr;

    if(cons->car_value.data)
      printf(" %s", cons->car_value.data);
    else
    {
      printf(" ");

      JPTL_display_recursive(info, c, cons->car);
    }
  }

  printf(")");
}

static struct JPTL_cons*
JPTL_display(struct JPT_info* info, struct JPTL_context* c, struct JPTL_cons* cons)
{
  struct JPTL_cons* result;

  if(!cons)
  {
    printf("nil");

    return 0;
  }

  result = JPTL_eval(info, c, cons->cdr);

  JPTL_display_recursive(info, c, result);

  return result;
}

static struct JPTL_cons*
JPTL_eval(struct JPT_info* info, struct JPTL_context* c,
          struct JPTL_cons* cons)
{
  struct JPTL_program* p = c->p;
  size_t i;
  const char* cmd;

  if(!cons)
    return 0;

  if(cons->car_value.data)
  {
    struct JPTL_variable* var = c->locals;

    while(var)
    {
      if(!strcmp(var->name, cons->car_value.data))
        return var->value;

      var = var->next;
    }

    asprintf(&p->error, "Undefined variable \"%s\"", cons->car_value.data);

    longjmp(p->error_handler, 1);
  }

  cons = cons->car;
  cmd = cons->car_value.data;

  if(!cmd)
  {
    asprintf(&p->error, "Expected function name, found nil");

    longjmp(p->error_handler, 1);
  }

  if(!strcmp(cmd, "quote"))
    return cons->cdr;

  for(i = 0; i < sizeof(functions) / sizeof(functions[0]); ++i)
  {
    if(!strcmp(cmd, functions[i].name))
      return functions[i].fun(info, c, cons);
  }

  asprintf(&p->error, "Unknown function \"%s\"", cmd);

  longjmp(p->error_handler, 1);
}

static void
JPTL_flatten_recursive(struct JPTL_program* p, struct JPTL_cons* target,
                       const struct JPTL_cons* source)
{
  while(source)
  {
    if(source->car_value.data)
    {
      const char* begin = source->car_value.data;
      const char* last = source->car_value.data + source->car_value.size;
      const char* end;

      while(begin != last)
      {
        end = begin;

        while(end != last && *end)
          ++end;

        if(begin != end)
        {
          if(!target->car)
          {
            target->car = JPTL_alloc(p, sizeof(struct JPTL_cons));
            target->cdr = target->car;
          }
          else
          {
            target->cdr->cdr = JPTL_alloc(p, sizeof(struct JPTL_cons));
            target->cdr = target->cdr->cdr;
          }

          target->cdr->car_value.data = begin;
          target->cdr->car_value.size = end - begin;
        }

        if(end == last)
          break;

        begin = end + 1;
      }
    }
    else if(source->car)
      JPTL_flatten_recursive(p, target, source->car);

    source = source->cdr;
  }
}

static struct JPTL_cons*
JPTL_flatten(struct JPT_info* info, struct JPTL_context* c,
             struct JPTL_cons* cons)
{
  struct JPTL_cons result;
  struct JPTL_cons* values;

  memset(&result, 0, sizeof(result));

  values = JPTL_eval(info, c, cons->cdr);

  JPTL_flatten_recursive(c->p, &result, values);

  return result.car;
}

static struct JPTL_cons*
JPTL_filter(struct JPT_info* info, struct JPTL_context* c,
            struct JPTL_cons* cons)
{
  struct JPTL_cons* child;
  struct JPTL_cons* result;
  struct JPTL_cons* filter;

  result = JPTL_eval(info, c, cons->cdr);

  if(!result)
    return 0;

  for(child = cons->cdr->cdr; child; child = child->cdr)
  {
    struct JPTL_cons* prev = 0;
    struct JPTL_cons* i;
    const char* filter_column;
    const char* filter_value;
    size_t filter_value_size;

    filter = JPTL_eval(info, c, child);

    filter_column = filter->car_value.data;
    filter_value = filter->cdr->car_value.data;
    filter_value_size = filter->cdr->car_value.size;

    i = result;

    while(i)
    {
      void* tmp;
      size_t tmp_size;

      if(-1 == jpt_get(info, i->car_value.data, filter_column, &tmp, &tmp_size)
      || tmp_size != filter_value_size
      || memcmp(tmp, filter_value, filter_value_size))
      {
        if(tmp)
          free(tmp);

        if(!prev)
          result = i->cdr;
        else
          prev->cdr = i->cdr;

        i = i->cdr;
      }
      else
      {
        prev = i;
        i = i->cdr;
      }
    }

    if(prev)
      prev->cdr = 0;
  }

  return result;
}

static struct JPTL_cons*
JPTL_nintersection(struct JPT_info* info, struct JPTL_context* c,
                   struct JPTL_cons* cons)
{
  struct JPTL_cons* child;
  struct JPTL_cons* values;
  struct JPTL_cons* result = 0;
  int first = 1;

  child = cons->cdr;

  while(child)
  {
    values = JPTL_eval(info, c, child);

    if(first)
    {
      result = values;
      first = 0;
    }
    else
    {
      struct JPTL_cons* prev = 0;
      struct JPTL_cons* a;
      struct JPTL_cons* b;
      int cmp;

      a = result;
      b = values;

      while(a && b)
      {
        cmp = strcmp(a->car_value.data, b->car_value.data);

        if(cmp == 0)
        {
          prev = a;
          a = a->cdr;
          b = b->cdr;
        }
        else if(cmp < 0)
        {
          if(!prev)
            result = a->cdr;
          else
            prev->cdr = a->cdr;

          a = a->cdr;
        }
        else /* cmp > 0 */
          b = b->cdr;
      }

      if(prev)
        prev->cdr = 0;
    }

    if(!result)
      break;

    child = child->cdr;
  }

  return result;
}

static struct JPTL_cons*
JPTL_let(struct JPT_info* info, struct JPTL_context* c,
         struct JPTL_cons* cons)
{
  struct JPTL_cons* i;
  struct JPTL_cons* result = 0;
  struct JPTL_context newc;

  if(!cons->cdr || !cons->cdr->car)
  {
    asprintf(&c->p->error, "let: variable list missing");

    longjmp(c->p->error_handler, 1);
  }

  if(!cons->cdr->cdr || !cons->cdr->cdr->car)
    return 0;

  /* Iterate over values */

  i = cons->cdr->car;

  while(i)
  {
    struct JPTL_variable* var;

    var = JPTL_alloc(c->p, sizeof(struct JPTL_variable));

    var->name = i->car_value.data;
    if(i->cdr->car_value.data)
    {
      var->value = JPTL_alloc(c->p, sizeof(struct JPTL_cons));
      var->value->car_value = i->cdr->car_value;
    }
    else
      var->value = JPTL_eval(info, c, i->cdr);
    var->next = c->locals;
    c->locals = var;

    i = i->cdr->cdr;
  }

  newc = *c;

  /* Iterate over body */

  i = cons->cdr->cdr;

  while(i)
  {
    result = JPTL_eval(info, &newc, i);

    if(!i->cdr)
      break;

    i = i->cdr;
  }

  return result;
}

static struct JPTL_cons*
JPTL_lookup(struct JPT_info* info, struct JPTL_context* c,
            struct JPTL_cons* cons)
{
  struct JPTL_program* p = c->p;
  struct JPTL_cons* columns;
  struct JPTL_cons* column;
  const char* column_name;

  struct JPTL_cons* rows;
  struct JPTL_cons* row;
  const char* row_name;

  struct JPTL_cons* result = 0;
  struct JPTL_cons* last = 0;

  if(!cons->cdr || !cons->cdr->car)
  {
    asprintf(&p->error, "lookup: column list missing");

    longjmp(p->error_handler, 1);
  }

  if(!cons->cdr->cdr)
  {
    asprintf(&p->error, "lookup: row list missing");

    longjmp(p->error_handler, 1);
  }

  columns = JPTL_eval(info, c, cons->cdr);

  rows = JPTL_eval(info, c, cons->cdr->cdr);

  for(row = rows; row; row = row->cdr)
  {
    struct JPTL_cons* r;

    row_name = row->car_value.data;

    if(!row_name)
      continue;

    r = JPTL_alloc(p, sizeof(struct JPTL_cons));

    if(!result)
    {
      result = JPTL_alloc(p, sizeof(struct JPTL_cons));
      result->car = r;
      last = result;
    }
    else
    {
      last->cdr = JPTL_alloc(p, sizeof(struct JPTL_cons));
      last = last->cdr;
      last->car = r;
    }

    for(column = columns; column; column = column->cdr)
    {
      struct JPTL_pool* pool;
      char* data;
      size_t data_size;

      column_name = column->car_value.data;

      if(!column_name)
        continue;

      if(-1 == jpt_get(info, row_name, column_name, (void*) &data, &data_size))
      {
        if(errno != ENOENT)
        {
          p->error = (char*) jpt_last_error();

          longjmp(p->error_handler, 1);
        }

        if(column != columns)
        {
          r->cdr = JPTL_alloc(p, sizeof(struct JPTL_cons));
          r = r->cdr;
        }
      }
      else
      {
        if(column != columns)
        {
          r->cdr = JPTL_alloc(p, sizeof(struct JPTL_cons));
          r = r->cdr;
        }

        r->car_value.data = data;
        r->car_value.size = data_size;

        pool = malloc(sizeof(struct JPTL_pool));
        pool->data = data;
        pool->data_used = JPT_POOL_SIZE;

        if(p->first_pool)
        {
          pool->next = p->first_pool->next;
          p->first_pool->next = pool;
        }
        else
        {
          pool->next = 0;
          p->first_pool = pool;
        }
      }
    }
  }

  return result;
}

static struct JPTL_cons*
JPTL_lookupf(struct JPT_info* info, struct JPTL_context* c,
             struct JPTL_cons* cons)
{
  struct JPTL_cons result;
  struct JPTL_cons* values;

  values = JPTL_lookup(info, c, cons);

  if(!values)
    return 0;

  memset(&result, 0, sizeof(result));
  JPTL_flatten_recursive(c->p, &result, values);

  return result.car;
}

static struct JPTL_cons*
JPTL_zipf(struct JPT_info* info, struct JPTL_context* c,
          struct JPTL_cons* cons)
{
  struct JPTL_program* p = c->p;
  struct JPTL_cons* tmp;
  struct JPTL_cons** values;
  struct JPTL_cons* result = 0;
  struct JPTL_cons* last = 0;
  size_t i, child_count;
  int found;

  child_count = 0;

  for(tmp = cons->cdr; tmp; tmp = tmp->cdr)
    ++child_count;

  if(!child_count)
    return 0;

  values = alloca(sizeof(struct JPTL_cons) * child_count);

  i = 0;

  for(tmp = cons->cdr; tmp; tmp = tmp->cdr)
    values[i++] = JPTL_eval(info, c, tmp);

  do
  {
    struct JPTL_cons* r = 0;

    found = 0;

    for(i = 0; i < child_count; ++i)
    {
      if(!values[i])
        continue;

      if(!r)
      {
        r = JPTL_alloc(p, sizeof(struct JPTL_cons));

        if(!result)
        {
          result = JPTL_alloc(p, sizeof(struct JPTL_cons));
          result->car = r;
          last = result;
        }
        else
        {
          last->cdr = JPTL_alloc(p, sizeof(struct JPTL_cons));
          last = last->cdr;
          last->car = r;
        }
      }

      tmp = values[i]->car;
      values[i] = values[i]->cdr;

      while(tmp)
      {
        if(found)
        {
          r->cdr = JPTL_alloc(p, sizeof(struct JPTL_cons));
          r = r->cdr;
        }

        r->car_value = tmp->car_value;

        tmp = tmp->cdr;

        found = 1;
      }
    }
  }
  while(found);

  return result;
}

static void
JPTL_free_program(struct JPTL_program* p)
{
  struct JPTL_pool* pool;

  while(p->first_pool)
  {
    pool = p->first_pool->next;
    free(p->first_pool->data);
    free(p->first_pool);
    p->first_pool = pool;
  }

  free(p->data);
  free(p);
}

static struct JPTL_program*
JPTL_compile(const char* query)
{
  struct JPTL_program* p;
  char* c;

  p = malloc(sizeof(struct JPTL_program));
  memset(p, 0, sizeof(struct JPTL_program));

  c = p->data = strdup(query);

  if(setjmp(p->error_handler))
  {
    JPT_set_error(p->error, EINVAL);
    JPTL_free_program(p);

    return 0;
  }

  p->head = JPTL_recursive_parse(p, &c);

  return p;
}

int
jptl_eval_string(struct JPT_info* info, const char* query, jptl_callback callback, void* arg)
{
  struct JPTL_context c;
  struct JPTL_cons* result;

  JPT_clear_error();

  memset(&c, 0, sizeof(c));
  c.p = JPTL_compile(query);

  if(!c.p)
    return -1;

  if(setjmp(c.p->error_handler))
  {
    JPT_set_error(c.p->error, EINVAL);

    return -1;
  }

  result = JPTL_eval(info, &c, c.p->head);

  callback(result, arg);

  JPTL_free_program(c.p);

  return 0;
}
