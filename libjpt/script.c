#include <ctype.h>
#include <errno.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#include "jpt.h"
#include "jpt_internal.h"

#define JPT_POOL_SIZE (64 * 1024)
#define SKIP_SPACE(p) do { while(isspace(*p)) *p++ = 0; } while(0)

struct JPT_variable
{
  const char* name;
  struct JPT_cons* value;

  struct JPT_variable* next;
};

struct JPT_pool
{
  char* data;
  size_t size;
  size_t data_used;

  struct JPT_pool* next;
};

struct JPT_context
{
  struct JPT_program* p;
  struct JPT_variable* locals;
};

struct JPT_program
{
  struct JPT_cons* head;
  char* data;

  jmp_buf error_handler;
  char* error;

  struct JPT_pool* first_pool;
};

static struct JPT_cons*
JPT_display(struct JPT_info* info, struct JPT_context* c,
             struct JPT_cons* cons);

static struct JPT_cons*
JPT_eval(struct JPT_info* info, struct JPT_context* c,
          struct JPT_cons* cons);

static struct JPT_cons*
JPT_flatten(struct JPT_info* info, struct JPT_context* c,
             struct JPT_cons* cons);

static struct JPT_cons*
JPT_filter(struct JPT_info* info, struct JPT_context* c,
            struct JPT_cons* cons);

static struct JPT_cons*
JPT_nintersection(struct JPT_info* info, struct JPT_context* c,
                   struct JPT_cons* cons);

static struct JPT_cons*
JPT_let(struct JPT_info* info, struct JPT_context* c,
         struct JPT_cons* cons);

static struct JPT_cons*
JPT_lookup(struct JPT_info* info, struct JPT_context* c,
            struct JPT_cons* cons);

static struct JPT_cons*
JPT_lookupf(struct JPT_info* info, struct JPT_context* c,
             struct JPT_cons* cons);

static struct JPT_cons*
JPT_zipf(struct JPT_info* info, struct JPT_context* c,
          struct JPT_cons* cons);

static const struct
{
  const char* name;
  struct JPT_cons* (*fun)(struct JPT_info* info, struct JPT_context* c, struct JPT_cons* cons);
} functions[] =
{
  { "display",       JPT_display },
  { "eval",          JPT_eval },
  { "flatten",       JPT_flatten },
  { "filter",        JPT_filter },
  { "nintersection", JPT_nintersection },
  { "let",           JPT_let },
  { "lookup",        JPT_lookup },
  { "lookupf",       JPT_lookupf },
  { "zipf",          JPT_zipf },
};

static void*
JPT_alloc(struct JPT_program* program, size_t size)
{
  struct JPT_pool* pool;
  void* result;

  if(program->first_pool
  && program->first_pool->data_used + size <= program->first_pool->size)
  {
    pool = program->first_pool;
  }
  else
  {
    pool = malloc(sizeof(struct JPT_pool));
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

static struct JPT_cons*
JPT_recursive_parse(struct JPT_program* p, char** pc)
{
  struct JPT_cons* result;
  char* c;

  result = JPT_alloc(p, sizeof(struct JPT_cons));
  c = *pc;

  if(!*c)
    return 0;

  SKIP_SPACE(c);

  if(*c == '\'')
  {
    *c++ = 0;

    result->car = JPT_alloc(p, sizeof(struct JPT_cons));
    result->car->car_value.data = "quote";
    result->car->car_value.size = 5;

    SKIP_SPACE(c);

    if(*c == '(')
    {
      *c++ = 0;

      result->car->cdr = JPT_recursive_parse(p, &c);

      SKIP_SPACE(c);
    }
    else
    {
      result->car->cdr = JPT_alloc(p, sizeof(struct JPT_cons));
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

    result->car = JPT_recursive_parse(p, &c);

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
    result->cdr = JPT_recursive_parse(p, &c);
  }

  *pc = c;

  return result;
}

static void
JPT_display_recursive(struct JPT_info* info, struct JPT_context* c,
                       struct JPT_cons* cons)
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
    JPT_display_recursive(info, c, cons->car);

  while(cons->cdr)
  {
    cons = cons->cdr;

    if(cons->car_value.data)
      printf(" %s", cons->car_value.data);
    else
    {
      printf(" ");

      JPT_display_recursive(info, c, cons->car);
    }
  }

  printf(")");
}

static struct JPT_cons*
JPT_display(struct JPT_info* info, struct JPT_context* c, struct JPT_cons* cons)
{
  struct JPT_cons* result;

  if(!cons)
  {
    printf("nil");

    return 0;
  }

  result = JPT_eval(info, c, cons->cdr);

  JPT_display_recursive(info, c, result);

  return result;
}

static struct JPT_cons*
JPT_eval(struct JPT_info* info, struct JPT_context* c,
          struct JPT_cons* cons)
{
  struct JPT_program* p = c->p;
  size_t i;
  const char* cmd;

  if(!cons)
    return 0;

  if(cons->car_value.data)
  {
    struct JPT_variable* var = c->locals;

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
JPT_flatten_recursive(struct JPT_program* p, struct JPT_cons* target,
                       const struct JPT_cons* source)
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
            target->car = JPT_alloc(p, sizeof(struct JPT_cons));
            target->cdr = target->car;
          }
          else
          {
            target->cdr->cdr = JPT_alloc(p, sizeof(struct JPT_cons));
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
      JPT_flatten_recursive(p, target, source->car);

    source = source->cdr;
  }
}

static struct JPT_cons*
JPT_flatten(struct JPT_info* info, struct JPT_context* c,
             struct JPT_cons* cons)
{
  struct JPT_cons result;
  struct JPT_cons* values;

  memset(&result, 0, sizeof(result));

  values = JPT_eval(info, c, cons->cdr);

  JPT_flatten_recursive(c->p, &result, values);

  return result.car;
}

static struct JPT_cons*
JPT_filter(struct JPT_info* info, struct JPT_context* c,
            struct JPT_cons* cons)
{
  struct JPT_cons* child;
  struct JPT_cons* result;
  struct JPT_cons* filter;

  result = JPT_eval(info, c, cons->cdr);

  if(!result)
    return 0;

  for(child = cons->cdr->cdr; child; child = child->cdr)
  {
    struct JPT_cons* prev = 0;
    struct JPT_cons* i;
    const char* filter_column;
    const char* filter_value;
    size_t filter_value_size;

    filter = JPT_eval(info, c, child);

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

static struct JPT_cons*
JPT_nintersection(struct JPT_info* info, struct JPT_context* c,
                   struct JPT_cons* cons)
{
  struct JPT_cons* child;
  struct JPT_cons* values;
  struct JPT_cons* result = 0;
  int first = 1;

  child = cons->cdr;

  while(child)
  {
    values = JPT_eval(info, c, child);

    if(first)
    {
      result = values;
      first = 0;
    }
    else
    {
      struct JPT_cons* prev = 0;
      struct JPT_cons* a;
      struct JPT_cons* b;
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

static struct JPT_cons*
JPT_let(struct JPT_info* info, struct JPT_context* c,
         struct JPT_cons* cons)
{
  struct JPT_cons* i;
  struct JPT_cons* result = 0;
  struct JPT_context newc;

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
    struct JPT_variable* var;

    var = JPT_alloc(c->p, sizeof(struct JPT_variable));

    var->name = i->car_value.data;
    if(i->cdr->car_value.data)
    {
      var->value = JPT_alloc(c->p, sizeof(struct JPT_cons));
      var->value->car_value = i->cdr->car_value;
    }
    else
      var->value = JPT_eval(info, c, i->cdr);
    var->next = c->locals;
    c->locals = var;

    i = i->cdr->cdr;
  }

  newc = *c;

  /* Iterate over body */

  i = cons->cdr->cdr;

  while(i)
  {
    result = JPT_eval(info, &newc, i);

    if(!i->cdr)
      break;

    i = i->cdr;
  }

  return result;
}

static struct JPT_cons*
JPT_lookup(struct JPT_info* info, struct JPT_context* c,
            struct JPT_cons* cons)
{
  struct JPT_program* p = c->p;
  struct JPT_cons* columns;
  struct JPT_cons* column;
  const char* column_name;

  struct JPT_cons* rows;
  struct JPT_cons* row;
  const char* row_name;

  struct JPT_cons* result = 0;
  struct JPT_cons* last = 0;

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

  columns = JPT_eval(info, c, cons->cdr);

  rows = JPT_eval(info, c, cons->cdr->cdr);

  for(row = rows; row; row = row->cdr)
  {
    struct JPT_cons* r;

    row_name = row->car_value.data;

    if(!row_name)
      continue;

    r = JPT_alloc(p, sizeof(struct JPT_cons));

    if(!result)
    {
      result = JPT_alloc(p, sizeof(struct JPT_cons));
      result->car = r;
      last = result;
    }
    else
    {
      last->cdr = JPT_alloc(p, sizeof(struct JPT_cons));
      last = last->cdr;
      last->car = r;
    }

    for(column = columns; column; column = column->cdr)
    {
      struct JPT_pool* pool;
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
          r->cdr = JPT_alloc(p, sizeof(struct JPT_cons));
          r = r->cdr;
        }
      }
      else
      {
        if(column != columns)
        {
          r->cdr = JPT_alloc(p, sizeof(struct JPT_cons));
          r = r->cdr;
        }

        r->car_value.data = data;
        r->car_value.size = data_size;

        pool = malloc(sizeof(struct JPT_pool));
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

static struct JPT_cons*
JPT_lookupf(struct JPT_info* info, struct JPT_context* c,
             struct JPT_cons* cons)
{
  struct JPT_cons result;
  struct JPT_cons* values;

  values = JPT_lookup(info, c, cons);

  if(!values)
    return 0;

  memset(&result, 0, sizeof(result));
  JPT_flatten_recursive(c->p, &result, values);

  return result.car;
}

static struct JPT_cons*
JPT_zipf(struct JPT_info* info, struct JPT_context* c,
          struct JPT_cons* cons)
{
  struct JPT_program* p = c->p;
  struct JPT_cons* tmp;
  struct JPT_cons** values;
  struct JPT_cons* result = 0;
  struct JPT_cons* last = 0;
  size_t i, child_count;
  int found;

  child_count = 0;

  for(tmp = cons->cdr; tmp; tmp = tmp->cdr)
    ++child_count;

  if(!child_count)
    return 0;

  values = alloca(sizeof(struct JPT_cons) * child_count);

  i = 0;

  for(tmp = cons->cdr; tmp; tmp = tmp->cdr)
    values[i++] = JPT_eval(info, c, tmp);

  do
  {
    struct JPT_cons* r = 0;

    found = 0;

    for(i = 0; i < child_count; ++i)
    {
      if(!values[i])
        continue;

      if(!r)
      {
        r = JPT_alloc(p, sizeof(struct JPT_cons));

        if(!result)
        {
          result = JPT_alloc(p, sizeof(struct JPT_cons));
          result->car = r;
          last = result;
        }
        else
        {
          last->cdr = JPT_alloc(p, sizeof(struct JPT_cons));
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
          r->cdr = JPT_alloc(p, sizeof(struct JPT_cons));
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
JPT_free_program(struct JPT_program* p)
{
  struct JPT_pool* pool;

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

static struct JPT_program*
JPT_compile(const char* query)
{
  struct JPT_program* p;
  char* c;

  p = malloc(sizeof(struct JPT_program));
  memset(p, 0, sizeof(struct JPT_program));

  c = p->data = strdup(query);

  if(setjmp(p->error_handler))
  {
    JPT_set_error(p->error, EINVAL);
    JPT_free_program(p);

    return 0;
  }

  p->head = JPT_recursive_parse(p, &c);

  return p;
}

int
jpt_eval(struct JPT_info* info, const char* query, jpt_cons_callback callback, void* arg)
{
  struct JPT_context c;
  struct JPT_cons* result;

  JPT_clear_error();

  memset(&c, 0, sizeof(c));
  c.p = JPT_compile(query);

  if(!c.p)
    return -1;

  if(setjmp(c.p->error_handler))
  {
    JPT_set_error(c.p->error, EINVAL);

    return -1;
  }

  result = JPT_eval(info, &c, c.p->head);

  callback(result, arg);

  JPT_free_program(c.p);

  return 0;
}
