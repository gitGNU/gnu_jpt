#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>

#include <php.h>
#include <php_ini.h>
#include <ext/standard/info.h>

#include "../djpt/djpt.h"
#include "../djpt/djpt_internal.h"

#include "php-djpt.h"

static struct
{
  size_t refcount;
  char* database;
  struct DJPT_info* info;
} handles[16];

function_entry djpt_functions[] =
{
    PHP_FE(djpt_init, 0)
    PHP_FE(djpt_close, 0)
    PHP_FE(djpt_insert, 0)
    PHP_FE(djpt_append, 0)
    PHP_FE(djpt_replace, 0)
    PHP_FE(djpt_remove, 0)
    PHP_FE(djpt_has_key, 0)
    PHP_FE(djpt_get, 0)
    PHP_FE(djpt_column_scan, 0)
    PHP_FE(djpt_eval, 0)
    PHP_FE(djpt_get_counter, 0)
    { 0, 0, 0 }
};

zend_module_entry djpt_module_entry =
{
#if ZEND_MODULE_API_NO >= 20010901
    STANDARD_MODULE_HEADER,
#endif
    "djpt",
    djpt_functions,
    PHP_MINIT(djpt),
    0,
    0,
    0,
    PHP_MINFO(djpt),
#if ZEND_MODULE_API_NO >= 20010901
    "1.0", /* Replace with version number for your extension */
#endif
    STANDARD_MODULE_PROPERTIES
};

ZEND_GET_MODULE(djpt)

PHP_MINIT_FUNCTION(djpt)
{
  REGISTER_STRING_CONSTANT("DJPT_EXT_VERSION", DJPT_VERSION, CONST_CS | CONST_PERSISTENT);

  return SUCCESS;
}

PHP_MINFO_FUNCTION(djpt)
{
  php_info_print_table_start();
  php_info_print_table_header(2, "djpt support", "enabled");
  php_info_print_table_end();
}

PHP_FUNCTION(djpt_init)
{
  int argc = ZEND_NUM_ARGS();
  char* database = 0;
  unsigned int database_len;
  long result;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "s", &database, &database_len))
    RETURN_FALSE;

  database = strndupa(database, database_len);

  for(result = 0; result < sizeof(handles) / sizeof(handles[0]); ++result)
  {
    if(handles[result].refcount
    && !strcmp(handles[result].database, database))
    {
      if(handles[result].info)
      {
        struct pollfd pfd;
        pfd.fd = handles[result].info->peer->fd;
        pfd.events = 0;
        pfd.revents = 0;

        if(0 < poll(&pfd, 1, 0)
        && 0 != (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)))
        {
          zend_error(E_NOTICE, "Database '%s' was disconnected, reconnecting", database);

          djpt_close(handles[result].info);
          handles[result].info = djpt_init(database);
        }
      }
      else
        handles[result].info = djpt_init(database);

      if(!handles[result].info)
      {
        zend_error(E_WARNING, "Failed to connect to database '%s': %s", database, djpt_last_error());

        RETURN_FALSE;
      }

      ++handles[result].refcount;

      RETURN_LONG(result + 1);
    }
  }

  for(result = 0; result < sizeof(handles) / sizeof(handles[0]); ++result)
    if(!handles[result].info)
      break;

  if(result == sizeof(handles) / sizeof(handles[0]))
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  handles[result].info = djpt_init(database);

  if(!handles[result].info)
  {
    zend_error(E_WARNING, "Could not connect to database '%s': %s", database, djpt_last_error());
    RETURN_FALSE;
  }

  handles[result].database = strdup(database);

  RETVAL_LONG(result + 1);
}

PHP_FUNCTION(djpt_close)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "l", &handle))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  if(--handles[handle].refcount)
    RETURN_TRUE;

  djpt_close(handles[handle].info);
  handles[handle].info = 0;
  free(handles[handle].database);

  RETVAL_TRUE;
}

PHP_FUNCTION(djpt_insert)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;
  char* row = 0;
  char* column = 0;
  char* value = 0;
  unsigned int row_len, column_len, value_len;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "lsss", &handle, &row, &row_len, &column, &column_len, &value, &value_len))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  row = strndupa(row, row_len);
  column = strndupa(column, column_len);

  if(-1 == djpt_insert(handles[handle].info, row, column, value, value_len, 0))
  {
    if(errno != EEXIST)
      zend_error(E_WARNING, "Insert failed: %s", djpt_last_error());

    RETURN_FALSE;
  }
  else
  {
    RETURN_TRUE;
  }
}

PHP_FUNCTION(djpt_append)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;
  char* row = 0;
  char* column = 0;
  char* value = 0;
  unsigned int row_len, column_len, value_len;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "lsss", &handle, &row, &row_len, &column, &column_len, &value, &value_len))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  row = strndupa(row, row_len);
  column = strndupa(column, column_len);

  if(-1 == djpt_insert(handles[handle].info, row, column, value, value_len, DJPT_APPEND))
  {
    zend_error(E_WARNING, "Append failed: %s", djpt_last_error());
    RETURN_FALSE;
  }
  else
  {
    RETURN_TRUE;
  }
}

PHP_FUNCTION(djpt_replace)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;
  char* row = 0;
  char* column = 0;
  char* value = 0;
  unsigned int row_len, column_len, value_len;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "lsss", &handle, &row, &row_len, &column, &column_len, &value, &value_len))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  row = strndupa(row, row_len);
  column = strndupa(column, column_len);

  if(-1 == djpt_insert(handles[handle].info, row, column, value, value_len, DJPT_REPLACE))
  {
    zend_error(E_WARNING, "Replace failed: %s", djpt_last_error());
    RETURN_FALSE;
  }
  else
  {
    RETURN_TRUE;
  }
}

PHP_FUNCTION(djpt_remove)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;
  char* row = 0;
  char* column = 0;
  unsigned int row_len, column_len;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "lss", &handle, &row, &row_len, &column, &column_len))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  row = strndupa(row, row_len);
  column = strndupa(column, column_len);

  if(-1 == djpt_remove(handles[handle].info, row, column))
  {
    if(errno != ENOENT)
      zend_error(E_WARNING, "Remove failed: %s", djpt_last_error());

    RETURN_FALSE;
  }
  else
  {
    RETURN_TRUE;
  }
}

PHP_FUNCTION(djpt_has_key)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;
  char* row = 0;
  char* column = 0;
  unsigned int row_len, column_len;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "lss", &handle, &row, &row_len, &column, &column_len))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  row = strndupa(row, row_len);
  column = strndupa(column, column_len);

  if(-1 == djpt_has_key(handles[handle].info, row, column))
  {
    if(errno != ENOENT)
      zend_error(E_WARNING, "Has-key failed: %s", djpt_last_error());

    RETURN_FALSE;
  }
  else
  {
    RETURN_TRUE;
  }
}


PHP_FUNCTION(djpt_get)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;
  char* row = 0;
  char* column = 0;
  unsigned int row_len, column_len;

  char *result = 0;
  size_t result_len = 0;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "lss", &handle, &row, &row_len, &column, &column_len))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  row = strndupa(row, row_len);
  column = strndupa(column, column_len);

  if(-1 == djpt_get(handles[handle].info, row, column, (void*) &result, &result_len))
  {
    if(errno != ENOENT)
      zend_error(E_WARNING, "Getting of %s/%s failed: %s", row, column, djpt_last_error());

    RETURN_FALSE;
  }

  RETVAL_STRINGL(result, result_len, 1);

  free(result);
}

static int
column_scan_cell_callback(const char* row, const char* column, const void* data, size_t data_size, uint64_t* timestamp, void* arg)
{
  zval* return_value = arg;

  add_assoc_stringl_ex(return_value, (char*) row, strlen(row) + 1, (char*) data, data_size, 1);

  return 0;
}

PHP_FUNCTION(djpt_column_scan)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;
  long limit = 0;
  char* column = 0;
  unsigned int column_len;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "lsl", &handle, &column, &column_len, &limit))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  column = strndupa(column, column_len);

  array_init(return_value);

  if(-1 == djpt_column_scan(handles[handle].info, column, column_scan_cell_callback, return_value, limit))
  {
    zend_error(E_WARNING, "Error scanning column '%s': %s", column, djpt_last_error());
    zend_hash_destroy(Z_ARRVAL_P(return_value));
    efree(Z_ARRVAL_P(return_value));
    RETURN_FALSE;
  }
}

static int
eval_callback(const void* data, size_t data_size, void* arg)
{
  zval* return_value = arg;

  add_next_index_stringl(return_value, (char*) data, data_size, 1);

  return 0;
}

PHP_FUNCTION(djpt_eval)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;
  long limit = 0;
  char* program = 0;
  unsigned int program_len;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "ls", &handle, &program, &program_len))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  program = strndupa(program, program_len);

  array_init(return_value);

  if(-1 == djpt_eval(handles[handle].info, program, eval_callback, return_value))
  {
    zend_error(E_WARNING, "Evail failed: %s", djpt_last_error());
    zend_hash_destroy(Z_ARRVAL_P(return_value));
    efree(Z_ARRVAL_P(return_value));
    RETURN_FALSE;
  }
}

PHP_FUNCTION(djpt_get_counter)
{
  int argc = ZEND_NUM_ARGS();
  long handle = 0;
  long limit = 0;
  char* counter = 0;
  unsigned int counter_len;

  if(FAILURE == zend_parse_parameters(argc TSRMLS_CC, "ls", &handle, &counter, &counter_len))
    RETURN_FALSE;
  --handle;

  if(handle > sizeof(handles) / sizeof(handles[0]) || handle < 0 || !handles[handle].info)
  {
    zend_error(E_WARNING, "Invalid database handle %ld", handle);
    RETURN_FALSE;
  }

  counter = strndupa(counter, counter_len);

  RETURN_LONG(djpt_get_counter(handles[handle].info, counter));
}
