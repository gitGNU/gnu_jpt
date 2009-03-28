/*  Common code for djpt client and server.
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
#include <fcntl.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include "djpt.h"
#include "djpt_internal.h"

#define DJPT_MEMTABLE_SIZE (8 * 1024 * 1024)

#ifndef DJPT_CLIENT
struct DJPT_jpt_handle DJPT_jpt_handles[16];
size_t DJPT_jpt_handle_alloc = 16;

pthread_mutex_t DJPT_jpt_handle_lock = PTHREAD_MUTEX_INITIALIZER;
#endif

ssize_t
DJPT_read_all(struct DJPT_peer* peer, void* target, size_t size)
{
  size_t remaining = size;
  char* o = target;

  if(peer->read_buffer)
  {
    while(remaining)
    {
      size_t amount = remaining;

      assert(peer->read_buffer_offset <= peer->read_buffer_fill);

      if(peer->read_buffer_offset == peer->read_buffer_fill)
      {
        int res = read(peer->fd, peer->read_buffer, peer->read_buffer_size);

        if(res <= 0)
          return -1;

        peer->read_buffer_offset = 0;
        peer->read_buffer_fill = res;
      }

      if(amount > peer->read_buffer_fill - peer->read_buffer_offset)
        amount = peer->read_buffer_fill - peer->read_buffer_offset;

      memcpy(o, peer->read_buffer + peer->read_buffer_offset, amount);

      peer->read_buffer_offset += amount;
      remaining -= amount;
      o += amount;
    }
  }
  else
  {
    while(remaining)
    {
      int res = read(peer->fd, o, remaining);

      if(res <= 0)
        return -1;

      o += res;
      remaining -= res;
    }
  }

  return size;
}

#ifndef DJPT_CLIENT
static int
DJPT_flush_buffer(struct DJPT_peer* peer)
{
  if(-1 == DJPT_write_all(peer, peer->write_buffer, peer->write_buffer_fill))
    return -1;

  peer->write_buffer_fill = 0;

  return 0;
}

static ssize_t
DJPT_write_buffered(struct DJPT_peer* peer, const void* source, size_t size)
{
  if(peer->write_buffer_fill + size < peer->write_buffer_size)
  {
    memcpy(peer->write_buffer + peer->write_buffer_fill, source, size);
    peer->write_buffer_fill += size;

    return size;
  }

  if(-1 == DJPT_flush_buffer(peer))
    return -1;

  if(size > peer->write_buffer_size)
    return DJPT_write_all(peer, source, size);

  memcpy(peer->write_buffer, source, size);
  peer->write_buffer_fill = size;

  return size;
}
#endif

ssize_t
DJPT_write_all(struct DJPT_peer* peer, const void* source, size_t size)
{
  size_t remaining = size;
  const char* i = source;

#ifndef DJPT_CLIENT
  if(peer->write_buffer_fill && source != peer->write_buffer && -1 == DJPT_flush_buffer(peer))
    return -1;

  peer->write_buffer_fill = 0;
#endif

  while(remaining)
  {
    int res = send(peer->fd, i, remaining, MSG_NOSIGNAL);

    if(res <= 0)
      return -1;

    i += res;
    remaining -= res;
  }

  return size;
}

#ifndef DJPT_CLIENT
static int
DJPT_column_scan_callback(const char* row, const char* column, const void* data, size_t data_size, uint64_t* timestamp, void* arg)
{
  struct DJPT_peer* peer = arg;
  struct DJPT_request response;
  size_t rowlen;

  rowlen = strlen(row);

  response.command = DJPT_REQ_VALUE;
  response.size = htonl(sizeof(response) + data_size + rowlen + 1);

  if(-1 == DJPT_write_buffered(peer, &response, sizeof(response)))
    return -1;

  if(-1 == DJPT_write_buffered(peer, row, rowlen + 1))
    return -1;

  if(-1 == DJPT_write_buffered(peer, data, data_size))
    return -1;

  if(peer->limit_arg && !--peer->limit_arg)
    return 1;

  return 0;
}

static int
DJPT_eval_callback(struct JPT_cons* data, void* arg)
{
  struct DJPT_peer* peer = arg;
  struct DJPT_request response;
  char row[1] = { 0 };

  response.command = DJPT_REQ_VALUE;

  while(data)
  {
    if(data->car)
      DJPT_eval_callback(data->car, peer);
    else
    {
      response.size = htonl(sizeof(response) + data->car_value.size + 1);

      if(-1 == DJPT_write_buffered(peer, &response, sizeof(response)))
        return -1;

      if(-1 == DJPT_write_buffered(peer, row, 1))
        return -1;

      if(-1 == DJPT_write_buffered(peer, data->car_value.data, data->car_value.size))
        return -1;
    }

    data = data->cdr;
  }

  return 0;
}

static int
DJPT_write_error(struct DJPT_peer* peer)
{
  struct DJPT_request_error error;
  const char* msg = jpt_last_error();
  size_t msglen = strlen(msg);

  error.command = DJPT_REQ_ERROR;
  error.size = htonl(sizeof(error) + msglen + 1);
  error.error = htonl(errno);

  if(-1 == DJPT_write_all(peer, &error, sizeof(error)))
    return -1;

  if(-1 == DJPT_write_all(peer, msg, msglen + 1))
    return -1;

  return 0;
}

static void
DJPT_release_jpt_handle(struct DJPT_jpt_handle* handle)
{
  pthread_mutex_lock(&DJPT_jpt_handle_lock);

  if(!--handle->refcount)
  {
    jpt_close(handle->info);
    free(handle->filename);

    handle->info = 0;
    handle->filename = 0;
  }

  pthread_mutex_unlock(&DJPT_jpt_handle_lock);
}
#endif

char*
DJPT_get_user_name()
{
  char* result = 0;
  uid_t euid;
  struct passwd* pwent;

  euid = getuid();

  while(0 != (pwent = getpwent()))
  {
    if(pwent->pw_uid == euid)
    {
      result = strdup(pwent->pw_name);

      break;
    }
  }

  endpwent();

  return result;
}

void
DJPT_peer_loop(struct DJPT_peer* peer, int flags)
{
  size_t i;
  struct DJPT_request* request = 0;

  for(;;)
  {
    struct DJPT_request header;

    if(-1 == DJPT_read_all(peer, &header, 5))
      goto done;

    header.size = ntohl(header.size);

    if(header.size < 5)
      goto done;

    if(header.size > DJPT_MAX_REQUEST_SIZE)
      goto done;

    request = malloc(header.size);

    if(!request)
      goto done;

    memcpy(request, &header, 5);

    if(-1 == DJPT_read_all(peer, request->data, request->size - 5))
      goto done;

    switch(request->command)
    {
    case DJPT_REQ_DISCONNECT:

      goto done;

#ifndef DJPT_CLIENT
    case DJPT_REQ_OPEN:

      {
        struct DJPT_request_open* open = (void*) request;
        struct JPT_info* info = 0;
        struct DJPT_jpt_handle* old_handle = 0;

        if(peer->jpt_handle)
          old_handle = &DJPT_jpt_handles[peer->jpt_handle - 1];

        pthread_mutex_lock(&DJPT_jpt_handle_lock);

        for(i = 0; i < DJPT_jpt_handle_alloc; ++i)
        {
          if(DJPT_jpt_handles[i].info
          && !strcmp(DJPT_jpt_handles[i].filename, open->filename))
          {
            struct DJPT_request eof;

            ++DJPT_jpt_handles[i].refcount;
            peer->jpt_handle = i + 1;
            peer->db = DJPT_jpt_handles[i].info;

            pthread_mutex_unlock(&DJPT_jpt_handle_lock);

            eof.command = DJPT_REQ_EOF;
            eof.size = htonl(sizeof(eof));

            if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
              goto done;

            break;
          }
        }

        if(old_handle)
          DJPT_release_jpt_handle(old_handle);

        if(i != DJPT_jpt_handle_alloc)
          break;

        syslog(LOG_INFO, "opening '%s'...", open->filename);

        info = jpt_init(open->filename, DJPT_MEMTABLE_SIZE, 0);

        if(!info)
        {
          pthread_mutex_unlock(&DJPT_jpt_handle_lock);

          syslog(LOG_INFO, "failed to open '%s': %s", open->filename, jpt_last_error());

          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request eof;

          for(i = 0; i < DJPT_jpt_handle_alloc; ++i)
            if(!DJPT_jpt_handles[i].info)
              break;

          DJPT_jpt_handles[i].info = info;
          DJPT_jpt_handles[i].filename = strdup(open->filename);
          DJPT_jpt_handles[i].refcount = 1;

          peer->jpt_handle = i + 1;
          peer->db = info;

          pthread_mutex_unlock(&DJPT_jpt_handle_lock);

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;

    case DJPT_REQ_INSERT:

      {
        struct DJPT_request_insert* insert = (void*) request;

        const char* row = insert->data;
        const char* column = insert->data + ntohl(insert->column_offset);
        const char* value = insert->data + ntohl(insert->value_offset);

        size_t size = insert->size - ntohl(insert->value_offset) - sizeof(struct DJPT_request_insert);
        int jptflags = insert->flags & 0x3F;

        if(-1 == jpt_insert(peer->db, row, column, value, size, jptflags))
        {
          if(insert->flags & DJPT_IGNORE_RESULT)
            break;

          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else if(!(insert->flags & DJPT_IGNORE_RESULT))
        {
          struct DJPT_request eof;

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;

    case DJPT_REQ_REMOVE:

      {
        struct DJPT_request_remove* remove = (void*) request;

        const char* row = remove->data;
        const char* column = remove->data + ntohl(remove->column_offset);

        if(-1 == jpt_remove(peer->db, row, column))
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request eof;

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;

    case DJPT_REQ_REMOVE_COLUMN:

      {
        struct DJPT_request_remove_column* remove_column = (void*) request;

        if(-1 == jpt_remove_column(peer->db, remove_column->column, remove_column->flags))
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request eof;

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;

    case DJPT_REQ_HAS_KEY:

      {
        struct DJPT_request_has_key* has_key = (void*) request;

        const char* row = has_key->data;
        const char* column = has_key->data + ntohl(has_key->column_offset);

        if(-1 == jpt_has_key(peer->db, row, column))
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request eof;

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;

    case DJPT_REQ_HAS_COLUMN:

      {
        struct DJPT_request_has_column* has_column = (void*) request;

        if(-1 == jpt_has_column(peer->db, has_column->column))
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request eof;

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;


    case DJPT_REQ_GET:

      {
        struct DJPT_request_get* get = (void*) request;

        const char* row = get->data;
        const char* column = get->data + ntohl(get->column_offset);
        void* data;
        size_t data_size;

        if(-1 == jpt_get(peer->db, row, column, &data, &data_size))
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request response;

          response.command = DJPT_REQ_VALUE;
          response.size = htonl(sizeof(response) + data_size);

          if(-1 == DJPT_write_buffered(peer, &response, sizeof(response)))
            goto done;

          if(-1 == DJPT_write_buffered(peer, data, data_size))
            goto done;

          free(data);
        }
      }

      break;

    case DJPT_REQ_COLUMN_SCAN:

      {
        struct DJPT_request_column_scan* column_scan = (void*) request;

        peer->limit_arg = ntohl(column_scan->limit);

        if(-1 == jpt_column_scan(peer->db, column_scan->column, DJPT_column_scan_callback, peer))
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request eof;

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;

    case DJPT_REQ_GET_COUNTER:

      {
        struct DJPT_request_get_counter* get_counter = (void*) request;
        uint64_t result;

        result = jpt_get_counter(peer->db, get_counter->name);

        if(result == (uint64_t) ~0ULL)
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request response;

          response.command = DJPT_REQ_VALUE;
          response.size = htonl(sizeof(response) + 8);

          if(-1 == DJPT_write_buffered(peer, &response, sizeof(response)))
            goto done;

          if(-1 == DJPT_write_buffered(peer, &result, 8))
            goto done;
        }
      }

      break;

    case DJPT_REQ_EVAL_STRING:

      {
        struct DJPT_request_eval_string* eval_string = (void*) request;

        if(-1 == jpt_eval(peer->db, eval_string->program, DJPT_eval_callback, peer))
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request eof;

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;

    case DJPT_REQ_COMPACT:

      {
        if(-1 == jpt_compact(peer->db))
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request eof;

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;

    case DJPT_REQ_MAJOR_COMPACT:

      {
        if(-1 == jpt_major_compact(peer->db))
        {
          if(-1 == DJPT_write_error(peer))
            goto done;
        }
        else
        {
          struct DJPT_request eof;

          eof.command = DJPT_REQ_EOF;
          eof.size = htonl(sizeof(eof));

          if(-1 == DJPT_write_all(peer, &eof, sizeof(eof)))
            goto done;
        }
      }

      break;
#endif
    }

#ifndef DJPT_CLIENT
    if(peer->write_buffer_fill)
      if(-1 == DJPT_flush_buffer(peer))
        goto done;
#endif

    free(request);
    request = 0;
  }

done:

  free(request);

#ifndef DJPT_CLIENT
  if(peer->jpt_handle)
  {
    DJPT_release_jpt_handle(&DJPT_jpt_handles[peer->jpt_handle - 1]);

    peer->jpt_handle = 0;
    peer->db = 0;
  }
#endif
}
