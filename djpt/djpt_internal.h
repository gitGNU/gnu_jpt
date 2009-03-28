/*  Data types internal to the djpt library.
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

#ifndef DJPT_INTERNAL_H_
#define DJPT_INTERNAL_H_ 1

#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>

#include "jpt.h"

#define PACKED __attribute__((packed))

#define DJPT_PEER_HAS_DATA      0x0002

#define DJPT_TCP_PORT 24782
#define DJPT_MAX_REQUEST_SIZE (64 * 1024 * 1024)

struct DJPT_peer
{
  int fd;

  struct sockaddr addr;
  socklen_t addrlen;

  pthread_t thread;

  unsigned int flags;

  struct JPT_info* db;

  size_t jpt_handle;

  char* read_buffer;
  size_t read_buffer_size;
  size_t read_buffer_fill;
  size_t read_buffer_offset;

  char* write_buffer;
  size_t write_buffer_size;
  size_t write_buffer_fill;

  uint32_t limit_arg;
};

struct DJPT_info
{
  struct DJPT_peer* peer;
};

struct DJPT_jpt_handle
{
  struct JPT_info* info;
  char* filename;
  size_t refcount;
};

#define DJPT_REQ_DISCONNECT     2
#define DJPT_REQ_OPEN           3
#define DJPT_REQ_INSERT         4
#define DJPT_REQ_REMOVE         5
#define DJPT_REQ_REMOVE_COLUMN  6
#define DJPT_REQ_HAS_KEY        7
#define DJPT_REQ_HAS_COLUMN     8
#define DJPT_REQ_GET            9
#define DJPT_REQ_COLUMN_SCAN    10
#define DJPT_REQ_VALUE          11
#define DJPT_REQ_EOF            12
#define DJPT_REQ_ERROR          13
#define DJPT_REQ_GET_COUNTER    14
#define DJPT_REQ_EVAL_STRING    15
#define DJPT_REQ_COMPACT        16
#define DJPT_REQ_MAJOR_COMPACT  17

struct DJPT_request
{
  uint32_t size;
  uint8_t command;
  char data[0];
} PACKED;

struct DJPT_request_error
{
  uint32_t size;
  uint8_t command;
  uint32_t error;
  char message[0];
} PACKED;

struct DJPT_request_challenge
{
  uint32_t size;
  uint8_t command;
  uint8_t challenge[16];
} PACKED;

struct DJPT_request_response
{
  uint32_t size;
  uint8_t command;
  uint8_t response[20];
} PACKED;

struct DJPT_request_disconnect
{
  uint32_t size;
  uint8_t command;
} PACKED;

struct DJPT_request_open
{
  uint32_t size;
  uint8_t command;
  char filename[0];
} PACKED;

struct DJPT_request_insert
{
  uint32_t size;
  uint8_t command;
  uint8_t flags;
  uint32_t column_offset;
  uint32_t value_offset;
  char data[0];
} PACKED;

struct DJPT_request_remove
{
  uint32_t size;
  uint8_t command;
  uint32_t column_offset;
  char data[0];
} PACKED;

struct DJPT_request_remove_column
{
  uint32_t size;
  uint8_t command;
  uint8_t flags;
  char column[0];
} PACKED;

struct DJPT_request_has_key
{
  uint32_t size;
  uint8_t command;
  uint32_t column_offset;
  char data[0];
} PACKED;

struct DJPT_request_has_column
{
  uint32_t size;
  uint8_t command;
  char column[0];
} PACKED;

struct DJPT_request_get
{
  uint32_t size;
  uint8_t command;
  uint32_t column_offset;
  char data[0];
} PACKED;

struct DJPT_request_column_scan
{
  uint32_t size;
  uint8_t command;
  uint32_t limit;
  char column[0];
} PACKED;

struct DJPT_request_value
{
  uint32_t size;
  uint8_t command;
  char value[0];
} PACKED;

struct DJPT_request_get_counter
{
  uint32_t size;
  uint8_t command;
  char name[0];
} PACKED;

struct DJPT_request_eval_string
{
  uint32_t size;
  uint8_t command;
  char program[0];
} PACKED;

struct DJPT_request_compact
{
  uint32_t size;
  uint8_t command;
} PACKED;

struct DJPT_request_major_compact
{
  uint32_t size;
  uint8_t command;
} PACKED;

extern struct DJPT_jpt_handle DJPT_jpt_handles[];
extern size_t DJPT_jpt_handle_alloc;

ssize_t
DJPT_read_all(struct DJPT_peer* peer, void* target, size_t size);

ssize_t
DJPT_write_all(struct DJPT_peer* peer, const void* target, size_t size);

char*
DJPT_get_user_name();

void
DJPT_peer_loop(struct DJPT_peer* peer, int flags);

#endif /* !DJPT_INTERNAL_H_ */
