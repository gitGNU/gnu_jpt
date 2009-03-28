#include <errno.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

#include "djpt.h"
#include "djpt_internal.h"
#include "jpt.h"

static __thread int DJPT_errno = 0;
static __thread char* DJPT_last_error = 0;

const char*
djpt_last_error()
{
  return DJPT_last_error ? DJPT_last_error : strerror(errno);
}

static void
DJPT_clear_error()
{
  free(DJPT_last_error);
  DJPT_errno = 0;
  DJPT_last_error = 0;
}

static void
DJPT_parse_error(struct DJPT_request_error* error)
{
  errno = ntohl(error->error);
  if(error->message[0])
    asprintf(&DJPT_last_error, "Remote error: %s", strdup(error->message));
  else
    asprintf(&DJPT_last_error, "Remote system error: %s", strerror(errno));
}

static void
DJPT_set_invalid_response()
{
  DJPT_last_error = strdup("Invalid response from djptd server");
}

static struct DJPT_request*
DJPT_read_request(struct DJPT_peer* peer)
{
  struct DJPT_request header;
  struct DJPT_request* request;

  if(-1 == DJPT_read_all(peer, &header, 5))
    return 0;

  header.size = ntohl(header.size);

  if(header.size < 5)
  {
    asprintf(&DJPT_last_error, "Too small request size from peer: Got %u bytes", (unsigned int) header.size);

    return 0;
  }

  if(header.size > DJPT_MAX_REQUEST_SIZE)
  {
    asprintf(&DJPT_last_error, "Too large request size from peer: Got %u bytes", (unsigned int) header.size);

    return 0;
  }

  request = malloc(header.size);

  if(!request)
  {
    asprintf(&DJPT_last_error, "Allocating %u bytes for peer request failed: %s", (unsigned int) header.size, strerror(errno));

    return 0;
  }

  memcpy(request, &header, 5);

  if(-1 == DJPT_read_all(peer, request->data, request->size - 5))
  {
    asprintf(&DJPT_last_error, "Read error while reading request from peer: %s", strerror(errno));

    free(request);

    return 0;
  }

  if(request->command == DJPT_REQ_ERROR)
  {
    DJPT_parse_error((struct DJPT_request_error*) request);

    return 0;
  }

  return request;
}

static int
DJPT_open(struct DJPT_peer* peer, const char* filename)
{
  struct DJPT_request_open* openrq;
  struct DJPT_request* response;
  size_t filenamelen;
  size_t size;
  int res;

  filenamelen = strlen(filename);

  size = sizeof(struct DJPT_request_open)
       + filenamelen + 1;

  openrq = malloc(size);
  openrq->command = DJPT_REQ_OPEN;
  openrq->size = htonl(size);
  strcpy(openrq->filename, filename);

  if(-1 == DJPT_write_all(peer, openrq, size))
    return -1;

  free(openrq);

  response = DJPT_read_request(peer);

  if(response && response->command == DJPT_REQ_EOF)
    res = 0;
  else
    res = -1;

  free(response);

  return res;
}

struct DJPT_info*
djpt_init(const char* database)
{
  struct sockaddr_un unixaddr;
  struct msghdr msghdr;
  struct iovec iov;
  struct cmsghdr* cmsg;
  struct ucred cred;
  struct DJPT_info* info;
  struct DJPT_peer* peer;
  char* user_name;
  char dummy = 0;
  int fd;

  DJPT_clear_error();

  user_name = DJPT_get_user_name();

  if(!user_name)
  {
    asprintf(&DJPT_last_error, "failed to determine own user name");
    errno = EINVAL;

    return 0;
  }

  if(-1 == (fd = socket(PF_UNIX, SOCK_STREAM, 0)))
    return 0;

  memset(&unixaddr, 0, sizeof(unixaddr));
  unixaddr.sun_family = AF_UNIX;
  strcpy(unixaddr.sun_path + 1, "DISTRIBUTED_JUNOPLAY_TABLE");
  strcat(unixaddr.sun_path + 1, "%");
  strcat(unixaddr.sun_path + 1, user_name);

  if(-1 == connect(fd, (struct sockaddr*) &unixaddr, sizeof(unixaddr)))
  {
    int status;
    pid_t pid;

    pid = fork();

    if(pid == -1)
    {
      close(fd);

      return 0;
    }

    if(!pid)
    {
      int i;
      char* args[2];

      for(i = 3; i < 1024; ++i)
        close(i);

      args[0] = BINDIR "/djptd";
      args[1] = 0;

      execve(args[0], args, environ);

      exit(EXIT_FAILURE);
    }

    waitpid(pid, &status, 0);

    if(-1 == connect(fd, (struct sockaddr*) &unixaddr, sizeof(unixaddr)))
    {
      close(fd);

      return 0;
    }
  }

  cred.pid = getpid();
  cred.uid = geteuid();
  cred.gid = getegid();

  cmsg = alloca(CMSG_SPACE(sizeof(struct ucred)));
  memcpy(CMSG_DATA(cmsg), &cred, sizeof(struct ucred));
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_CREDENTIALS;
  cmsg->cmsg_len = sizeof(*cmsg) + sizeof(struct ucred);

  memset(&msghdr, 0, sizeof(msghdr));
  iov.iov_base = &dummy;
  iov.iov_len = 1;
  msghdr.msg_iov = &iov;
  msghdr.msg_iovlen = 1;
  msghdr.msg_control = cmsg;
  msghdr.msg_controllen = CMSG_ALIGN(cmsg->cmsg_len);

  for(;;)
  {
    if(-1 != sendmsg(fd, &msghdr, 0))
      break;

    if(errno == EINTR)
      continue;

    close(fd);

    return 0;
  }

  peer = malloc(sizeof(struct DJPT_peer));
  memset(peer, 0, sizeof(struct DJPT_peer));
  peer->fd = fd;

  if(-1 == DJPT_open(peer, database))
  {
    free(peer);
    close(fd);

    return 0;
  }

  info = malloc(sizeof(struct DJPT_info));
  info->peer = peer;

  return info;
}

void
djpt_close(struct DJPT_info* info)
{
  DJPT_clear_error();

  free(info->peer->read_buffer);
  free(info->peer->write_buffer);
  close(info->peer->fd);
  free(info->peer);
  free(info);
}

int
djpt_insert(struct DJPT_info* info,
            const char* row, const char* column,
            const void* value, size_t value_size, int flags)
{
  struct DJPT_request_insert* insert;
  struct DJPT_request* response;
  size_t rowlen, columnlen;
  size_t size;
  int res;

  DJPT_clear_error();

  rowlen = strlen(row);
  columnlen = strlen(column);

  size = sizeof(struct DJPT_request_insert)
       + rowlen + 1
       + columnlen + 1
       + value_size;

  insert = malloc(size);
  insert->command = DJPT_REQ_INSERT;
  insert->size = htonl(size);
  insert->flags = flags;
  insert->column_offset = htonl(rowlen + 1);
  insert->value_offset = htonl(rowlen + 1 + columnlen + 1);
  strcpy(insert->data, row);
  strcpy(insert->data + rowlen + 1, column);
  memcpy(insert->data + rowlen + 1 + columnlen + 1, value, value_size);

  if(-1 == DJPT_write_all(info->peer, insert, size))
    return -1;

  free(insert);

  if(flags & DJPT_IGNORE_RESULT)
    return 0;

  response = DJPT_read_request(info->peer);

  if(response && response->command == DJPT_REQ_EOF)
    res = 0;
  else
    res = -1;

  free(response);

  return res;
}

int
djpt_remove(struct DJPT_info* info, const char* row, const char* column)
{
  struct DJPT_request_remove* remove;
  struct DJPT_request* response;
  size_t rowlen, columnlen;
  size_t size;
  int res;

  DJPT_clear_error();

  rowlen = strlen(row);
  columnlen = strlen(column);

  size = sizeof(struct DJPT_request_remove)
       + rowlen + 1
       + columnlen + 1;

  remove = malloc(size);
  remove->command = DJPT_REQ_REMOVE;
  remove->size = htonl(size);
  remove->column_offset = htonl(rowlen + 1);
  strcpy(remove->data, row);
  strcpy(remove->data + rowlen + 1, column);

  if(-1 == DJPT_write_all(info->peer, remove, size))
    return -1;

  free(remove);

  response = DJPT_read_request(info->peer);

  if(response && response->command == DJPT_REQ_EOF)
    res = 0;
  else
    res = -1;

  free(response);

  return res;
}

int
djpt_remove_column(struct DJPT_info* info, const char* column, int flags)
{
  struct DJPT_request_remove_column* remove;
  struct DJPT_request* response;
  size_t columnlen;
  size_t size;
  int res;

  DJPT_clear_error();

  columnlen = strlen(column);

  size = sizeof(struct DJPT_request_remove_column)
       + columnlen + 1;

  remove = malloc(size);
  remove->command = DJPT_REQ_REMOVE_COLUMN;
  remove->size = htonl(size);
  remove->flags = flags;
  strcpy(remove->column, column);

  if(-1 == DJPT_write_all(info->peer, remove, size))
    return -1;

  free(remove);

  response = DJPT_read_request(info->peer);

  if(response && response->command == DJPT_REQ_EOF)
    res = 0;
  else
    res = -1;

  free(response);

  return res;
}

int
djpt_create_column(struct DJPT_info* info, const char* column, int flags)
{
  DJPT_clear_error();

  return -1;
}

int
djpt_has_key(struct DJPT_info* info, const char* row, const char* column)
{
  struct DJPT_request_has_key* has_key;
  struct DJPT_request* response;
  size_t rowlen, columnlen;
  size_t size;
  int res;

  DJPT_clear_error();

  rowlen = strlen(row);
  columnlen = strlen(column);

  size = sizeof(struct DJPT_request_has_key)
       + rowlen + 1
       + columnlen + 1;

  has_key = malloc(size);
  has_key->command = DJPT_REQ_HAS_KEY;
  has_key->size = htonl(size);
  has_key->column_offset = htonl(rowlen + 1);
  strcpy(has_key->data, row);
  strcpy(has_key->data + rowlen + 1, column);

  if(-1 == DJPT_write_all(info->peer, has_key, size))
    return -1;

  free(has_key);

  response = DJPT_read_request(info->peer);

  if(response && response->command == DJPT_REQ_EOF)
    res = 0;
  else
    res = -1;

  free(response);

  return res;
}

int
djpt_has_column(struct DJPT_info* info, const char* column)
{
  struct DJPT_request_has_column* has_column;
  struct DJPT_request* response;
  size_t columnlen;
  size_t size;
  int res;

  DJPT_clear_error();

  columnlen = strlen(column);

  size = sizeof(struct DJPT_request_has_column)
       + columnlen + 1;

  has_column = malloc(size);
  has_column->command = DJPT_REQ_HAS_COLUMN;
  has_column->size = htonl(size);
  strcpy(has_column->column, column);

  if(-1 == DJPT_write_all(info->peer, has_column, size))
    return -1;

  free(has_column);

  response = DJPT_read_request(info->peer);

  if(response && response->command == DJPT_REQ_EOF)
    res = 0;
  else
    res = -1;

  free(response);

  return res;
}

int
djpt_get(struct DJPT_info* info, const char* row, const char* column,
         void** value, size_t* value_size)
{
  struct DJPT_request_get* get;
  struct DJPT_request* response;
  size_t rowlen, columnlen;
  size_t size;
  int res;

  DJPT_clear_error();

  *value = 0;
  *value_size = 0;

  rowlen = strlen(row);
  columnlen = strlen(column);

  size = sizeof(struct DJPT_request_get)
       + rowlen + 1
       + columnlen + 1;

  get = malloc(size);
  get->command = DJPT_REQ_GET;
  get->size = htonl(size);
  get->column_offset = htonl(rowlen + 1);
  strcpy(get->data, row);
  strcpy(get->data + rowlen + 1, column);

  if(-1 == DJPT_write_all(info->peer, get, size))
    return -1;

  free(get);

  response = DJPT_read_request(info->peer);

  if(response && response->command == DJPT_REQ_VALUE)
  {
    struct DJPT_request_value* ret_value = (void*) response;

    *value_size = ret_value->size - sizeof(struct DJPT_request_value);
    *value = malloc(*value_size);

    memcpy(*value, ret_value->value, *value_size);

    res = 0;
  }
  else
  {
    if(response)
      DJPT_set_invalid_response();

    res = -1;
  }

  free(response);

  return res;
}

int
djpt_get_fixed(struct DJPT_info* info, const char* row, const char* column,
               void* value, size_t value_size)
{
  void* tmp;
  size_t tmpsize;

  DJPT_clear_error();

  djpt_get(info, row, column, &tmp, &tmpsize);

  if(tmpsize > value_size)
  {
    errno = E2BIG;

    free(tmp);

    return -1;
  }

  if(value_size > tmpsize)
    value_size = tmpsize;

  memcpy(value, tmp, value_size);

  free(tmp);

  return value_size;
}

int
djpt_scan(struct DJPT_info* info, djpt_cell_callback callback, void* arg)
{
  DJPT_clear_error();

  return -1;
}

int
djpt_column_scan(struct DJPT_info* info, const char* column,
                 djpt_cell_callback callback, void* arg, size_t limit)
{
  uint64_t timestamp;
  struct DJPT_request_column_scan* column_scan;
  struct DJPT_request* response;
  size_t columnlen;
  size_t size;
  int res = 0;

  char tempname[64];
  int tempfd;
  struct JPT_info* tempdb;

  DJPT_clear_error();

  columnlen = strlen(column);

  size = sizeof(struct DJPT_request_column_scan) + columnlen + 1;

  column_scan = malloc(size);
  column_scan->command = DJPT_REQ_COLUMN_SCAN;
  column_scan->limit = htonl(limit);
  column_scan->size = htonl(size);
  strcpy(column_scan->column, column);

  timestamp = jpt_gettime();

  if(-1 == DJPT_write_all(info->peer, column_scan, size))
    return -1;

  free(column_scan);

  response = DJPT_read_request(info->peer);

  if(!response)
  {
    free(response);

    return -1;
  }

  strcpy(tempname, "/tmp/djpt.XXXXXX");

  tempfd = mkstemp(tempname);

  tempdb = jpt_init(tempname, 1024 * 1024, 0);

  if(!tempdb)
  {
    asprintf(&DJPT_last_error, "Failed to create temporary table for scanning: %s", jpt_last_error());

    close(tempfd);
    unlink(tempname);
    strcat(tempname, ".log");
    unlink(tempname);

    free(response);

    return -1;
  }

  close(tempfd);
  unlink(tempname);
  strcat(tempname, ".log");
  unlink(tempname);

  while(res == 0)
  {
    if(response)
    {
      if(response->command == DJPT_REQ_VALUE)
      {
        struct DJPT_request_value* ret_value = (void*) response;

        const char* row = ret_value->value;
        const char* data = strchr(row, 0) + 1;
        size_t size = ret_value->size - sizeof(struct DJPT_request_value) - (data - row);

        jpt_insert_timestamp(tempdb, row, column, data, size, &timestamp, 0);

        res = 0;
      }
      else if(response->command == DJPT_REQ_EOF)
      {
        free(response);

        break;
      }
      else
      {
        asprintf(&DJPT_last_error, "Got unexpected response to column scan: %d", response->command);
        res = -1;
      }
    }
    else
      res = -1;

    free(response);

    response = DJPT_read_request(info->peer);
  }

  if(res == 0)
    res = jpt_column_scan(tempdb, column, (jpt_cell_callback) callback, arg);

  jpt_close(tempdb);

  return res;
}

int
djpt_eval(struct DJPT_info* info, const char* program,
          djpt_eval_callback callback, void* arg)
{
  struct DJPT_request_eval_string* eval_string;
  struct DJPT_request* response;
  size_t programlen;
  size_t size;
  int res = 0;

  DJPT_clear_error();

  programlen = strlen(program);

  size = sizeof(struct DJPT_request_eval_string) + programlen + 1;

  eval_string = malloc(size);
  eval_string->command = DJPT_REQ_EVAL_STRING;
  eval_string->size = htonl(size);
  strcpy(eval_string->program, program);

  if(-1 == DJPT_write_all(info->peer, eval_string, size))
    return -1;

  free(eval_string);

  for(;;)
  {
    response = DJPT_read_request(info->peer);

    if(response->command == DJPT_REQ_VALUE)
    {
      struct DJPT_request_value* ret_value = (void*) response;

      if(-1 == callback(ret_value->value + 1, ret_value->size - sizeof(struct DJPT_request_value) - 1, arg))
        res = -1;
    }
    else if(response->command == DJPT_REQ_EOF)
    {
      free(response);

      break;
    }
    else
      res = -1;

    free(response);
  }

  return res;
}

uint64_t
djpt_get_counter(struct DJPT_info* info, const char* name)
{
  struct DJPT_request_get_counter* get;
  struct DJPT_request* response;
  size_t namelen;
  size_t size;
  uint64_t result;

  DJPT_clear_error();

  namelen = strlen(name);

  size = sizeof(struct DJPT_request_get_counter) + namelen + 1;

  get = malloc(size);
  get->command = DJPT_REQ_GET_COUNTER;
  get->size = htonl(size);
  strcpy(get->name, name);

  if(-1 == DJPT_write_all(info->peer, get, size))
    return (uint64_t) ~0ULL;

  free(get);

  response = DJPT_read_request(info->peer);

  if(response && response->command == DJPT_REQ_VALUE)
  {
    struct DJPT_request_value* ret_value = (void*) response;

    memcpy(&result, ret_value->value, 8);
  }
  else
    result = (uint64_t) ~0ULL;

  free(response);

  return result;
}

int
djpt_compact(struct DJPT_info* info)
{
  struct DJPT_request_compact* compact;
  struct DJPT_request* response;
  size_t size;
  int res;

  DJPT_clear_error();

  size = sizeof(struct DJPT_request_compact);

  compact = malloc(size);
  compact->command = DJPT_REQ_COMPACT;
  compact->size = htonl(size);

  if(-1 == DJPT_write_all(info->peer, compact, size))
    return -1;

  free(compact);

  response = DJPT_read_request(info->peer);

  if(response && response->command == DJPT_REQ_EOF)
    res = 0;
  else
    res = -1;

  free(response);

  return res;
}

int
djpt_major_compact(struct DJPT_info* info)
{
  struct DJPT_request_major_compact* major_compact;
  struct DJPT_request* response;
  size_t size;
  int res;

  DJPT_clear_error();

  size = sizeof(struct DJPT_request_major_compact);

  major_compact = malloc(size);
  major_compact->command = DJPT_REQ_MAJOR_COMPACT;
  major_compact->size = htonl(size);

  if(-1 == DJPT_write_all(info->peer, major_compact, size))
    return -1;

  free(major_compact);

  response = DJPT_read_request(info->peer);

  if(response && response->command == DJPT_REQ_EOF)
    res = 0;
  else
    res = -1;

  free(response);

  return res;
}
