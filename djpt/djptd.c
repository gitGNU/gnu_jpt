/*  djpt daemon entry point.
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

#if HAVE_CONFIG_H
#include "config.h"
#endif

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <syslog.h>
#include <unistd.h>
#include <getopt.h>

#include "djpt_internal.h"
#include "jpt.h"

static struct option long_options[] =
{
  { "help", 0, 0, 'h' },
  { "version", 0, 0, 'v' },
  { 0, 0, 0, 0 }
};

static int unixfd = -1;

static void help(const char* argv0)
{
  printf("Usage: %s [OPTION]...\n"
         "Distributed jpt daemon\n"
         "\n"
         "Mandatory arguments to long options are mandatory for short"
         " options too\n"
         "\n"
         " -d, --debug                print errors to stderr.  Implies -n\n"
         " -n, --no-detach            do not detach from console\n"
         "     --help     display this help and exit\n"
         "     --version  display version information and exit\n"
         "\n"
         "Report bugs to <morten@rashbox.org>.\n", argv0);
}

static void*
peer_thread(void* arg)
{
  char buf[CMSG_SPACE(sizeof(struct ucred))];
  struct msghdr msghdr;
  struct iovec iov;
  struct cmsghdr* cmsg;
  struct DJPT_peer* peer;
  int auth_ok = 0;
  char dummy;

  peer = arg;

  memset(&msghdr, 0, sizeof(msghdr));
  iov.iov_base = &dummy;
  iov.iov_len = 1;
  msghdr.msg_iov = &iov;
  msghdr.msg_iovlen = 1;
  msghdr.msg_control = (void*) buf;
  msghdr.msg_controllen = CMSG_SPACE(sizeof(struct ucred));

  if(-1 == recvmsg(peer->fd, &msghdr, 0))
  {
    fprintf(stderr, "No receive: %s\n", strerror(errno));

    goto done;
  }

  for(cmsg = CMSG_FIRSTHDR(&msghdr); cmsg; cmsg = CMSG_NXTHDR(&msghdr,cmsg))
  {
    if(cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_CREDENTIALS)
    {
      struct ucred* cred;
      cred = (struct ucred*) CMSG_DATA(cmsg);

      if(cred->uid == getuid())
        auth_ok = 1;

      break;
    }
  }

  if(!auth_ok)
    goto done;

  syslog(LOG_INFO, "accepted a new peer connection");

  DJPT_peer_loop(peer, 0);

done:
  free(peer->read_buffer);
  free(peer->write_buffer);
  close(peer->fd);

  free(peer);

  return 0;
}

void
exithandler()
{
  size_t i;

  close(unixfd);

  for(i = 0; i < DJPT_jpt_handle_alloc; ++i)
    if(DJPT_jpt_handles[i].info)
      jpt_close(DJPT_jpt_handles[i].info);
}

void
sighandler(int signal)
{
  switch(signal)
  {
  case SIGHUP:

    {
      int i;

      for(i = 0; i < DJPT_jpt_handle_alloc; ++i)
      {
        if(DJPT_jpt_handles[i].info)
        {
          /* XXX: Reopen */
        }
      }
    }

    break;

  default:

    exit(EXIT_SUCCESS);
  }
}

int
main(int argc, char** argv)
{
  char* user_name;
  int debug = 0;
  int nodetach = 0;
  int one = 1;

  signal(SIGTERM, sighandler);
  signal(SIGINT, sighandler);
  signal(SIGHUP, sighandler);
  signal(SIGUSR1, SIG_IGN);
  signal(SIGUSR2, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);

  for(;;)
  {
    int optindex = 0;
    int c;

    c = getopt_long(argc, argv, "dn", long_options, &optindex);

    if (c == -1)
      break;

    switch(c)
    {
    case 'd':

      debug = 1;
      nodetach = 1;

      break;

    case 'n':

      nodetach = 1;

      break;

    case 'h':

      help(argv[0]);

      return EXIT_SUCCESS;

    case 'v':

      printf("djptd %s - Distributed JPT daemon\n", PACKAGE_VERSION);
      printf("Copyright (C) 2007 Morten Hustveit\n"
          "This is free software.  You may redistribute copies of it under the terms of\n"
          "the GNU General Public License <http://www.gnu.org/licenses/gpl.html>.\n"
          "There is NO WARRANTY, to the extent permitted by law.\n"
          "\n"
          "Authors:\n"
          "  Morten Hustveit\n");

      return EXIT_SUCCESS;

    case '?':

      fprintf(stderr, "Try `%s --help' for more information.\n", argv[0]);

      return EXIT_FAILURE;
    }
  }

  if(optind != argc)
  {
    fprintf(stderr, "Usage: %s [OPTIONS]...\n", argv[0]);

    return EXIT_FAILURE;
  }

  openlog("djptd", LOG_PID, LOG_DAEMON);

  user_name = DJPT_get_user_name();

  if(!user_name)
  {
    fprintf(stderr, "Failed to retrieve own user name: %s\n", strerror(errno));

    return EXIT_FAILURE;
  }

  syslog(LOG_INFO, "starting djptd as %s", user_name);

  if(-1 == (unixfd = socket(PF_UNIX, SOCK_STREAM, 0)))
  {
    fprintf(stderr, "Failed to create Unix socket: %s\n", strerror(errno));

    return EXIT_FAILURE;
  }

  struct sockaddr_un unixaddr;
  memset(&unixaddr, 0, sizeof(unixaddr));
  unixaddr.sun_family = AF_UNIX;
  strcpy(unixaddr.sun_path + 1, "DISTRIBUTED_JUNOPLAY_TABLE");
  strcat(unixaddr.sun_path + 1, "%");
  strcat(unixaddr.sun_path + 1, user_name);

  if(-1 == bind(unixfd, (struct sockaddr*) &unixaddr, sizeof(unixaddr)))
  {
    fprintf(stderr, "Failed to bind Unix socket: %s\n", strerror(errno));

    return EXIT_FAILURE;
  }

  if(-1 == listen(unixfd, 16))
  {
    fprintf(stderr, "Failed to start listening on Unix socket: %s\n", strerror(errno));

    return EXIT_FAILURE;
  }

  atexit(exithandler);

  if(!nodetach)
    daemon(0, 0);

  for(;;)
  {
    fd_set readfds;
    struct DJPT_peer* peer;
    int maxfd;

    FD_ZERO(&readfds);
    FD_SET(unixfd, &readfds);

    maxfd = unixfd;

    if(-1 == select(maxfd + 1, &readfds, 0, 0, 0))
    {
      if(errno == EINTR)
        continue;

      break;
    }

    peer = malloc(sizeof(struct DJPT_peer));

    memset(peer, 0, sizeof(struct DJPT_peer));

    if(FD_ISSET(unixfd, &readfds))
    {
      peer->fd = accept(unixfd, &peer->addr, &peer->addrlen);

      if(peer->fd == -1)
      {
        fprintf(stderr, "accept failed: %s\n", strerror(errno));

        return EXIT_FAILURE;
      }

      if(-1 == setsockopt(peer->fd, SOL_SOCKET, SO_PASSCRED, &one, sizeof(one)))
      {
        fprintf(stderr, "Failed to set SO_PASSCRED: %s\n", strerror(errno));

        close(peer->fd);
        peer->fd = -1;
      }
    }
    else
      peer->fd = -1;

    if(peer->fd == -1)
    {
      free(peer);

      continue;
    }

    pthread_create(&peer->thread, 0, peer_thread, peer);
    pthread_detach(peer->thread);
  }

  return EXIT_SUCCESS;
}
