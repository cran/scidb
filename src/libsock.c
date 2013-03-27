/* Minimalist socket support functions */
#include <stdio.h>
#ifdef WIN32
#include <winsock2.h>
#include <windows.h>
#else
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <poll.h>
#define INVALID_SOCKET -1
#endif

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Utils.h>
#include <R_ext/Rdynload.h>
#ifndef WIN32
#include <R_ext/Callbacks.h>
#include <R_ext/eventloop.h>
#include <R_ext/Parse.h>
#endif
#include "libsock.h"
#include <errno.h>

#ifdef WIN32
WSADATA wsaData;
#endif

void
R_init_scidb(DllInfo * info)
{
#ifdef WIN32
  int j = WSAStartup(MAKEWORD(2,2),&wsaData);
  if(j!=0) error("Windows socket initialization error");
#endif
}

void
R_unload_scidb(DllInfo * info)
{
#ifdef WIN32
  WSACleanup();
#endif
}



/* tcpconnect
 * connect to the specified host and port, returning a socket
 */
int
tcpconnect (char *host, int port)
{
  struct hostent *h;
  struct sockaddr_in sa;
#ifdef WIN32
  SOCKET s;
#else
  int s;
#endif

  h = gethostbyname (host);
  if (!h)
    {
      s = -1;
    }
  else
    {
      s = socket (AF_INET, SOCK_STREAM, 0);
      memset(&sa, 0, sizeof(sa));
      sa.sin_family = AF_INET;
      sa.sin_port = htons (port);
      sa.sin_addr = *(struct in_addr *) h->h_addr;
      if (connect (s, (struct sockaddr *) &sa, sizeof (sa)) < 0)
	{
#ifdef WIN32
          closesocket(s);
#else
          close (s);
#endif
	  return -1;
	}
    }
/*
#ifdef WIN32
    u_long iMode=1;
    ioctlsocket(s,FIONBIO,&iMode);
#else
    if(fcntl(s, F_SETFL, O_NONBLOCK) < 0)
     {
       close(s);
       return -1;
     }
    signal(SIGPIPE, SIG_IGN);
#endif
*/
  return (int)s;
}

static void
recv_finalize (SEXP M)
{
  free((void *)R_ExternalPtrAddr(M));
}



SEXP SOCK_CLOSE (SEXP S)
{ 
#ifdef WIN32
  return ScalarInteger(closesocket((SOCKET)INTEGER(S)[0]));
#else
  return ScalarInteger(close(INTEGER(S)[0]));
#endif
}


SEXP SOCK_NAME(SEXP S)
{
  struct sockaddr_in sin;
  socklen_t slen;
  int s = INTEGER(S)[0];
  slen = sizeof(sin);
  memset(&sin, 0, sizeof (sin));
#ifdef WIN32
  getsockname ((SOCKET)s, (struct sockaddr *) &sin, &slen);
#else
  getsockname (s, (struct sockaddr *) &sin, &slen);
#endif
  return ScalarInteger(ntohs(sin.sin_port));
}

SEXP SOCK_CONNECT(SEXP HOST, SEXP PORT)
{
  char *host =  (char *)CHAR(STRING_ELT(HOST, 0));
  int port = INTEGER(PORT)[0];
  return ScalarInteger(tcpconnect(host, port));
}

SEXP SOCK_SEND(SEXP S, SEXP DATA)
{ 
  size_t len = (size_t)length(DATA);
  int s = INTEGER(S)[0];
  int ts = 0,  // total sent
      sent = 0;
  while(ts < len) {
#ifdef WIN32
    sent = send((SOCKET)s, (const void *)&(RAW(DATA)[ts]), len-ts, 0);
#else
    sent = send(s, (const void *)&(RAW(DATA)[ts]), len-ts, 0);
#endif
    ts+=sent;
  }
  return ScalarInteger(ts);
}

/* A generic recv wrapper function */
SEXP SOCK_RECV(SEXP S, SEXP EXT, SEXP BS, SEXP MAXBUFSIZE)
{
  SEXP ans = R_NilValue;
  void *buf;
  char *msg, *p;
//  struct pollfd pfds;
  int  j, s = INTEGER(S)[0];
  size_t k = 0;
  double maxbufsize = REAL(MAXBUFSIZE)[0];
  int bufsize = MBUF;
  int bs = INTEGER(BS)[0];
  if(maxbufsize < bs) maxbufsize = bs;
  buf = (void *)malloc(bs);
  msg = (char *)malloc(bs);
  p = msg;
//  pfds.fd = s;
//  pfds.events = POLLIN;
//  h = poll(&pfds, 1, 50);
//  while(h>0) {
  j = 1;
  while(j>=0) {
#ifdef WIN32
    j = recv((SOCKET)s, buf, bs, 0);
#else
    j = recv(s, buf, bs, 0);
#endif
    if(j<1) break;
/* If we exceed the maxbufsize, break. This leaves data
 * in the TCP RX buffer. XXX We need to tell R that this
 * is an incomplete read so this can be handled at a high
 * level, for example by closing the connection or whatever.
 * The code is here for basic protection from DoS and memory
 * overcommit attacks. 
 */
    if(k+j > maxbufsize) break;
    if(k + j > bufsize) {
      bufsize = bufsize + MBUF;
      msg = (char *)realloc(msg, bufsize);  
    }
    p = msg + k;
    memcpy((void *)p, buf, j);
    k = k + j;
//    h=poll(&pfds, 1, 50);
  }
  if(INTEGER(EXT)[0]) {
/* return a pointer to the recv buffer */
    ans = R_MakeExternalPtr ((void *)msg, R_NilValue, R_NilValue);
    R_RegisterCFinalizer (ans, recv_finalize);
    free(buf);
  }
  else {
/* Copy to a raw vector */
    PROTECT(ans=allocVector(RAWSXP,k));
    p = (char *)RAW(ans);
    memcpy((void *)p, (void *)msg, k);
    free(buf);
    free(msg);
    UNPROTECT(1);
  }
  return ans;
}


#ifdef WIN32
int
mingw_poll (struct pollfd *fds, unsigned int nfds, int timo)
{
  struct timeval timeout, *toptr;
  fd_set ifds, ofds, efds, *ip, *op;
  int i, rc;

  /* Set up the file-descriptor sets in ifds, ofds and efds. */
  FD_ZERO (&ifds);
  FD_ZERO (&ofds);
  FD_ZERO (&efds);
  for (i = 0, op = ip = 0; i < nfds; ++i)
    {
      fds[i].revents = 0;
      if (fds[i].events & (POLLIN | POLLPRI))
        {
          ip = &ifds;
          FD_SET (fds[i].fd, ip);
        }
      if (fds[i].events & POLLOUT)
        {
          op = &ofds;
          FD_SET (fds[i].fd, op);
        }
      FD_SET (fds[i].fd, &efds);
    }

  /* Set up the timeval structure for the timeout parameter */
  if (timo < 0)
    {
      toptr = 0;
    }
  else
    {
      toptr = &timeout;
      timeout.tv_sec = timo / 1000;
      timeout.tv_usec = (timo - timeout.tv_sec * 1000) * 1000;
    }

#ifdef DEBUG_POLL
  printf ("Entering select() sec=%ld usec=%ld ip=%lx op=%lx\n",
          (long) timeout.tv_sec, (long) timeout.tv_usec, (long) ip,
          (long) op);
#endif
  rc = select (0, ip, op, &efds, toptr);
#ifdef DEBUG_POLL
  printf ("Exiting select rc=%d\n", rc);
#endif

  if (rc <= 0)
    return rc;

  if (rc > 0)
    {
      for (i = 0; i < nfds; ++i)
        {
          int fd = fds[i].fd;
          if (fds[i].events & (POLLIN | POLLPRI) && FD_ISSET (fd, &ifds))
            fds[i].revents |= POLLIN;
          if (fds[i].events & POLLOUT && FD_ISSET (fd, &ofds))
            fds[i].revents |= POLLOUT;
          if (FD_ISSET (fd, &efds))
            /* Some error was detected ... should be some way to know. */
            fds[i].revents |= POLLHUP;
#ifdef DEBUG_POLL
          printf ("%d %d %d revent = %x\n",
                  FD_ISSET (fd, &ifds), FD_ISSET (fd, &ofds), FD_ISSET (fd,
                                                                        &efds),
                  fds[i].revents);
#endif
        }
    }
  return rc;
}
#endif
