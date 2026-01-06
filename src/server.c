#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/signal.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <regex.h>
#include <uthash.h>

#include "macros.h"
#include "server.h"
#include "db.h"
#include "cfg.h"

#define BACKLOG 1024

#define CTRL_FLAG_SHUTDOWN 1u
#define CTRL_FLAG_RELOAD   2u

typedef enum conn_type_t
{
  CONN_SOCKET=0,
  CONN_LISTEN,
  CONN_SIGNAL,
} conn_type_t;

typedef enum conn_state_t
{
  STATE_RECV,
  STATE_SEND,
  STATE_CLOSE
} conn_state_t;

typedef struct conn_t
{
  conn_type_t type;
  int fd;
  conn_state_t state;

  void* buf;
  size_t szin;

  const void* hdr;
  size_t hdr_sz;
  size_t hdr_sent;

  const void* body;
  size_t body_sz;
  size_t body_sent;
  UT_hash_handle hh;
} conn_t;

static conn_t lconn,sconn;

static pthread_t* tpool;
static size_t threads_count=0;
static _Atomic uint32_t ctrl_flags=0;
//static _Atomic uint64_t actives;
static int sfd=-1;
static int lfd=-1;

static db_t* db=0;

static const char* rpat1="^\\(GET\\|HEAD\\)[[:blank:]]\\+\\([^[:blank:]]\\+\\)";
static regex_t rre1;

static const uint64_t one=1;

static inline void writeone(int fd) { write(fd,&one,sizeof(one)); }

static const void* content(const char* request,size_t sz,size_t* rsz)
{
  regmatch_t match[3]={0,};

  if(regexec(&rre1,request,3,match,0)) return 0;
  size_t dlen=0;
  const void* d=db_get2(db,request+match[2].rm_so,match[2].rm_eo-match[2].rm_so,&dlen);

  if(!d) return 0;
  *rsz=dlen;
  if(request[match[1].rm_so]=='G' || request[match[1].rm_so]=='g')  return d;

  void* b=memmem(d,dlen,"\r\n\r\n",4);
  if(!b) return 0;
  *rsz=b-d+4;
  return d;
}

static void* worker(void* arg);
static int listen_tcp4(const char *ip,uint16_t port,int backlog);
static int listen_unix(const char *path, int backlog);

int server_start(const cfg_server_t* c)
{
  if(!c) $abort("no config provided");
  regcomp(&rre1,rpat1,REG_NEWLINE|REG_ICASE);
  db=db_open(c->db);

  threads_count=c->threads;
  tpool=md_tcalloc(pthread_t,threads_count);

  {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    if(sigaction(SIGPIPE, &sa, NULL)<0) $abort("sigpipe handler");
  }

  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  sigaddset(&mask, SIGHUP);
  sigaddset(&mask, SIGINT);

  lfd=c->port ? listen_tcp4(c->socket,c->port,c->backlog) : listen_unix(c->socket,c->backlog);
  sfd=signalfd(-1,&mask,SFD_NONBLOCK|SFD_CLOEXEC);

  lconn.fd=lfd;
  lconn.type=CONN_LISTEN;
  sconn.fd=sfd;
  sconn.type=CONN_SIGNAL;

  for(size_t i=0;i<threads_count;i++) pthread_create(tpool+i,0,worker,(void*)c);
$msg("Server started");
  for(size_t i=0;i<threads_count;i++) pthread_join(tpool[i],0);
  md_free(tpool);
  regfree(&rre1);
  close(sfd); sconn.fd=sfd=-1;
  close(lfd); lconn.fd=lfd=-1;

  db_close(db);
  db=0;
$msg("Server stopped");
  return 0;
}


static int listen_tcp4(const char *ip,uint16_t port,int backlog)
{
  if(backlog<=0) backlog=BACKLOG;

  int fd=socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,IPPROTO_TCP);
  if(fd<0) $abort("create listen socket");
  int on = 1;

  if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))==-1) $abort("listen socket options");

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family=AF_INET;
  addr.sin_port=htons(port);

  if(!ip || !strcmp(ip,"*")) addr.sin_addr.s_addr=htonl(INADDR_ANY);
  else
  {
    if(inet_pton(AF_INET, ip, &addr.sin_addr)!= 1) $abort("listen tcp4: bad address");
  }

  if(bind(fd, (struct sockaddr *)&addr, sizeof(addr))== -1) $abort("tcp4 bind");
  if(listen(fd, backlog)== -1) $abort("tcp4 listen");

$msg("listen at http://%s:%hu",ip,port);
  return fd;
}


static int listen_unix(const char *path, int backlog)
{
    if(!path || !*path) $abort("empty path");
    if(backlog <= 0) backlog=BACKLOG;

    unlink(path);

    int fd=socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if(fd == -1) $abort("socket creation");

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family=AF_UNIX;

    size_t path_len=strlen(path);
    if(path_len+1>=sizeof(addr.sun_path)) $abort("path too long");
    memcpy(addr.sun_path,path,path_len+1);
    socklen_t len=(socklen_t)(offsetof(struct sockaddr_un, sun_path)+path_len+1);
    if(bind(fd, (struct sockaddr *)&addr, len)== -1) $abort("bind");
    chmod(path, 0666);
    if(listen(fd, backlog)== -1) $abort("listen");

$msg("listen at unix socket %s",path);
    return fd;
}


static conn_t* incoming(const cfg_server_t* cfg,int efd)
{
  int f=-1;
  struct sockaddr_storage sa;
  socklen_t slen = sizeof(sa);
  f=accept4(lfd,(struct sockaddr *)&sa,&slen,SOCK_NONBLOCK | SOCK_CLOEXEC);

  if(f<0)
  {
    if(!(errno == EAGAIN || errno == EWOULDBLOCK)) perror("accept");
    return 0;
  }

  conn_t* rv=md_new(rv);
  rv->type=CONN_SOCKET;
  rv->state=STATE_RECV;
  rv->fd=f;
  rv->buf=md_calloc(cfg->inbuf);

  struct epoll_event ev={0,};
  ev.data.ptr=rv;
  ev.events=EPOLLIN|EPOLLRDHUP|EPOLLERR;
  epoll_ctl(efd,EPOLL_CTL_ADD,rv->fd,&ev);

  return rv;
}


static int handle_signalfd(int sfd)
{
  for(;;)
  {
    struct signalfd_siginfo si;
    ssize_t n = read(sfd, &si, sizeof(si));

    if(n<0)
    {
      if(errno == EAGAIN || errno == EWOULDBLOCK) return 0;
      perror("read(signalfd)");
      return 0;
    }
    if(!n) return 0;
    if(n!=sizeof(si))
    {
      $msg("short read from signalfd: %zd",n);
      return 0;
    }

    switch (si.ssi_signo)
    {
      case SIGTERM:
      case SIGINT:
      case SIGQUIT:
//            atomic_store_explicit(&terminate, true, memory_order_relaxed);
        return 1;

      case SIGHUP:
//            atomic_store_explicit(&reload, true, memory_order_relaxed);
        return 0;

      default:
        return 0;
    }
  }
  return 0;
}

static int handle_in(const cfg_server_t* cfg,conn_t* c,int efd)
{
  for(;;)
  {
    if(c->szin>=cfg->inbuf) return 1;
    ssize_t n=read(c->fd,c->buf+c->szin,cfg->inbuf-c->szin);
    if(!n) return 1;
    if(n<0)  return !(errno == EAGAIN || errno == EWOULDBLOCK);
    c->szin+=(size_t)n;

    void* line_end = memmem(c->buf,c->szin,"\r\n",2);
    if(!line_end) continue;

    size_t bs=0;
    const void* b=content(c->buf,line_end-c->buf,&bs);
    c->hdr=b? cfg->headers : cfg->h404;
    c->hdr_sz=strlen(c->hdr);
    c->hdr_sent=0;
    c->body=b;
    c->body_sz=b?bs:0;
    c->body_sent=0;
    c->state=STATE_SEND;

//? try to write immediately

    struct epoll_event ev={0,};
    ev.data.ptr=c;
    ev.events=EPOLLOUT|EPOLLRDHUP|EPOLLERR;
    epoll_ctl(efd,EPOLL_CTL_MOD,c->fd,&ev);
    break;
  }

  return 0;
}

static int handle_out(const cfg_server_t* cfg,conn_t* c)
{
  while(c->hdr_sent<c->hdr_sz)
  {
    ssize_t n=write(c->fd,c->hdr+c->hdr_sent,c->hdr_sz-c->hdr_sent);
    if(n<0)
      return !(errno == EAGAIN || errno == EWOULDBLOCK);
    c->hdr_sent+=(size_t)n;
  }

  while(c->body_sent<c->body_sz)
  {
    ssize_t n=write(c->fd,c->body+c->body_sent,c->body_sz-c->body_sent);
    if(n<0)
      return !(errno == EAGAIN || errno == EWOULDBLOCK);
    c->body_sent+=(size_t)n;
  }

//$msg("close");
  c->state=STATE_CLOSE;
  return 0;
}


static void* worker(void* arg)
{
  const cfg_server_t* cfg=arg;
  conn_t* root=0;

  int epfd = epoll_create1(EPOLL_CLOEXEC);
  if(epfd<0) $abort("epoll creating error");


  struct epoll_event ev;
  ev.events=EPOLLIN|EPOLLERR|EPOLLEXCLUSIVE;
  ev.data.ptr=&lconn;
  epoll_ctl(epfd,EPOLL_CTL_ADD,lfd,&ev);

  ev.events=EPOLLIN|EPOLLERR;
  ev.data.ptr=&sconn;
  epoll_ctl(epfd,EPOLL_CTL_ADD,sfd,&ev);

  struct epoll_event *events=md_tcalloc(struct epoll_event,cfg->backlog);

  for(;;)
  {
    int n=epoll_wait(epfd,events,cfg->backlog,-1);

    if(n<0 && errno!=EINTR)
    {
      perror("epoll_wait");
      break;
    }
    for(int i = 0;i<n;i++)
    {
      uint32_t evmask = events[i].events;
      conn_t *c=events[i].data.ptr;
      switch(c->type)
      {
        case CONN_LISTEN:
          {
            conn_t* z=incoming(cfg,epfd);

            if(!z) continue;
#if LOCALDEBUG
            conn_t* t=0;
            HASH_FIND(hh,root,&z->fd,sizeof(z->fd),t);
            if(t) $abort("integrity failed");
#endif
            HASH_ADD_KEYPTR(hh,root,&z->fd,sizeof(z->fd),z);
          }
          continue;
        case CONN_SIGNAL:
          $msg("got signal, exiting");
          if(!handle_signalfd(sfd)) continue;
          atomic_fetch_or(&ctrl_flags,CTRL_FLAG_SHUTDOWN);
          break;
        default:
        {
//          if(evmask&(EPOLLERR|EPOLLHUP|EPOLLRDHUP)) c->state=STATE_CLOSE;
          if(evmask&EPOLLERR) c->state=STATE_CLOSE;
          else
          {
            if((evmask&EPOLLIN) && c->state==STATE_RECV && handle_in(cfg,c,epfd)) c->state=STATE_CLOSE;
            if(c->state==STATE_SEND && handle_out(cfg,c)) c->state=STATE_CLOSE;
          }
          if(c->state==STATE_CLOSE)
          {
            epoll_ctl(epfd,EPOLL_CTL_DEL,c->fd,0);
            close(c->fd);
            HASH_DELETE(hh,root,c);
            md_free(c->buf);
            md_free(c);
          }
        }
      }
    }
  }
  close(epfd);
  md_free(events);

  while(root)
  {
    conn_t* r=root;
    HASH_DELETE(hh,root,r);
    close(r->fd);
    md_free(r->buf);
    md_free(r);
  }

  return 0;
}

