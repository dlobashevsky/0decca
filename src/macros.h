
//! \file
//! common macros

__attribute__ ((noreturn))
static inline void error_(const char* msg,const char* func,const char* file,int line) 
{
  fprintf(stderr,"ERROR %s <%s:%d>\t",func,file,line);
  perror(msg);abort();
}

#define $abort(x_)	error_(x_,__func__,__FILE__,__LINE__)
#define $$	fprintf(stderr,"#** %s <%s:%d>\n",__func__,__FILE__,__LINE__);
#define $msg(s,...)	fprintf(stderr,"# %s <%s:%u>:\t" s "\n", __func__,__FILE__,__LINE__, ##__VA_ARGS__)

#define md_malloc(x)	({ void* tmp__=malloc(x); if(!tmp__) $abort("mem"); tmp__; })
#define md_free		free
#define md_calloc(x)	({ void* tmp__=calloc(x,1); if(!tmp__) $abort("mem"); tmp__; })
#define md_strdup(x)	({ void* tmp__=x ? strdup(x) : 0; if(!tmp__) $abort("mem"); tmp__; })
#define md_realloc(x,y)	({ void* tmp__=realloc(x,y); if(!tmp__) $abort("mem"); tmp__; })
#define md_sprintf(s__,...)	({ char* tmp__=0; asprintf(&tmp__,s__, ##__VA_ARGS__); tmp__; })

#define md_tmalloc(x,y)		((x*)md_malloc(sizeof(x)*(y)))
#define md_tcalloc(x,y)		((x*)md_calloc(sizeof(x)*(y)))
#define md_pmalloc(x)		((void*)(md_malloc((x)*sizeof(void*))))
#define md_pcalloc(x)		((void*)(md_calloc((x)*sizeof(void*))))
#define md_new(x)		((typeof(x))md_calloc(sizeof(x[0])))
#define md_anew(x,y)		((typeof(x))md_calloc(sizeof(x[0])*(y)))

#define obstack_chunk_alloc 	malloc
#define obstack_chunk_free 	free

#define rc_malloc(x)	({ void* tmp__=malloc((x)+sizeof(uint64_t)); if(!tmp__) $abort("rcmem"); *((uint64_t*)tmp__)=1; tmp__+sizeof(uint64_t); })
#define rc_free(x)	do { if(x) { uint64_t* tmp__=(uint64_t*)(x)-1; if(tmp__[0]--<=1) free(tmp__);} } while(0)
#define rc_calloc(x)	({ void* tmp__=calloc((x)+sizeof(uint64_t),1); if(!tmp__) $abort("rcmem"); *((uint64_t*)tmp__)=1; tmp__+sizeof(uint64_t); })
#define rc_dup(x)	({ uint64_t* tmp__=(void*)(x)-sizeof(uint64_t); tmp__[0]++; (void*)(x); })
/*
//! how to realloc data with refernce counter :-)
#define rc_realloc(x,y)	({ void* rv__=0; if(x) \
{  if(y) { uint64_t* tmp__=((uint64_t*)(x))-1; if(tmp__[0]!=1) $abort; rv__=md_realloc(tmp__,(y)+sizeof(uint64_t)); rv__-=sizeof(uint64_t); } else rc_free(x); } \
else { if(y) rv__=rc_malloc(y); } rv__; })
*/

static inline void* rc_realloc(void* x,size_t y)
{
  if(x)
  {
    if(y) 
    { 
      uint64_t* tmp=((uint64_t*)x)-1;
      if(tmp[0]!=1) $abort("rcmem");
      return md_realloc(tmp,y+sizeof(uint64_t))+sizeof(uint64_t);
    } 
    else 
    {
      rc_free(x); 
      return 0;
    }
  }
  else 
    return y ? rc_malloc(y) : 0;
}

#define rc_tmalloc(x,y)		((x*)rc_malloc(sizeof(x)*(y)))
#define rc_tcalloc(x,y)		((x*)rc_calloc(sizeof(x)*(y)))
#define rc_pmalloc(x)		((void*)(rc_malloc((x)*sizeof(void*))))
#define rc_pcalloc(x)		((void*)(rc_calloc((x)*sizeof(void*))))
#define rc_new(x)		((typeof(x))rc_calloc(sizeof(x[0])))
#define rc_anew(x,y)		((typeof(x))rc_calloc(sizeof(x[0])*(y)))


#define KILO                   1024ULL
#define MEGA                   1048576ULL
#define GIGA                   1073741824ULL

#define GROW_GETSIZE(step,s)   ((s) ? (1+((s)-1)/(step))*(step) :1)


#define QUOTE_(x)		#x
#define QSTRINGIFY(x)		QUOTE_(x)


// sync primitives
//! abstract destructor
typedef void dtr_t(void*);



#ifdef NOATOMIC
#define LOCK_INIT(x)	do {  pthread_mutexattr_t pa;  pthread_mutexattr_init(&pa);\
  pthread_mutexattr_settype(&pa,PTHREAD_MUTEX_ADAPTIVE_NP);  pthread_mutex_init(&(x),&pa); \
  pthread_mutexattr_destroy(&pa); } while(0)

#define LOCK_FREE(x)	pthread_mutex_destroy(&(x))
#define LOCK(x)		pthread_mutex_trylock(&(x))
#define UNLOCK(x)	pthread_mutex_unlock(&(x))

#else

/*
static inline void atomic_lock(volatile atomic_flag *x)
{
  for(;;)
  {
    for(size_t i=0;i<100;i++)
      if(!atomic_flag_test_and_set(x)) return;
    pthread_yield();
  }
}
*/

#define LOCK_INIT(x)	atomic_flag_clear(&(x))
#define LOCK_FREE(x)
#define LOCK(x)		while(atomic_flag_test_and_set(&(x))) sched_yield()
//#define LOCK(x)		atomic_lock(&(x))
#define UNLOCK(x)	atomic_flag_clear(&(x))
#endif


#define LOCK_SYNC(_x)	while(_x) sched_yield()
