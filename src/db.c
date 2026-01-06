#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <xxhash.h>
#include <cmph.h>
#include <uuid/uuid.h>
#include <sqlite3.h>
#include <uthash.h>

#include "macros.h"
#include "config.h"
#include "db.h"
#include "cfg.h"

#define DB_MAGIC_INDEX	0xf0caec0dU
#define DB_MAGIC_DATA	0xfecaec0dU
#define DB_MAGIC_NAMES	0xfccaec0dU
#define DB_MAGIC_HASH	0xfdcaec0dU

#define DB_SEED		0xdeadc0deU

/*
Content-Type: application/vnd.mapbox-vector-tile
Content-Encoding: gzip

y = (2^z - 1) - tile_row


Cache-Control: public, max-age=86400
Last-Modified: Wed, 03 Dec 2025 00:00:00 GMT
Access-Control-Allow-Origin: *

-------
1. Content-Length

Длина gzipped тела. У разных тайлов разная, это прям часть твоего index/tiles.dat.

2. ETag

Как обсуждали:

либо глобальный ETag по версии tileset’а (просто строка, одинаковая для всех),

либо (лучше) хэш тайла.

Для B-варианта:

при дедупе храним xxhash64 сырых или gzipped байтов;

ETag = "mvt-" + hex(hash).

3. Может быть, какие-то кастомные заголовки


*/

typedef struct db_header_t
{
  uint32_t magic;
  uuid_t uuid;
  uint16_t parts;
  uint16_t part;
  uint32_t records;
  uint64_t created;
  uint64_t size;
  uint64_t hash;
} __attribute__((packed)) db_header_t;

typedef struct db_idx_record_t
{
  DB_IDX_RECORD_OFFSET off;
  DB_IDX_RECORD_OFFSET noff;
  DB_IDX_RECORD_LEN len;
  DB_IDX_RECORD_NAME nlen;
} __attribute__((packed)) db_idx_record_t;


typedef struct db_file_t
{
  int fd;
  void* data;
  size_t sz;
} db_file_t;

typedef struct db_part_t
{
  db_file_t* index;
  db_file_t* data;
  db_file_t* name;
  size_t record_count;
  const db_idx_record_t* records;
  const void* strings;
  const void* names;
  cmph_t* hash;
} db_part_t;

struct db_t
{
  size_t cnt;
  db_part_t* parts;
};

typedef struct db_tmphash_t
{
  uint64_t h;

  DB_IDX_RECORD_OFFSET off;
  DB_IDX_RECORD_LEN len;

  UT_hash_handle hh;
} db_tmphash_t;

static uintmax_t utils_time_abs(void)
{
  struct timeval t;
  gettimeofday(&t,0);
  return t.tv_sec*1000ULL+(t.tv_usec+500ULL)/1000ULL;
}

char* utils_time_format(uintmax_t t)
{
  uintmax_t time=utils_time_abs()-t;
  char *bf;
  if(time<60000)
  {
     asprintf(&bf,"%ju ms",time);
     return bf;
  }

  uintmax_t secs=time/1000U;
  uintmax_t mins=secs/60;
  uintmax_t hours=mins/60;
  uintmax_t days=hours/24;
  uintmax_t weeks=days/7;

  secs%=60;
  mins%=60;
  hours%=24;
  days%=7;

  if(weeks)
  {
     asprintf(&bf,"%ju weeks %ju days %02ju:%02ju:%02ju.%03ju",weeks,days,hours,mins,secs,time%1000);
     return bf;
  }
  if(days)
  {
     asprintf(&bf,"%ju days %02ju:%02ju:%02ju.%03ju",days,hours,mins,secs,time%1000);
     return bf;
  }

  asprintf(&bf,"%02ju:%02ju:%02ju.%03ju",hours,mins,secs,time%1000);
  return bf;
}


static uint64_t xx(const void* d,uint64_t s)
{
  XXH3_state_t* const state=XXH3_createState();
  uint64_t seed = DB_SEED;
  if(XXH3_64bits_reset_withSeed(state, seed)==XXH_ERROR)  $abort("XXHASH error");
  if(XXH3_64bits_update(state,d,s)==XXH_ERROR) $abort("XXHASH update error");
  XXH64_hash_t hash = XXH3_64bits_digest(state);
  XXH3_freeState(state);
  return hash;
}

static db_file_t* db_file_open(const char* fn)
{
  struct stat st;

  if(stat(fn,&st) || ! (S_ISREG(st.st_mode) || S_ISLNK(st.st_mode)) || st.st_size<=sizeof(db_header_t)) $abort("stat file error");
  int fd=open(fn, O_RDONLY);
  if(fd== -1) $abort("open file error");
  void* data=mmap(0,st.st_size,PROT_READ,MAP_PRIVATE|DB_MMAP_FLAGS,fd,0);
  if(data==MAP_FAILED)
  {
    close(fd);
    $abort("mmap file error");
  }

  {
    const db_header_t* h=data;
    const void* d=data+sizeof(db_header_t);
    uint64_t hash=xx(d,st.st_size-sizeof(db_header_t));
    if((h->magic&0xffffff)!=0xcaec0dU) $abort("no valid signature");
    if(h->hash!=hash) $abort("integrity check failed, hash mismatch");
  }

  madvise(data,st.st_size,MADV_RANDOM|MADV_WILLNEED);

  db_file_t* rv=md_new(rv);
  rv->fd=fd;
  rv->data=data;
  rv->sz=st.st_size;

  return rv;
}

static db_file_t* db_file_create(const char* fn,size_t sz)
{
  struct stat st;
  if(!stat(fn,&st)) $abort("file exists");

  int fd=open(fn,O_RDWR | O_CREAT | O_TRUNC,(mode_t)0660);
  if(fd == -1) $abort("create file error");

  if(posix_fallocate(fd,0,sz))
  {
    close(fd);
    $abort("not enough disk space");
  }

  void* data=mmap(0,sz,PROT_READ|PROT_WRITE,MAP_SHARED|DB_MMAP_FLAGS,fd,0);
  if(data==MAP_FAILED)
  {
    close(fd);
    $abort("mmap file error");
  }

  madvise(data,sz,MADV_RANDOM|MADV_WILLNEED);

  db_file_t* rv=md_new(rv);
  rv->fd=fd;
  rv->data=data;
  rv->sz=sz;

  return rv;
}

static void db_file_free(db_file_t* dbf)
{
  munmap(dbf->data,dbf->sz);
  close(dbf->fd);
  md_free(dbf);
}

static int lineparse(char* bf,uint64_t* names,uint64_t* headers,uint64_t* bodies,char* pathbuf)
{
  char* state=0;
  {
    char* t=strchr(bf,'\n');
    if(t) *t=0;
    t=strchr(bf,'\r');
    if(t) *t=0;
  }
  char* s=strtok_r(bf,"\t",&state);
  if(!s) return  -1;
  *names+=strlen(s)+1;
  char* fn=strtok_r(0,"\t",&state);

  struct stat st;
  char *p=realpath(fn,pathbuf);
  if(!p) $abort(fn);
  if(stat(p,&st) || ! (S_ISREG(st.st_mode) || S_ISLNK(st.st_mode))) $abort(fn);
//  free(p);

  *bodies+=st.st_size;
  *headers+=2;

  char* h=0;
  while(h=strtok_r(0,"\t",&state))
    *headers+=2+strlen(h);

  return 0;
}

static int lineparse2(char* bf,void* dst,void* ndst,size_t* name,size_t* body,uint64_t* dedup,char* pathbuf)
{
  char* state=0;
  {
    char* t=strchr(bf,'\n');
    if(t) *t=0;
    t=strchr(bf,'\r');
    if(t) *t=0;
  }
  char* s=strtok_r(bf,"\t",&state);
  if(!s) return  -1;
  *name=strlen(s);
//  void* next=mempcpy(dst,s,*name+1);
  memcpy(ndst,s,*name+1);

  void* next=dst;
  char* fn=strtok_r(0,"\t",&state);

  struct stat st;
  char *p=realpath(fn,pathbuf);
  if(!p) $abort(fn);
  if(stat(p,&st) || ! (S_ISREG(st.st_mode) || S_ISLNK(st.st_mode))) $abort(fn);

//  *bodies+=st.st_size;
  *body=0;

  char* h=0;
  while(h=strtok_r(0,"\t",&state))
  {
    size_t l=strlen(h);
    next=mempcpy(next,h,l);
    next=mempcpy(next,"\r\n",2);
    *body+=l+2;
  }
  next=mempcpy(next,"\r\n",2);
  *body+=2;

  FILE *f=fopen(p,"rb");
  if(!f || fread(next,st.st_size,1,f)!=1) $abort(p);
  fclose(f);

  *body+=st.st_size;
  if(dedup) *dedup=xx(dst,*body);

  return 0;
}

//! return error string
int db_build(const cfg_build_t* c)
{
  if(!c) $abort("no conig to build");

  uint64_t t0=utils_time_abs();

  char uuid[37];
  uuid_t u;
  uuid_generate_random(u);
  uuid_unparse_lower(u,uuid);
  $msg("create database %s, source=%s, uuid=%s",c->db,c->src,uuid);

  size_t items=0;
  uint64_t names=0;
  uint64_t headers=0;
  uint64_t bodies=0;

  mkdir(c->db,0770);
  FILE* f=fopen(c->src,"r");
  if(!f) $abort("no source");
  char pathbuf[PATH_MAX+1];
  size_t l=0;
  char* bf=0;

  while(getline(&bf,&l,f)>0)
  {
    if(lineparse(bf,&names,&headers,&bodies,pathbuf))
    {
      $msg("can not parse line %s",bf);
      $abort("input format error");
    }
    items++;
  }

  $msg("records %zd, names %ld, headers %ld, bodies %ld",items,names,headers,bodies);
  char** pidx=md_pcalloc(items);
  char* arena=md_calloc(names);
  rewind(f);

  uint64_t i=0;
  uint64_t n=0;

  while(getline(&bf,&l,f)>0)
  {
    char* p=strchr(bf,'\t');
    if(!p) $abort(bf);
    *p++=0;
    pidx[i++]=arena+n;
    strcpy(arena+n,bf);
    n+=strlen(bf)+1;
  }
$$
  cmph_io_adapter_t *source=cmph_io_vector_adapter(pidx,items);
  cmph_config_t *config = cmph_config_new(source);
  cmph_config_set_algo(config,PHASH_ALGO);
$$
  cmph_t* hash = cmph_new(config);
  cmph_config_destroy(config);
  cmph_io_vector_adapter_destroy(source);
  if(!hash) $abort("hash generation failed");

// create supplied files
  char* name_hash=md_sprintf("%s/hash.part0",c->db);
  char* name_idx=md_sprintf("%s/idx.part0",c->db);
  char* name_data=md_sprintf("%s/data.part0",c->db);
  char* name_names=md_sprintf("%s/names.part0",c->db);

  db_file_t* fidx=db_file_create(name_idx,items*sizeof(db_idx_record_t)+sizeof(db_header_t));
  db_file_t* fdata=db_file_create(name_data,headers+bodies+sizeof(db_header_t));
  db_file_t* fnames=db_file_create(name_names,names+sizeof(db_header_t));

  void* start_data=fdata->data+sizeof(db_header_t);
  void* start_names=fnames->data+sizeof(db_header_t);
  db_idx_record_t* start_idx=fidx->data+sizeof(db_header_t);

  db_header_t hidx={0,};
  db_header_t hdata={0,};
  db_header_t hhash={0,};
  db_header_t hname={0,};
  hidx.magic=DB_MAGIC_INDEX;
  hdata.magic=DB_MAGIC_DATA;
  hhash.magic=DB_MAGIC_HASH;
  hname.magic=DB_MAGIC_NAMES;
  hname.parts=hhash.parts=hidx.parts=hdata.parts=1;
  hname.part=hhash.part=hidx.part=hdata.part=0;
  hname.created=hhash.created=hidx.created=hdata.created=time(0);
  hname.records=hhash.records=hidx.records=hdata.records=items;
  hidx.size=items*sizeof(db_idx_record_t);
  hdata.size=headers+bodies;
  hname.size=names;

  memcpy(hdata.uuid,&u,sizeof(u));
  memcpy(hidx.uuid,&u,sizeof(u));
  memcpy(hhash.uuid,&u,sizeof(u));
  memcpy(hname.uuid,&u,sizeof(u));

  {
    size_t hl=0;
    char* hd=0;
    FILE* fhash=open_memstream(&hd,&hl);
    cmph_dump(hash,fhash);
    fclose(fhash);

    hhash.size=hl;
    hhash.hash=xx(hd,hl);

    fhash=fopen(name_hash,"wb");
    if(fwrite(&hhash,sizeof(hhash),1,fhash)!=1) $abort(name_hash);
    if(fwrite(hd,hl,1,fhash)!=1) $abort(name_hash);
    fclose(fhash);

    free(hd);
  }
  md_free(pidx);
  md_free(arena);

  rewind(f);

  size_t off=0;
  size_t noff=0;
  size_t final=0;

  {
    db_tmphash_t* root=0;
    uint64_t dhash;
    while(getline(&bf,&l,f)>0)
    {
      size_t nsz=0;
      size_t bsz=0;
      lineparse2(bf,start_data+off,start_names+noff,&nsz,&bsz,c->dedup ? &dhash : 0,pathbuf);
      ssize_t q=cmph_search(hash,bf,nsz);

      if(q<0) $abort(bf);  //hash integrity broken
      start_idx[q].noff=noff;
      start_idx[q].nlen=nsz;
      noff+=nsz+1;

      if(c->dedup)
      {
        db_tmphash_t* r=0;
        HASH_FIND(hh,root,&dhash,sizeof(dhash),r);
        if(!r)
        {
          r=md_new(r);
          r->h=dhash;
          r->off=off;
          r->len=bsz;
          HASH_ADD_KEYPTR(hh,root,&r->h,sizeof(r->h),r);
          start_idx[q].len=bsz;
          start_idx[q].off=off;
          off+=bsz;
        }
        else
        {
          start_idx[q].len=r->len;
          start_idx[q].off=r->off;
        }
      }
      else
      {
        start_idx[q].len=bsz;
        start_idx[q].off=off;
        off+=bsz;
      }
    }

    if(c->dedup)
    {
      final=off;
      while(root)
      {
        db_tmphash_t* l=root;
        HASH_DELETE(hh,root,l);
        md_free(l);
      }
    }
  }

  free(bf);
  fclose(f);

  if(!c->dedup) final=headers+bodies;
  hdata.size=final;

  hidx.hash=xx(fidx->data+sizeof(db_header_t),items*sizeof(db_idx_record_t));
  memcpy(fidx->data,&hidx,sizeof(hidx));
  hdata.hash=xx(fdata->data+sizeof(db_header_t),final);
  memcpy(fdata->data,&hdata,sizeof(hdata));
  hname.hash=xx(fnames->data+sizeof(db_header_t),names);
  memcpy(fnames->data,&hname,sizeof(hname));

  db_file_free(fidx);
  db_file_free(fdata);
  db_file_free(fnames);

  if(c->dedup) truncate(name_data,final+sizeof(db_header_t));

  md_free(name_hash);
  md_free(name_idx);
  md_free(name_data);
  md_free(name_names);

  cmph_destroy(hash);

  {
    char *z=utils_time_format(t0);
    $msg("build done, %s taken",z);
    free(z);
  }

  return 0;
}


static int sqcb(void *data, int argc, char **argv, char **azColName)
{
  uint64_t* res=data;
  *res=atoll(argv[0]);
  return 0;
}


/*
sqlite> select count(*) from tiles_shallow;
1152782
sqlite> select count(*) from tiles_data;
381501
select sum(length(tile_data)) from tiles_data;
3370982506

*/

//! return error string
int db_build_tiles(const cfg_build_t* c)
{
  const char* header="Content-Length: %ld\r\nETag: mvt-%016lx\r\n\r\n";
  const size_t hsz=strlen(header)-3-6+16;  // -%ld -%016lx + 16 hex hash
  const char* url="/%ld/%ld/%ld.mvt";

  if(!c) $abort("no conig to build");

  uint64_t t0=utils_time_abs();

  char uuid[37];
  uuid_t u;
  uuid_generate_random(u);
  uuid_unparse_lower(u,uuid);
  $msg("create database %s, source=%s, uuid=%s",c->db,c->src,uuid);

  size_t items=0;
  uint64_t names=0;
  uint64_t headers=0;
  uint64_t bodies=0;
//  uint64_t records=0;
  uint64_t tiles=0;

  sqlite3 *db;

  if(sqlite3_open_v2(c->src, &db,SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,0)!=SQLITE_OK) $abort("SQLite open error");

  sqlite3_exec(db, "PRAGMA query_only=ON;", 0, 0, 0);
  sqlite3_exec(db, "PRAGMA temp_store=MEMORY;", 0, 0, 0);
  sqlite3_exec(db, "PRAGMA cache_size=-200000;", 0, 0, 0);   // ~200k страниц
  sqlite3_exec(db, "PRAGMA mmap_size=1073741824;", 0, 0, 0); // 1 GiB

  mkdir(c->db,0770);

  sqlite3_stmt *stmt;

// headers are Content-Length, ETag = "mvt-" + hex(hash).
//$msg("header size is %zd",sizeof(db_header_t));
  {
    char path[1024];

    sqlite3_exec(db,"select sum(length(tile_data)) from tiles_data;", sqcb, &bodies, 0);
    sqlite3_exec(db,"select sum(length(length(tile_data))) from tiles_data;", sqcb, &headers, 0);
    sqlite3_exec(db,"select count(*) from tiles_data;", sqcb, &tiles, 0);
    sqlite3_exec(db,"select count(*) from tiles_shallow;", sqcb, &items, 0);

    headers+=tiles*hsz;

    sqlite3_prepare_v2(db,"select zoom_level,tile_column,tile_row from tiles_shallow;",-1,&stmt,0);
    while(sqlite3_step(stmt) != SQLITE_DONE)
    {
      uint64_t zoom=sqlite3_column_int(stmt,0);
      uint64_t col=sqlite3_column_int(stmt,1);
      uint64_t row=sqlite3_column_int(stmt,2);

      snprintf(path,sizeof(path)-1,url,zoom,col,(1ULL<<zoom)-1-row);
      names+=strlen(path)+1;
    }
    sqlite3_finalize(stmt);
  }

  $msg("records %zd, names %ld, headers %ld, bodies %ld, uniq tiles %ld",items,names,headers,bodies,tiles);
  char** pidx=md_pcalloc(items);
  char* arena=md_calloc(names);

  uint64_t i=0;
  uint64_t n=0;

  {
    char path[1024];
    sqlite3_prepare_v2(db,"select zoom_level,tile_column,tile_row from tiles_shallow;",-1,&stmt,0);
    while(sqlite3_step(stmt) != SQLITE_DONE)
    {
      uint64_t zoom=sqlite3_column_int(stmt,0);
      uint64_t col=sqlite3_column_int(stmt,1);
      uint64_t row=sqlite3_column_int(stmt,2);

      int64_t y=(1ULL<<zoom)-1-row;
      snprintf(path,sizeof(path)-1,url,zoom,col,y);
//$msg("stage1: <%s>",path);
      pidx[i++]=arena+n;
      strcpy(arena+n,path);
      n+=strlen(path)+1;
    }
    sqlite3_finalize(stmt);
  }

  cmph_io_adapter_t *source=cmph_io_vector_adapter(pidx,items);
  cmph_config_t *config = cmph_config_new(source);
  cmph_config_set_algo(config,PHASH_ALGO);

  cmph_t* hash = cmph_new(config);
  cmph_config_destroy(config);
  cmph_io_vector_adapter_destroy(source);
  if(!hash) $abort("hash generation failed");

// create result files
  char* name_hash=md_sprintf("%s/hash.part0",c->db);
  char* name_idx=md_sprintf("%s/idx.part0",c->db);
  char* name_data=md_sprintf("%s/data.part0",c->db);
  char* name_names=md_sprintf("%s/names.part0",c->db);

  db_file_t* fidx=db_file_create(name_idx,items*sizeof(db_idx_record_t)+sizeof(db_header_t));
  db_file_t* fdata=db_file_create(name_data,headers+bodies+sizeof(db_header_t));
  db_file_t* fnames=db_file_create(name_names,names+sizeof(db_header_t));

  void* start_data=fdata->data+sizeof(db_header_t);
  void* start_names=fnames->data+sizeof(db_header_t);
  db_idx_record_t* start_idx=fidx->data+sizeof(db_header_t);

  db_header_t hidx={0,};
  db_header_t hdata={0,};
  db_header_t hhash={0,};
  db_header_t hname={0,};
  hidx.magic=DB_MAGIC_INDEX;
  hdata.magic=DB_MAGIC_DATA;
  hhash.magic=DB_MAGIC_HASH;
  hname.magic=DB_MAGIC_NAMES;
  hname.parts=hhash.parts=hidx.parts=hdata.parts=1;
  hname.part=hhash.part=hidx.part=hdata.part=0;
  hname.created=hhash.created=hidx.created=hdata.created=time(0);
  hname.records=hhash.records=hidx.records=hdata.records=items;
  hidx.size=items*sizeof(db_idx_record_t);
  hdata.size=headers+bodies;
  hname.size=names;

  memcpy(hdata.uuid,&u,sizeof(u));
  memcpy(hidx.uuid,&u,sizeof(u));
  memcpy(hhash.uuid,&u,sizeof(u));
  memcpy(hname.uuid,&u,sizeof(u));

  {
    size_t hl=0;
    char* hd=0;
    FILE* fhash=open_memstream(&hd,&hl);
    cmph_dump(hash,fhash);
    fclose(fhash);

    hhash.size=hl;
    hhash.hash=xx(hd,hl);

    fhash=fopen(name_hash,"wb");
    if(fwrite(&hhash,sizeof(hhash),1,fhash)!=1) $abort(name_hash);
    if(fwrite(hd,hl,1,fhash)!=1) $abort(name_hash);
    fclose(fhash);

    free(hd);
  }

  md_free(pidx);
  md_free(arena);

  size_t off=0;
  size_t noff=0;
//  size_t final=0;

  {
    char path[1024];
    char hx[hsz+512];
    sqlite3_prepare_v2(db,
      "select tiles_shallow.tile_data_id as id,tiles_shallow.zoom_level as zoom_level,tiles_shallow.tile_column as tile_column,tiles_shallow.tile_row as tile_row, tiles_data.tile_data as tile_data "
//      ", length(tile_data),length(length(tile_data)) "
      "from tiles_shallow join tiles_data on tiles_shallow.tile_data_id = tiles_data.tile_data_id order by id;",
       -1, &stmt,0);

    size_t last=0;
    size_t next=0;
//    size_t cnt=0;
    while(sqlite3_step(stmt) != SQLITE_DONE)
    {


//      const unsigned char* sid=sqlite3_column_text(stmt, 0);
      uint64_t id=sqlite3_column_int(stmt, 0);
      uint64_t zoom=sqlite3_column_int(stmt, 1);
      uint64_t col=sqlite3_column_int(stmt, 2);
      uint64_t row=sqlite3_column_int(stmt, 3);
      const void* b=sqlite3_column_blob(stmt, 4);
      size_t bz=sqlite3_column_bytes(stmt, 4);

      snprintf(path,sizeof(path)-1,url,zoom,col,(1ULL<<zoom)-1-row);

      uint64_t nsz=strlen(path);
      ssize_t q=cmph_search(hash,path,nsz);
      if(q<0) $abort("name integrity");

      uint64_t h=xx(b,bz);
      snprintf(hx,sizeof(hx)-1,header,bz,h);

      start_idx[q].len=bz+strlen(hx);
      start_idx[q].off=off;

      start_idx[q].nlen=nsz+1;
      start_idx[q].noff=noff;

      memcpy(start_names+noff,path,start_idx[q].nlen);
      noff+=start_idx[q].nlen;

      if(last!=id)
      {
        start_idx[q].off=next;
        void* t=mempcpy(start_data+start_idx[q].off,hx,strlen(hx));
        memcpy(t,b,bz);
        off=next;
        next=start_idx[q].off+start_idx[q].len;
      }
      last=id;
    }
    sqlite3_finalize(stmt);
  }

//  hidx.size=items*sizeof(db_idx_record_t);
  hidx.hash=xx(fidx->data+sizeof(db_header_t),hidx.size);
  memcpy(fidx->data,&hidx,sizeof(hidx));
  hdata.hash=xx(fdata->data+sizeof(db_header_t),hdata.size);
  memcpy(fdata->data,&hdata,sizeof(hdata));
  hname.hash=xx(fnames->data+sizeof(db_header_t),names);
  memcpy(fnames->data,&hname,sizeof(hname));

  db_file_free(fidx);
  db_file_free(fdata);
  db_file_free(fnames);

  md_free(name_hash);
  md_free(name_idx);
  md_free(name_data);
  md_free(name_names);

  cmph_destroy(hash);
  sqlite3_close(db);

  {
    char *z=utils_time_format(t0);
    $msg("build done, %s taken",z);
    free(z);
  }

  return 0;
}


static int db_check(const db_file_t* df,const char* uuid,uint64_t records,uint32_t magic)
{
  if(!df) return -1;
  db_header_t* h=df->data;
  if(*(uint32_t*)df->data!=magic) return -1;
  if(h->records!=records || h->size+sizeof(db_header_t)!=df->sz) return -1;

  char u[37];
  uuid_unparse_lower(h->uuid,u);
  if(strcmp(uuid,u)) return -1;

  return 0;
}

db_t* db_open(const char* fn)
{
  char* name_hash=md_sprintf("%s/hash.part0",fn);
  char* name_idx=md_sprintf("%s/idx.part0",fn);
  char* name_data=md_sprintf("%s/data.part0",fn);
  char* name_names=md_sprintf("%s/names.part0",fn);

  db_file_t* didx=db_file_open(name_idx);
  db_file_t* ddata=db_file_open(name_data);
  db_file_t* dhash=db_file_open(name_hash);
  db_file_t* dname=db_file_open(name_names);

  if(!didx || !ddata || !dhash || !dname) $abort("open database error");

  db_header_t* dh=(db_header_t*)(didx->data);

  char u[37];
  uuid_unparse_lower(dh->uuid,u);
$msg("try to open database %s",u);

  if(db_check(didx,u,dh->records,DB_MAGIC_INDEX)) $abort("index file integrity check failed");
  if(db_check(ddata,u,dh->records,DB_MAGIC_DATA)) $abort("data file integrity check failed");
  if(db_check(dhash,u,dh->records,DB_MAGIC_HASH)) $abort("hash file integrity check failed");
  if(db_check(dname,u,dh->records,DB_MAGIC_NAMES)) $abort("names file integrity check failed");

  db_t* rv=md_new(rv);
  rv->cnt=1;
  rv->parts=md_new(rv->parts);
  db_part_t* p=rv->parts;

  p->index=didx;
  p->data=ddata;
  p->name=dname;
  p->record_count=dh->records;
  p->records=didx->data+sizeof(db_header_t);
  p->strings=ddata->data+sizeof(db_header_t);
  p->names=dname->data+sizeof(db_header_t);

  {
    FILE* ft=fopen(name_hash,"r");
    fseek(ft,sizeof(db_header_t),SEEK_SET);
    p->hash=cmph_load(ft);
    fclose(ft);
  }

  if(!p->hash) $abort("hash data integrity failed");
  db_file_free(dhash);

  md_free(name_hash);
  md_free(name_idx);
  md_free(name_data);
  md_free(name_names);

  return rv;
}


void db_close(db_t* db)
{
  if(!db) return;

  db_file_free(db->parts->index);
  db_file_free(db->parts->data);
  db_file_free(db->parts->name);

  cmph_destroy(db->parts->hash);

  md_free(db->parts);
  md_free(db);
}


const void* db_get(const db_t* db,const char* key,size_t* retlen)
{
  if(!db || !db->parts || !key || !*key || !retlen) return 0;

  db_part_t* p=db->parts;

  ssize_t r=cmph_search(p->hash,key,strlen(key));
  if(r<0 || r>=p->record_count) return 0;
  const db_idx_record_t* t=p->records+r;
  if(strcmp(p->names+t->noff,key)) return 0;

  *retlen=t->len;
  return p->strings+t->off;
}


const void* db_get2(const db_t* db,const char* key,size_t klen,size_t* retlen)
{
  if(!db || !db->parts || !key || !*key || !klen || !retlen) return 0;
  db_part_t* p=db->parts;

  ssize_t r=cmph_search(p->hash,key,klen);
  if(r<0 || r>=p->record_count) return 0;
  const db_idx_record_t* t=p->records+r;

//$msg("request <%.*s> found <%s>",(int)klen,key,(char*)(p->names+t->noff));
//$msg("%d %zd | %d",t->nlen,klen,t->len);
  if(t->nlen!=klen+1 || memcmp(p->names+t->noff,key,klen)) return 0;

  *retlen=t->len;
  return p->strings+t->off;
}



