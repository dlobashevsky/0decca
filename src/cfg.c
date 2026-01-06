#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include <jansson.h>

#include "macros.h"
#include "cfg.h"



cfg_build_t *CFGB=0;
cfg_server_t *CFGS=0;

cfg_build_t* cfg_init_build(const char* json_filename)
{
  json_error_t error;
  json_t *j=json_load_file(json_filename,0, &error);
  if(!j) $abort(error.text);

  if(CFGB) cfg_build_free(CFGB);
  cfg_build_t* rv=md_new(rv);

  if(json_unpack(j,"{s:s,s:s,s:b}","src",&rv->src,"db",&rv->db,"dedup",&rv->dedup))  $abort("unpack error");
  rv->src=strdup(rv->src);
  rv->db=strdup(rv->db);

/*
  dedup
  parts
*/


  json_decref(j);

  CFGB=rv;
  return rv;
}

static char* hjoin(json_t* j,const char* fl,int tail)
{
  if(!j || !json_is_array(j)) $abort("headers must be array of strings");

  size_t l=0;
  char* m=0;
  FILE *mem=open_memstream(&m,&l);
  fprintf(mem,"%s\r\n",fl);
  size_t s=json_array_size(j);
  for(size_t i=0;i<s;i++)
  {
    json_t* n=json_array_get(j,i);
    if(!json_is_string(n)) $abort("headers must be strings");
    fprintf(mem,"%s\r\n",json_string_value(n));
  }
  if(tail) fprintf(mem,"\r\n");
  fclose(mem);
  return m;
}

cfg_server_t* cfg_init_server(const char* json_filename)
{
  json_error_t error;
  json_t *j=json_load_file(json_filename,0, &error);
  if(!j) $abort(error.text);

  if(CFGS) cfg_server_free(CFGS);
  cfg_server_t* rv=md_new(rv);

  json_t* h=0;
  json_t* nf=0;
  if(json_unpack(j,"{s:s,s:s,s:o,s:o,s:i,s:i,s:i,s:i}","db",&rv->db,"socket",&rv->socket,"headers",&h,"h404",&nf,"threads",&rv->threads,"port",&rv->port,"backlog",&rv->backlog,"inbuffer",&rv->inbuf))  $abort("unpack error");

  rv->db=strdup(rv->db);
  rv->socket=strdup(rv->socket);
/*
  size_t l=0;
  char* m=0;
  FILE *mem=open_memstream(&m,&l);
  fprintf(mem,"HTTP/1.1 200 OK\r\n");
  if(h)
  {
    if(!json_is_array(h)) $abort("headers must be array of strings");
    size_t s=json_array_size(h);
    for(size_t i=0;i<s;i++)
    {
      json_t* n=json_array_get(h,i);
      if(!json_is_string(n)) $abort("headers must be strings");
      fprintf(mem,"%s\r\n",json_string_value(n));
    }
  }
  fclose(mem);
  rv->headers=m;
*/
  rv->headers=hjoin(h,"HTTP/1.1 200 OK",0);
  rv->h404=hjoin(nf,"HTTP/1.1 404 Not Found",1);

  json_decref(j);
  CFGS=rv;
  return rv;
}



void cfg_build_free(cfg_build_t* cfg)
{
  if(!cfg) return;
  free(cfg->src);
  free(cfg->db);
  md_free(cfg);
  CFGB=0;
}

void cfg_server_free(cfg_server_t* cfg)
{
  if(!cfg) return;
  free(cfg->db);
  free(cfg->socket);
  free(cfg->headers);
  free(cfg->h404);
  md_free(cfg);
  CFGS=0;
}
