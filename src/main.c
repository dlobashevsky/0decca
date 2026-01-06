#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "macros.h"
#include "db.h"
#include "config.h"
#include "cfg.h"
#include "server.h"

static const char* usage="c0defeed [-b buildconfig.json | -t buildfromtiles.json | -s serverconfig.json]\nOptions are mutually exclusive\n";


int main(int ac,char** av)
{
  int c;
  const char* scfg=0;
  const char* bcfg=0;
  int tiles=0;

  while((c=getopt(ac,av,"hb:s:t:"))!=-1)
    switch (c)
    {
      case 'h':
        fprintf(stderr,"%s",usage);
        break;
      case 'b':
        bcfg=optarg;
        tiles=0;
        break;
      case 't':
        bcfg=optarg;
        tiles=1;
        break;
      case 's':
        scfg=optarg;
        break;
      default:
        fprintf(stderr,"%s",usage);
        return -1;
    }
  if(scfg && bcfg)
  {
    fprintf(stderr,"%s",usage);
    return -1;
  }

  if(bcfg)
  {
    cfg_build_t* cb=cfg_init_build(bcfg);
    if(!cb) $abort("config read error");
    int ret=tiles ? db_build_tiles(cb) : db_build(cb);
    cfg_build_free(cb);
    return 0;
  }

  if(scfg)
  {
    cfg_server_t* cs=cfg_init_server(scfg);
    if(!cs) $abort("config read error");
    server_start(cs);
    cfg_server_free(cs);
    return 0;
  }
$msg("done");
  return 0;
}
