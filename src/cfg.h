typedef struct cfg_build_t
{
  char* src;
  char* db;
// size_t parts;
  int dedup;
} cfg_build_t;

typedef struct cfg_server_t
{
  char* db;
  char* socket;
  int backlog;
  int inbuf;
  char* headers;
  char* h404;
  int threads;
  int port;

// log settings
// metrics
// health
} cfg_server_t;


extern cfg_build_t* CFGB;
extern cfg_server_t* CFGS;

cfg_build_t* cfg_init_build(const char* json_filename);
cfg_server_t* cfg_init_server(const char* json_filename);

void cfg_build_free(cfg_build_t*);
void cfg_server_free(cfg_server_t*);
