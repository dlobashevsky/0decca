
typedef struct db_t db_t;

struct cfg_build_t;
struct cfg_server_t;

int db_build(const struct cfg_build_t*);
int db_build_tiles(const struct cfg_build_t*);

db_t* db_open(const char* folder);
void db_close(db_t*);

const void* db_get(const db_t* db,const char* key,size_t* retlen);
const void* db_get2(const db_t* db,const char* key,size_t klen,size_t* retlen);

