#ifndef DQLITE_H
#define DQLITE_H

#include <stdio.h>
#include <stdint.h>

#define DQLITE_OK    0
#define DQLITE_ERROR 1
#define DQLITE_NOMEM 2
#define DQLITE_PROTO 3

#define DQLITE_PROTOCOL_VERSION 0xbf93ea39

typedef struct dqlite__service dqlite_service;
typedef struct dqlite_cluster {
  void *ctx;
  const char * (*xLeader)(void*);
  const char **(*xServers)(void*);
  int          (*xRecover)(void*, uint64_t);
} dqlite_cluster;

dqlite_service *dqlite_service_alloc();
void dqlite_service_free(dqlite_service *s);

int dqlite_service_init(dqlite_service *s, FILE *log, dqlite_cluster *cluster);
void dqlite_service_close(dqlite_service *s);

int dqlite_service_run(dqlite_service *s);

/* Stop a dqlite service.
**
** This is a thread-safe API.
**
** In case of error, the caller must invoke sqlite3_free
** against the returned errmsg.
*/
int dqlite_service_stop(dqlite_service *s, char **errmsg);

/* Start handling a new connection.
**
** This is a thread-safe API.
**
** In case of error, the caller must invoke sqlite3_free
** against the returned errmsg.
*/
int dqlite_service_handle(dqlite_service *s, int socket, char **errrmsg);

const char* dqlite_service_errmsg(dqlite_service*);

#endif /* DQLITE_H */
