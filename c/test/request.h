#ifndef DQLITE_TEST_REQUEST_H
#define DQLITE_TEST_REQUEST_H

#include <unistd.h>
#include <stdint.h>

#include "../src/message.h"

/* Helpers to render dqlite requests. */
void test_request_helo(struct dqlite__message *m, uint64_t client_id);
void test_request_heartbeat(struct dqlite__message *m, uint64_t timestamp);
void test_request_open(struct dqlite__message *m, const char *name);

#endif /* DQLITE_TEST_REQUEST_H */
