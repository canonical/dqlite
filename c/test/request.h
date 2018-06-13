#ifndef DQLITE_TEST_REQUEST_H
#define DQLITE_TEST_REQUEST_H

#include <unistd.h>
#include <stdint.h>

#include "../src/message.h"
#include "../src/request.h"
#include "../src/schema.h"

#include "message.h"

DQLITE__SCHEMA_ENCODER_DEFINE(test_request, DQLITE__REQUEST_SCHEMA_TYPES);

TEST_MESSAGE_SEND_DEFINE(helo, DQLITE__REQUEST_SCHEMA_HELO);
TEST_MESSAGE_SEND_DEFINE(heartbeat, DQLITE__REQUEST_SCHEMA_HEARTBEAT);
TEST_MESSAGE_SEND_DEFINE(open, DQLITE__REQUEST_SCHEMA_OPEN);

#endif /* DQLITE_TEST_REQUEST_H */
