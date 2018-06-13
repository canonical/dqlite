#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>

#include <CUnit/CUnit.h>

#include "../src/schema.h"
#include "../src/request.h"
#include "../include/dqlite.h"

#include "message.h"
#include "request.h"

DQLITE__SCHEMA_ENCODER_IMPLEMENT(test_request, DQLITE__REQUEST_SCHEMA_TYPES);

TEST_MESSAGE_SEND_IMPLEMENT(helo, DQLITE_HELO, test_request, DQLITE__REQUEST_SCHEMA_HELO);
TEST_MESSAGE_SEND_IMPLEMENT(heartbeat, DQLITE_HEARTBEAT, test_request, DQLITE__REQUEST_SCHEMA_HEARTBEAT);
TEST_MESSAGE_SEND_IMPLEMENT(open, DQLITE_OPEN, test_request, DQLITE__REQUEST_SCHEMA_OPEN);
