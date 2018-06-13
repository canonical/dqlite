#include <assert.h>
#include <string.h>

#include <CUnit/CUnit.h>

#include "../src/response.h"

#include "response.h"

DQLITE__SCHEMA_DECODER_IMPLEMENT(test_response, DQLITE__RESPONSE_SCHEMA_TYPES);

TEST_MESSAGE_SEND_IMPLEMENT(welcome, DQLITE_WELCOME, dqlite__response, DQLITE__RESPONSE_SCHEMA_WELCOME);
TEST_MESSAGE_SEND_IMPLEMENT(servers, DQLITE_SERVERS, dqlite__response, DQLITE__RESPONSE_SCHEMA_SERVERS);
TEST_MESSAGE_SEND_IMPLEMENT(db, DQLITE_DB, dqlite__response, DQLITE__RESPONSE_SCHEMA_DB);
