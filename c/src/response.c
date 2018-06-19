#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "dqlite.h"
#include "error.h"
#include "lifecycle.h"
#include "response.h"
#include "message.h"
#include "schema.h"

DQLITE__SCHEMA_IMPLEMENT(dqlite__response_welcome, DQLITE__RESPONSE_SCHEMA_WELCOME);
DQLITE__SCHEMA_IMPLEMENT(dqlite__response_servers, DQLITE__RESPONSE_SCHEMA_SERVERS);
DQLITE__SCHEMA_IMPLEMENT(dqlite__response_db_error, DQLITE__RESPONSE_SCHEMA_DB_ERROR);
DQLITE__SCHEMA_IMPLEMENT(dqlite__response_db, DQLITE__RESPONSE_SCHEMA_DB);
DQLITE__SCHEMA_IMPLEMENT(dqlite__response_stmt, DQLITE__RESPONSE_SCHEMA_STMT);
DQLITE__SCHEMA_IMPLEMENT(dqlite__response_result, DQLITE__RESPONSE_SCHEMA_RESULT);
DQLITE__SCHEMA_IMPLEMENT(dqlite__response_rows, DQLITE__RESPONSE_SCHEMA_ROWS);
DQLITE__SCHEMA_IMPLEMENT(dqlite__response_empty, DQLITE__RESPONSE_SCHEMA_EMPTY);

DQLITE__SCHEMA_ENCODER_IMPLEMENT(dqlite__response, DQLITE__RESPONSE_SCHEMA_TYPES);
