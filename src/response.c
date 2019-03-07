#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "assert.h"
#include "error.h"
#include "lifecycle.h"
#include "message.h"
#include "response.h"
#include "schema.h"

SCHEMA__IMPLEMENT(dqlite__response_failure, DQLITE__RESPONSE_SCHEMA_FAILURE);
SCHEMA__IMPLEMENT(dqlite__response_server, DQLITE__RESPONSE_SCHEMA_SERVER);
SCHEMA__IMPLEMENT(dqlite__response_welcome, DQLITE__RESPONSE_SCHEMA_WELCOME);
SCHEMA__IMPLEMENT(dqlite__response_servers, DQLITE__RESPONSE_SCHEMA_SERVERS);
SCHEMA__IMPLEMENT(dqlite__response_db, DQLITE__RESPONSE_SCHEMA_DB);
SCHEMA__IMPLEMENT(dqlite__response_stmt, DQLITE__RESPONSE_SCHEMA_STMT);
SCHEMA__IMPLEMENT(dqlite__response_result, DQLITE__RESPONSE_SCHEMA_RESULT);
SCHEMA__IMPLEMENT(dqlite__response_rows, DQLITE__RESPONSE_SCHEMA_ROWS);
SCHEMA__IMPLEMENT(dqlite__response_empty, DQLITE__RESPONSE_SCHEMA_EMPTY);

SCHEMA__HANDLER_IMPLEMENT(dqlite__response, DQLITE__RESPONSE_SCHEMA_TYPES);
