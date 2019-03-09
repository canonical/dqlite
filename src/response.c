#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "error.h"
#include "lifecycle.h"
#include "message.h"
#include "response.h"
#include "schema.h"

SCHEMA__IMPLEMENT(response_failure, RESPONSE__SCHEMA_FAILURE);
SCHEMA__IMPLEMENT(response_server, RESPONSE__SCHEMA_SERVER);
SCHEMA__IMPLEMENT(response_welcome, RESPONSE__SCHEMA_WELCOME);
SCHEMA__IMPLEMENT(response_servers, RESPONSE__SCHEMA_SERVERS);
SCHEMA__IMPLEMENT(response_db, RESPONSE__SCHEMA_DB);
SCHEMA__IMPLEMENT(response_stmt, RESPONSE__SCHEMA_STMT);
SCHEMA__IMPLEMENT(response_result, RESPONSE__SCHEMA_RESULT);
SCHEMA__IMPLEMENT(response_rows, RESPONSE__SCHEMA_ROWS);
SCHEMA__IMPLEMENT(response_empty, RESPONSE__SCHEMA_EMPTY);

SCHEMA__HANDLER_IMPLEMENT(response, RESPONSE__SCHEMA_TYPES);
