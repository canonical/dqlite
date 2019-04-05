#include "request.h"

#define REQUEST__IMPLEMENT(LOWER, UPPER, _) \
	SERIALIZE__IMPLEMENT(request_##LOWER, REQUEST_##UPPER);

REQUEST__TYPES(REQUEST__IMPLEMENT, );

SERIALIZE__IMPLEMENT(request_connect, REQUEST_CONNECT);

SCHEMA__IMPLEMENT(request_leader_, REQUEST__SCHEMA_LEADER);
SCHEMA__IMPLEMENT(request_client_, REQUEST__SCHEMA_CLIENT);
SCHEMA__IMPLEMENT(request_heartbeat, REQUEST__SCHEMA_HEARTBEAT);
SCHEMA__IMPLEMENT(request_open_, REQUEST__SCHEMA_OPEN);
SCHEMA__IMPLEMENT(request_prepare_, REQUEST__SCHEMA_PREPARE);
SCHEMA__IMPLEMENT(request_exec_, REQUEST__SCHEMA_EXEC);
SCHEMA__IMPLEMENT(request_query_, REQUEST__SCHEMA_QUERY);
SCHEMA__IMPLEMENT(request_finalize_, REQUEST__SCHEMA_FINALIZE);
SCHEMA__IMPLEMENT(request_exec_sql_, REQUEST__SCHEMA_EXEC_SQL);
SCHEMA__IMPLEMENT(request_query_sql_, REQUEST__SCHEMA_QUERY_SQL);
SCHEMA__IMPLEMENT(request_interrupt, REQUEST__SCHEMA_INTERRUPT);

SCHEMA__HANDLER_IMPLEMENT(request, REQUEST__SCHEMA_TYPES);
