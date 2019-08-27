#ifndef DQLITE_PROTOCOL_H_
#define DQLITE_PROTOCOL_H_

/* Special datatypes */
#define DQLITE_UNIXTIME 9
#define DQLITE_ISO8601 10
#define DQLITE_BOOLEAN 11

#define DQLITE_PROTO 1001 /* Protocol error */

/* Current protocol version */
#define DQLITE_PROTOCOL_VERSION 1

/* Legacly pre-1.0 version. */
#define DQLITE_PROTOCOL_VERSION_LEGACY 0x86104dd760433fe5

/* Special value indicating that a batch of rows is over, but there are more. */
#define DQLITE_RESPONSE_ROWS_PART 0xeeeeeeeeeeeeeeee

/* Special value indicating that the result set is complete. */
#define DQLITE_RESPONSE_ROWS_DONE 0xffffffffffffffff

/* Request types */
#define DQLITE_REQUEST_LEADER 0
#define DQLITE_REQUEST_CLIENT 1
#define DQLITE_REQUEST_HEARTBEAT 2
#define DQLITE_REQUEST_OPEN 3
#define DQLITE_REQUEST_PREPARE 4
#define DQLITE_REQUEST_EXEC 5
#define DQLITE_REQUEST_QUERY 6
#define DQLITE_REQUEST_FINALIZE 7
#define DQLITE_REQUEST_EXEC_SQL 8
#define DQLITE_REQUEST_QUERY_SQL 9
#define DQLITE_REQUEST_INTERRUPT 10
#define DQLITE_REQUEST_CONNECT 11
#define DQLITE_REQUEST_JOIN 12
#define DQLITE_REQUEST_PROMOTE 13
#define DQLITE_REQUEST_REMOVE 14
#define DQLITE_REQUEST_DUMP 15
#define DQLITE_REQUEST_CLUSTER 16

/* Response types */
#define DQLITE_RESPONSE_FAILURE 0
#define DQLITE_RESPONSE_SERVER 1
#define DQLITE_RESPONSE_SERVER_LEGACY 1
#define DQLITE_RESPONSE_WELCOME 2
#define DQLITE_RESPONSE_SERVERS 3
#define DQLITE_RESPONSE_DB 4
#define DQLITE_RESPONSE_STMT 5
#define DQLITE_RESPONSE_RESULT 6
#define DQLITE_RESPONSE_ROWS 7
#define DQLITE_RESPONSE_EMPTY 8
#define DQLITE_RESPONSE_FILES 9

#endif /* DQLITE_PROTOCOL_H_ */
