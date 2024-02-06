/* Utilities around error handling. */

#ifndef ERROR_H_
#define ERROR_H_

#include <stddef.h>
#include <string.h>

#define ERR_CODE_TO_STRING_MAP(X)                                       \
    X(RAFT_NOMEM, "out of memory")                                      \
    X(RAFT_BADID, "server ID is not valid")                             \
    X(RAFT_DUPLICATEID, "server ID already in use")                     \
    X(RAFT_DUPLICATEADDRESS, "server address already in use")           \
    X(RAFT_BADROLE, "server role is not valid")                         \
    X(RAFT_MALFORMED, "encoded data is malformed")                      \
    X(RAFT_NOTLEADER, "server is not the leader")                       \
    X(RAFT_LEADERSHIPLOST, "server has lost leadership")                \
    X(RAFT_SHUTDOWN, "server is shutting down")                         \
    X(RAFT_CANTBOOTSTRAP, "bootstrap only works on new clusters")       \
    X(RAFT_CANTCHANGE, "a configuration change is already in progress") \
    X(RAFT_CORRUPT, "persisted data is corrupted")                      \
    X(RAFT_CANCELED, "operation canceled")                              \
    X(RAFT_NAMETOOLONG, "resource name too long")                       \
    X(RAFT_TOOBIG, "data is too big")                                   \
    X(RAFT_NOCONNECTION, "no connection to remote server available")    \
    X(RAFT_BUSY, "operation can't be performed at this time")           \
    X(RAFT_IOERR, "I/O error")                                          \
    X(RAFT_NOTFOUND, "Resource not found")                              \
    X(RAFT_INVALID, "Invalid parameter")                                \
    X(RAFT_UNAUTHORIZED, "No access to resource")                       \
    X(RAFT_NOSPACE, "Not enough disk space")                            \
    X(RAFT_TOOMANY, "System or raft limit met or exceeded")

/* Format an error message. */
#define ErrMsgPrintf(ERRMSG, ...) \
    snprintf(ERRMSG, RAFT_ERRMSG_BUF_SIZE, __VA_ARGS__)

/* Wrap the given error message with an additional prefix message.. */
#define ErrMsgWrapf(ERRMSG, ...)            \
    do {                                    \
        char _errmsg[RAFT_ERRMSG_BUF_SIZE]; \
        ErrMsgPrintf(_errmsg, __VA_ARGS__); \
        errMsgWrap(ERRMSG, _errmsg);        \
    } while (0)

void errMsgWrap(char *e, const char *format);

/* Transfer an error message from an object to another, wrapping it. */
#define ErrMsgTransfer(ERRMSG1, ERRMSG2, FORMAT)    \
    memcpy(ERRMSG2, ERRMSG1, RAFT_ERRMSG_BUF_SIZE); \
    ErrMsgWrapf(ERRMSG2, FORMAT)

#define ErrMsgTransferf(ERRMSG1, ERRMSG2, FORMAT, ...) \
    memcpy(ERRMSG2, ERRMSG1, RAFT_ERRMSG_BUF_SIZE);    \
    ErrMsgWrapf(ERRMSG2, FORMAT, __VA_ARGS__)

/* Use the static error message for the error with the given code. */
#define ErrMsgFromCode(ERRMSG, CODE) \
    ErrMsgPrintf(ERRMSG, "%s", errCodeToString(CODE))

/* Format the out of memory error message. */
#define ErrMsgOom(ERRMSG) ErrMsgFromCode(ERRMSG, RAFT_NOMEM)

/* Convert a numeric raft error code to a human-readable error message. */
const char *errCodeToString(int code);

#endif /* ERROR_H_ */
