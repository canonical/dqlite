#ifndef DQLITE_LIFECYCLE_H
#define DQLITE_LIFECYCLE_H

/*
 * APIs to help debugging object lifecycle issues, such as non-matching
 * numbers of xxx_init and xxx_close calls.
 */

#define DQLITE__LIFECYCLE_ERROR      0
#define DQLITE__LIFECYCLE_FSM        1
#define DQLITE__LIFECYCLE_MESSAGE    2
#define DQLITE__LIFECYCLE_REQUEST    3
#define DQLITE__LIFECYCLE_RESPONSE   4
#define DQLITE__LIFECYCLE_GATEWAY    5
#define DQLITE__LIFECYCLE_CONN       6
#define DQLITE__LIFECYCLE_QUEUE      7
#define DQLITE__LIFECYCLE_QUEUE_ITEM 8
#define DQLITE__LIFECYCLE_DB         9

#ifdef DQLITE_DEBUG
void dqlite__lifecycle_init(int type);
void dqlite__lifecycle_close(int type);
#else
#define dqlite__lifecycle_init(x)
#define dqlite__lifecycle_close(x)
#endif /* DQLITE_DEBUG */

/* Return 0 if all initialized objects have been closed, or otherwise an error
 * message describing what has been leaked */
int dqlite__lifecycle_check(char **errmsg);

#endif /* DQLITE_LIFECYCLE_H */

