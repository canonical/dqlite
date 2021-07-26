/* Translate to/from dqlite types */

#ifndef DQLITE_TRANSLATE_H_
#define DQLITE_TRANSLATE_H_

/* Translate a raft error to a dqlite one. */
int translateRaftErrCode(int code);

/* Translate a dqlite role code to its raft equivalent. */
int translateDqliteRole(int role);

/* Translate a raft role code to its dqlite equivalent. */
int translateRaftRole(int role);

#endif /* DQLITE_TRANSLATE_H_ */
