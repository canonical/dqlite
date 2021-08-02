#include "translate.h"

#include <raft.h>

#include "assert.h"
#include "leader.h"
#include "protocol.h"

/* Translate a raft error to a dqlite one. */
int translateRaftErrCode(int code)
{
	switch (code) {
		case RAFT_NOTLEADER:
			return SQLITE_IOERR_NOT_LEADER;
		case RAFT_LEADERSHIPLOST:
			return SQLITE_IOERR_LEADERSHIP_LOST;
		case RAFT_CANTCHANGE:
			return SQLITE_BUSY;
		default:
			return SQLITE_ERROR;
	}
}

/* Translate a dqlite role code to its raft equivalent. */
int translateDqliteRole(int role)
{
	switch (role) {
		case DQLITE_VOTER:
			return RAFT_VOTER;
		case DQLITE_STANDBY:
			return RAFT_STANDBY;
		case DQLITE_SPARE:
			return RAFT_SPARE;
		default:
			/* For backward compat with clients that don't set a
			 * role. */
			return DQLITE_VOTER;
	}
}

/* Translate a raft role code to its dqlite equivalent. */
int translateRaftRole(int role)
{
	switch (role) {
		case RAFT_VOTER:
			return DQLITE_VOTER;
		case RAFT_STANDBY:
			return DQLITE_STANDBY;
		case RAFT_SPARE:
			return DQLITE_SPARE;
		default:
			assert(0);
			return -1;
	}
}
