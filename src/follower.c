#include "follower.h"

void follower__init(struct follower *f, sqlite3 *conn) {
	f->conn = conn;
}

const char *follower__filename(struct follower *f)
{
	return sqlite3_db_filename(f->conn, "main");
}
