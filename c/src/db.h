#ifndef DQLITE_DB_H
#define DQLITE_DB_H

#include <sqlite3.h>

/* Hold state for a single open SQLite database */
struct dqlite__db {
	sqlite3* db; /* Underlying SQLite database */
};

#endif /* DQLITE_DB_H */
