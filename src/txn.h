#ifndef TXN_H_
#define TXN_H_

#include <stdbool.h>

#include <sqlite3.h>

struct txn
{
	unsigned long long id;
	sqlite3 *conn;
	bool is_zombie;
};

#endif /* TXN_H_*/
