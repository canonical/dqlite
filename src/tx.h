#ifndef TX_H_
#define TX_H_

#include <stdbool.h>

#include <sqlite3.h>

struct tx
{
	unsigned long long id;
	sqlite3 *conn;
	bool is_zombie;
};

#endif /* TX_H_*/
