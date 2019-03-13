#include "db.h"

void db__init(struct db *db, const char *filename)
{
	db->filename = filename;
}
