#include "bind.h"
#include <sqlite3.h>
#include "protocol.h"
#include "tuple.h"
#include "utils.h"

/* Bind a single parameter. */
static int bind_one(sqlite3_stmt *stmt, int n, struct value *value)
{
	int rc;

	switch (value->type) {
	case SQLITE_INTEGER:
		rc = sqlite3_bind_int64(stmt, n, value->integer);
		break;
	case SQLITE_FLOAT:
		rc = sqlite3_bind_double(stmt, n, value->real);
		break;
	case SQLITE_BLOB:
		rc = sqlite3_bind_blob(stmt, n, value->blob.base,
						(int)value->blob.len,
						SQLITE_STATIC);
		break;
	case SQLITE_NULL:
		rc = sqlite3_bind_null(stmt, n);
		break;
	case SQLITE_TEXT:
		rc = sqlite3_bind_text(stmt, n, value->text, -1,
						SQLITE_STATIC);
		break;
	case DQLITE_ISO8601:
		rc = sqlite3_bind_text(stmt, n, value->text, -1,
						SQLITE_STATIC);
		break;
	case DQLITE_BOOLEAN:
		rc = sqlite3_bind_int64(stmt, n,
					value->boolean == 0 ? 0 : 1);
		break;
	default:
		IMPOSSIBLE("value outside of enum");
	}

	if (rc != 0) {
		return DQLITE_ERROR;
	}
	return DQLITE_OK;
}

int bind__params(sqlite3_stmt *stmt, struct tuple_decoder *decoder)
{
	int requested = sqlite3_bind_parameter_count(stmt);
	int available = (int)tuple_decoder__remaining(decoder);

	for (int i = 0; i < requested && i < available; i++) {
		struct value value;
		int rc = tuple_decoder__next(decoder, &value);
		if (rc != 0) {
			return rc;
		}
		rc = bind_one(stmt, (int)(i + 1), &value);
		if (rc != 0) {
			return rc;
		}
	}

	return DQLITE_OK;
}
