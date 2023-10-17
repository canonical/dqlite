#include "bind.h"
#include "tuple.h"

/* Bind a single parameter. */
static int bind_one(sqlite3_stmt *stmt, int n, const struct value *value)
{
	int rc;

	/* TODO: the binding calls below currently use SQLITE_TRANSIENT when
	 * passing pointers to data (for TEXT or BLOB datatypes). This way
	 * SQLite makes its private copy of the data before the bind call
	 * returns, and we can reuse the message body buffer. The overhead of
	 * the copy is typically low, but if it becomes a concern, this could be
	 * optimized to make no copy and instead prevent the message body from
	 * being reused. */
	switch (value->type) {
		case SQLITE_INTEGER:
			rc = sqlite3_bind_int64(stmt, n, value->integer);
			break;
		case SQLITE_FLOAT:
			rc = sqlite3_bind_double(stmt, n, value->float_);
			break;
		case SQLITE_BLOB:
			rc = sqlite3_bind_blob(stmt, n, value->blob.base,
					       (int)value->blob.len,
					       SQLITE_TRANSIENT);
			break;
		case SQLITE_NULL:
			rc = sqlite3_bind_null(stmt, n);
			break;
		case SQLITE_TEXT:
			rc = sqlite3_bind_text(stmt, n, value->text, -1,
					       SQLITE_TRANSIENT);
			break;
		case DQLITE_ISO8601:
			rc = sqlite3_bind_text(stmt, n, value->text, -1,
					       SQLITE_TRANSIENT);
			break;
		case DQLITE_BOOLEAN:
			rc = sqlite3_bind_int64(stmt, n,
						value->boolean == 0 ? 0 : 1);
			break;
		default:
			rc = DQLITE_PROTO;
			break;
	}

	return rc;
}

int parseParams(struct cursor *cursor, int format, struct value **out)
{
	struct tuple_decoder decoder;
	struct value *head;
	struct value *prev;
	unsigned long i;
	int rv;

	assert(format == TUPLE__PARAMS || format == TUPLE__PARAMS32);

	/* If the payload has been fully consumed, it means there are no
	 * parameters to bind. */
	if (cursor->cap == 0) {
		return 0;
	}

	rv = tuple_decoder__init(&decoder, 0, format, cursor);
	if (rv != 0) {
		goto err;
	}

	head = sqlite3_malloc(sizeof(*head));
	if (head == NULL) {
		rv = DQLITE_NOMEM;
		goto err;
	}
	prev = head;
	for (i = 0; i < tuple_decoder__n(&decoder); i++) {
		prev->next = sqlite3_malloc(sizeof(*prev->next));
		if (prev->next == NULL) {
			goto err_after_alloc_head;
		}
		rv = tuple_decoder__next(&decoder, prev->next);
		if (rv != 0) {
			goto err_after_alloc_head;
		}
		prev = prev->next;
	}

	*out = head;
	return 0;

err_after_alloc_head:
	freeParams(head);
err:
	return rv;
}

int bindParams(sqlite3_stmt *stmt, const struct value *params)
{
	int i;
	int rv;

	i = 1;
	for (const struct value *cur = params; cur != NULL; cur = cur->next) {
		rv = bind_one(stmt, i, cur);
		if (rv != 0) {
			return rv;
		}
		i += 1;
	}
	return 0;
}

void freeParams(struct value *params)
{
	struct value *cur;
	struct value *old;

	cur = params;
	while (cur != NULL) {
		old = cur;
		cur = old->next;
		sqlite3_free(old);
	}
}
