#include <assert.h>
#include <stddef.h>

#include <sqlite3.h>

#include "dqlite.h"
#include "error.h"
#include "lifecycle.h"
#include "registry.h"
#include "stmt.h"

/* The maximum number of columns we expect (for bindings or rows) is 255, which
 * can fit in one byte. */
#define DQLITE__STMT_MAX_COLUMNS (1 << 8) - 1

void dqlite__stmt_init(struct dqlite__stmt *stmt)
{
	assert(stmt != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_STMT);

	dqlite__error_init(&stmt->error);
}

void dqlite__stmt_close(struct dqlite__stmt *stmt)
{
	assert(stmt != NULL);

	if (stmt->stmt != NULL) {
		/* Ignore the return code, since it will be non-zero in case the
		 * most rececent evaluation of the statement failed. */
		sqlite3_finalize(stmt->stmt); }

	dqlite__error_close(&stmt->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_STMT);
}

int dqlite__stmt_bind(
	struct dqlite__stmt *s,
	struct dqlite__message *message,
	int *rc)
{
	int err;
	uint8_t param_count;
	uint8_t pad = 0;
	uint8_t i;
	uint8_t param_types[DQLITE__STMT_MAX_COLUMNS]; /* TODO: allow more than 255 params */

	assert(s != NULL);
	assert(s->stmt != NULL);
	assert(message != NULL);

	err = dqlite__message_body_get_uint8(message, &param_count);

	assert(err == 0 || err == DQLITE_OVERFLOW || err == DQLITE_EOM);

	if (err == DQLITE_OVERFLOW) {
		 /* No bindings were provided */
		return 0;
	}

	if (err == DQLITE_EOM) {
		/* The body contains a column count, but nothing more */
		dqlite__error_printf(&s->error, "no param types provided");
		return DQLITE_PROTO;
	}

	/* Clients are expected to pad the column type bytes to reach the word
	 * boundary (including the first byte, the param count) */
	if ((param_count + 1) % DQLITE__MESSAGE_WORD_SIZE != 0) {
		pad = DQLITE__MESSAGE_WORD_SIZE - ((param_count + 1) % DQLITE__MESSAGE_WORD_SIZE);
	}

	for (i = 0; i < param_count + pad; i++) {
		err = dqlite__message_body_get_uint8(message, &param_types[i]);
		if (err != 0) {
			/* The only possible way this should fail is end-of-message */
			assert(err == DQLITE_EOM);
			dqlite__error_printf(&s->error, "incomplete param types");
			return DQLITE_PROTO;
		}
	}

	for (i = 0; i < param_count; i++) {
		int64_t integer;
		double float_;
		text_t text;

		switch (param_types[i]) {
		case SQLITE_INTEGER:
			err = dqlite__message_body_get_int64(message, &integer);
			if (err == 0 || err == DQLITE_EOM) {
				*rc = sqlite3_bind_int64(s->stmt, i + 1, integer);
			}
			break;
		case SQLITE_FLOAT:
			err = dqlite__message_body_get_double(message, &float_);
			if (err == 0 || err == DQLITE_EOM) {
				*rc = sqlite3_bind_double(s->stmt, i + 1, float_);
			}
			break;
		case SQLITE_BLOB:
			assert(0); /* TODO */
			break;
		case SQLITE_NULL:
			/* TODO: allow null to be encoded with 0 bytes */
			err = dqlite__message_body_get_int64(message, &integer);
			if (err == 0 || err == DQLITE_EOM) {
				*rc = sqlite3_bind_null(s->stmt, i + 1);
			}
			break;
		case SQLITE_TEXT:
			err = dqlite__message_body_get_text(message, &text);
			if (err == 0 || err == DQLITE_EOM) {
				*rc = sqlite3_bind_text(s->stmt, i + 1, text, -1, SQLITE_STATIC);
			}
			break;
		default:
			dqlite__error_printf(&s->error, "unknown type %d for param %d", param_types[i], i);
			return DQLITE_PROTO;
		}

		if (err != 0 || *rc != SQLITE_OK) {
			break;
		}
	}

	if (err != DQLITE_EOM) {
		/* TODO: differentiate between the various cases, e.g. err == 0,
		 * err == DQLITE_OVERFLOW, err == DQLITE_PARSE */
		dqlite__error_printf(&s->error, "invalid params");
		return DQLITE_PROTO;
	}

	if (*rc != SQLITE_OK) {
		return DQLITE_ENGINE;
	}

	return 0;
}

int dqlite__stmt_exec(
	struct dqlite__stmt *stmt,
	uint64_t *last_insert_id,
	uint64_t *rows_affected)
{
	int rc;

	assert(stmt != NULL);
	assert(stmt->stmt != NULL);

	rc = sqlite3_step(stmt->stmt);
	if (rc != SQLITE_DONE)
		return rc;

	*last_insert_id = sqlite3_last_insert_rowid(stmt->db);
	*rows_affected = sqlite3_changes(stmt->db);

	return 0;
}

DQLITE__REGISTRY_METHODS(dqlite__stmt_registry, dqlite__stmt);
