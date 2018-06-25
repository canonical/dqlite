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

void dqlite__stmt_init(struct dqlite__stmt *s)
{
	assert(s != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_STMT);

	dqlite__error_init(&s->error);
}

void dqlite__stmt_close(struct dqlite__stmt *s)
{
	assert(s != NULL);

	if (s->stmt != NULL) {
		/* Ignore the return code, since it will be non-zero in case the
		 * most rececent evaluation of the statement failed. */
		sqlite3_finalize(s->stmt); }

	dqlite__error_close(&s->error);

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
	assert(rc != NULL);

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
	struct dqlite__stmt *s,
	uint64_t *last_insert_id,
	uint64_t *rows_affected)
{
	int rc;

	assert(s != NULL);
	assert(s->stmt != NULL);

	rc = sqlite3_step(s->stmt);
	if (rc != SQLITE_DONE)
		return rc;

	*last_insert_id = sqlite3_last_insert_rowid(s->db);
	*rows_affected = sqlite3_changes(s->db);

	return 0;
}

static int dqlite__stmt_put_row(
	struct dqlite__stmt *s,
	struct dqlite__message *message,
	int column_count)
{
	int err;
	int i;
	int pad;
	int header_bits;
	int *column_types;
	uint8_t slot = 0;

	assert(s != NULL);
	assert(message != NULL);
	assert(column_count > 0);

	/* Allocate an array to store the column column_types */
	column_types = (int*)sqlite3_malloc(column_count * sizeof(*column_types));
	if (column_types == NULL) {
		dqlite__error_oom(&s->error, "failed to create column column_types array");
		return DQLITE_NOMEM;
	}

	/* Each column needs a 4 byte slot to store the column type. The row
	 * header must be padded to reach word boundary. */
	header_bits = column_count * 4;
	if ((header_bits % DQLITE__MESSAGE_WORD_BITS) != 0) {
		pad = (DQLITE__MESSAGE_WORD_BITS - (header_bits % DQLITE__MESSAGE_WORD_BITS)) / 4;
	} else {
		pad = 0;
	}

	/* Write the row header */
	for (i = 0; i < column_count + pad; i++) {
		int column_type;

		if (i < column_count) {
			/* Actual column, fetch the type */
			column_type = sqlite3_column_type(s->stmt, i);
			assert(column_type < 16);
			column_types[i] = column_type;
		} else {
			/* Padding */
			column_type = 0;
		}

		if (i % 2 == 0) {
			/* Fill the lower 4 bits of the slot */
			slot = column_type;
		} else {
			/* Fill the higher 4 bits of the slot and flush it */
			slot |= column_type << 4;
			err = dqlite__message_body_put_uint8(message, slot);
			if (err != 0) {
				assert(err == DQLITE_NOMEM);
				dqlite__error_wrapf(
					&s->error, &message->error,
					"failed to write row header");
				goto out;
			}
		}
	}

	/* Write the row columns */
	for (i = 0; i < column_count; i++) {
		int64_t integer;
		double float_;
		text_t text;

		switch (column_types[i]) {
		case SQLITE_INTEGER:
			integer = sqlite3_column_int64(s->stmt, i);
			err = dqlite__message_body_put_int64(message, integer);
			break;
		case SQLITE_FLOAT:
			float_ = sqlite3_column_double(s->stmt, i);
			err = dqlite__message_body_put_double(message, float_);
			break;
		case SQLITE_BLOB:
			assert(0); /* TODO */
			break;
		case SQLITE_NULL:
			/* TODO: allow null to be encoded with 0 bytes */
			err = dqlite__message_body_put_int64(message, 0);
			break;
		case SQLITE_TEXT:
			text = (text_t)sqlite3_column_text(s->stmt, i);
			err = dqlite__message_body_put_text(message, text);
			break;
		default:
			dqlite__error_printf(&s->error, "unknown type %d for column %d", column_types[i], i);
			err = DQLITE_ERROR;
			goto out;
		}

		if (err != 0) {
			break;
		}
	}

 out:
	sqlite3_free(column_types);

	return err;
}

int dqlite__stmt_query(
	struct dqlite__stmt *s,
	struct dqlite__message *message,
	int *rc)
{
	int err;
	int column_count;
	int i;

	assert(s != NULL);
	assert(s->stmt != NULL);
	assert(message != NULL);
	assert(rc != NULL);

	column_count = sqlite3_column_count(s->stmt);
	if (column_count <= 0) {
		dqlite__error_printf(&s->error, "stmt doesn't yield any column");
	}

	/* Insert the column count */
	err = dqlite__message_body_put_uint64(message, (uint64_t)column_count);
	if (err != 0) {
		return err;
	}

	/* Insert the column names */
	for (i = 0; i < column_count; i++) {
		const char *name = sqlite3_column_name(s->stmt, i);
		err = dqlite__message_body_put_text(message, name);
		if (err != 0) {
			return err;
		}
	}

	err = 0; /* In case there's no row and the loop breaks immediately */
	do {
		*rc = sqlite3_step(s->stmt);
		if (*rc != SQLITE_ROW)
			break;

		err = dqlite__stmt_put_row(s, message, column_count);
		if (err != 0) {
			return err;
		}

	} while (1);

	return 0;
}

DQLITE__REGISTRY_METHODS(dqlite__stmt_registry, dqlite__stmt);
