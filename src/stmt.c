#include <assert.h>
#include <stddef.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

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
		sqlite3_finalize(s->stmt);
	}

	dqlite__error_close(&s->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_STMT);
}

const char *dqlite__stmt_hash(struct dqlite__stmt *stmt)
{
	(void)stmt;

	return NULL;
}

/* Bind a parameter. */
static int dqlite__stmt_bind_param(struct dqlite__stmt *   s,
                                   struct dqlite__message *message,
                                   int                     i,
                                   int                     type,
                                   int *                   rc)
{
	int64_t  integer;
	double   float_;
	uint64_t null;
	text_t   text;
	int      err;
	uint64_t flag;

	assert(s != NULL);

	/* TODO: the binding calls below currently use SQLITE_TRANSIENT when
	 * passing pointers to data (for TEXT or BLOB datatypes). This way
	 * SQLite makes its private copy of the data before the bind call
	 * returns, and we can reuse the message body buffer. The overhead of
	 * the copy is typically low, but if it becomes a concern, this could be
	 * optimized to make no copy and instead prevent the message body from
	 * being reused. */
	switch (type) {

	case SQLITE_INTEGER:
		err = dqlite__message_body_get_int64(message, &integer);
		if (err == 0 || err == DQLITE_EOM) {
			*rc = sqlite3_bind_int64(s->stmt, i, integer);
		}
		break;

	case SQLITE_FLOAT:
		err = dqlite__message_body_get_double(message, &float_);
		if (err == 0 || err == DQLITE_EOM) {
			*rc = sqlite3_bind_double(s->stmt, i, float_);
		}
		break;

	case SQLITE_BLOB:
		assert(0); /* TODO */
		break;

	case SQLITE_NULL:
		/* TODO: allow null to be encoded with 0 bytes? */
		err = dqlite__message_body_get_uint64(message, &null);
		if (err == 0 || err == DQLITE_EOM) {
			*rc = sqlite3_bind_null(s->stmt, i);
		}
		break;

	case SQLITE_TEXT:
		err = dqlite__message_body_get_text(message, &text);
		if (err == 0 || err == DQLITE_EOM) {
			*rc = sqlite3_bind_text(
			    s->stmt, i, text, -1, SQLITE_TRANSIENT);
		}
		break;

	case DQLITE_ISO8601:
		err = dqlite__message_body_get_text(message, &text);
		if (err == 0 || err == DQLITE_EOM) {
			*rc = sqlite3_bind_text(
			    s->stmt, i, text, -1, SQLITE_TRANSIENT);
		}
		break;

	case DQLITE_BOOLEAN:
		err = dqlite__message_body_get_uint64(message, &flag);
		if (err == 0 || err == DQLITE_EOM) {
			*rc = sqlite3_bind_int64(s->stmt, i, flag == 0 ? 0 : 1);
		}
		break;

	default:
		dqlite__error_printf(&s->error, "unknown type %d", type);
		return DQLITE_PROTO;
	}

	return err;
}

int dqlite__stmt_bind(struct dqlite__stmt *s, struct dqlite__message *message)
{
	int     err;
	uint8_t pad = 0;
	uint8_t i;
	uint8_t count;
	uint8_t types[DQLITE__MESSAGE_MAX_BINDINGS];

	assert(s != NULL);
	assert(s->stmt != NULL);
	assert(message != NULL);

	sqlite3_reset(s->stmt);

	/* First check if we reached the end of the message. Since bindings are
	 * always the last part of a message, no further data means that no
	 * bindings were supplied and there's nothing to do. */
	if (dqlite__message_has_been_fully_consumed(message)) {
		return SQLITE_OK;
	}

	/* Get the number of parameters. The maximum value is 255. */
	err = dqlite__message_body_get_uint8(message, &count);
	assert(err == 0);

	/* Clients are expected to pad the column type bytes to reach the word
	 * boundary (including the first byte, the param count) */
	if ((count + 1) % DQLITE__MESSAGE_WORD_SIZE != 0) {
		pad = DQLITE__MESSAGE_WORD_SIZE -
		      ((count + 1) % DQLITE__MESSAGE_WORD_SIZE);
	}

	/* Get the type of each parameter. */
	for (i = 0; i < count + pad; i++) {
		err = dqlite__message_body_get_uint8(message, &types[i]);
		if (err != 0) {
			/* The only possible way this should fail is
			 * end-of-message
			 */
			assert(err == DQLITE_EOM);
			if (i == count + pad - 1) {
				/* All parameter types are present, but there's
				 * no further data holding parameter values. */
				dqlite__error_printf(&s->error,
				                     "incomplete param values");
			} else {
				/* Not all parameter types were provided. */
				dqlite__error_printf(&s->error,
				                     "incomplete param types");
			}
			return SQLITE_ERROR;
		}
	}

	/* Get the value of each parameter. */
	for (i = 0; i < count; i++) {
		int rc = SQLITE_OK;

		err = dqlite__stmt_bind_param(s, message, i + 1, types[i], &rc);
		if (err == DQLITE_EOM) {
			if (i != count - 1) {
				/* We reached the end of the message but we did
				 * not exhaust the parameters. */
				dqlite__error_printf(&s->error,
				                     "incomplete param values");
				return SQLITE_ERROR;
			}
		} else if (err != 0) {
			dqlite__error_wrapf(
			    &s->error, &s->error, "invalid param %d", i + 1);
			return SQLITE_ERROR;
		}

		if (rc != SQLITE_OK) {
			dqlite__error_printf(&s->error, sqlite3_errmsg(s->db));
			return rc;
		}
	}

	return SQLITE_OK;
}

int dqlite__stmt_exec(struct dqlite__stmt *s,
                      uint64_t *           last_insert_id,
                      uint64_t *           rows_affected)
{
	int rc;

	assert(s != NULL);
	assert(s->stmt != NULL);

	rc = sqlite3_step(s->stmt);
	if (rc != SQLITE_DONE) {
		dqlite__error_printf(&s->error, sqlite3_errmsg(s->db));
		return rc;
	}

	*last_insert_id = sqlite3_last_insert_rowid(s->db);
	*rows_affected  = sqlite3_changes(s->db);

	return SQLITE_OK;
}

/* Append a single row to the message. */
static int dqlite__stmt_row(struct dqlite__stmt *   s,
                            struct dqlite__message *message,
                            int                     column_count)
{
	int     err;
	int     i;
	int     pad;
	int     header_bits;
	int *   column_types;
	uint8_t slot = 0;

	assert(s != NULL);
	assert(message != NULL);
	assert(column_count > 0);

	/* Allocate an array to store the column types
	 *
	 * TODO: use a statically allocated buffer when the column count is
	 * small. */
	column_types =
	    (int *)sqlite3_malloc(column_count * sizeof(*column_types));
	if (column_types == NULL) {
		dqlite__error_oom(&s->error,
		                  "failed to create column column_types array");
		return SQLITE_NOMEM;
	}

	/* Each column needs a 4 byte slot to store the column type. The row
	 * header must be padded to reach word boundary. */
	header_bits = column_count * 4;
	if ((header_bits % DQLITE__MESSAGE_WORD_BITS) != 0) {
		pad = (DQLITE__MESSAGE_WORD_BITS -
		       (header_bits % DQLITE__MESSAGE_WORD_BITS)) /
		      4;
	} else {
		pad = 0;
	}

	/* Write the row header */
	for (i = 0; i < column_count + pad; i++) {
		int column_type;

		if (i < column_count) {
			/* Actual column, figure the type */
			const char *column_type_name;

			column_type = sqlite3_column_type(s->stmt, i);

			/* TODO: find a better way to handle time types */
			column_type_name = sqlite3_column_decltype(s->stmt, i);
			if (column_type_name != NULL) {
				if (strcmp(column_type_name, "DATETIME") == 0) {
					if (column_type == SQLITE_INTEGER) {
						column_type = DQLITE_UNIXTIME;
					} else {
						assert(column_type ==
						           SQLITE_TEXT ||
						       column_type ==
						           SQLITE_NULL);
						column_type = DQLITE_ISO8601;
					}
				}
				if (strcmp(column_type_name, "BOOLEAN") == 0) {
					assert(column_type == SQLITE_INTEGER ||
					       column_type == SQLITE_NULL);
					column_type = DQLITE_BOOLEAN;
				}
			}

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
				    &s->error,
				    &message->error,
				    "failed to write row header");
				err = SQLITE_NOMEM;
				goto out;
			}
		}
	}

	/* Write the row columns */
	for (i = 0; i < column_count; i++) {
		int64_t integer;
		double  float_;
		text_t  text;

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
			err  = dqlite__message_body_put_text(message, text);
			break;
		case DQLITE_UNIXTIME:
			integer = sqlite3_column_int64(s->stmt, i);
			err = dqlite__message_body_put_int64(message, integer);
			break;
		case DQLITE_ISO8601:
			text = (text_t)sqlite3_column_text(s->stmt, i);
			if (text == NULL)
				text = "";
			err = dqlite__message_body_put_text(message, text);
			break;
		case DQLITE_BOOLEAN:
			integer = sqlite3_column_int64(s->stmt, i);
			err     = dqlite__message_body_put_uint64(message,
                                                              integer != 0);
			break;
		default:
			dqlite__error_printf(&s->error,
			                     "unknown type %d for column %d",
			                     column_types[i],
			                     i);
			err = SQLITE_ERROR;
			goto out;
		}

		if (err != 0) {
			break;
		}
	}

out:
	sqlite3_free(column_types);

	if (err != 0) {
		assert(!dqlite__error_is_null(&s->error));
		return SQLITE_ERROR;
	}

	return SQLITE_OK;
}

int dqlite__stmt_query(struct dqlite__stmt *s, struct dqlite__message *message)
{
	int column_count;
	int err;
	int i;
	int rc;

	assert(s != NULL);
	assert(s->stmt != NULL);
	assert(message != NULL);

	column_count = sqlite3_column_count(s->stmt);
	if (column_count <= 0) {
		dqlite__error_printf(&s->error,
		                     "stmt doesn't yield any column");
		return SQLITE_ERROR;
	}

	/* Insert the column count */
	err = dqlite__message_body_put_uint64(message, (uint64_t)column_count);
	if (err != 0) {
		dqlite__error_wrapf(&s->error,
		                    &message->error,
		                    "failed to encode column count");
		return SQLITE_ERROR;
	}

	/* Insert the column names */
	for (i = 0; i < column_count; i++) {
		const char *name = sqlite3_column_name(s->stmt, i);

		err = dqlite__message_body_put_text(message, name);
		if (err != 0) {
			dqlite__error_wrapf(&s->error,
			                    &message->error,
			                    "failed to encode column name %d",
			                    i);
			return SQLITE_ERROR;
		}
	}

	/* Insert the rows. */
	do {
		if (dqlite__message_is_large(message)) {
			/* If we are already filled the static buffer, let's
			 * break for now, we'll send more rows in a separate
			 * response. */
			rc = SQLITE_ROW;
			break;
		}

		rc = sqlite3_step(s->stmt);
		if (rc != SQLITE_ROW) {
			break;
		}

		rc = dqlite__stmt_row(s, message, column_count);
		if (rc != SQLITE_OK) {
			break;
		}

	} while (1);

	return rc;
}

DQLITE__REGISTRY_METHODS(dqlite__stmt_registry, dqlite__stmt);
