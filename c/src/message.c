#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <sqlite3.h>

#include "dqlite.h"
#include "lifecycle.h"
#include "message.h"

static void dqlite__message_reset(struct dqlite__message *m)
{
	assert(m != NULL);

	m->type = 0;
	m->flags = 0;
	m->words = 0;
	m->extra = 0;
	m->buf2 = NULL;
	m->offset = 0;
	m->cap = 0;
}

void dqlite__message_init(struct dqlite__message *m)
{
	assert(m != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_MESSAGE);

	dqlite__message_reset(m);

	dqlite__error_init(&m->error);
}

void dqlite__message_close(struct dqlite__message *m)
{
	assert(m != NULL);

	dqlite__error_close(&m->error);

	if (m->buf2 != NULL) {
		sqlite3_free(m->buf2);
	}

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_MESSAGE);
}

void dqlite__message_header_buf(
	struct dqlite__message *m,
	uint8_t **buf,
	size_t *len)
{
	assert(m != NULL);
	assert(buf != NULL);
	assert(len != NULL);

	/* The message header is stored in the first part of the dqlite_message
	 * structure. */
	*buf = (uint8_t*)m;

	/* The length is fixed */
	*len = DQLITE__MESSAGE_HEADER_LEN;
}

size_t dqlite__message_body_len(struct dqlite__message *m)
{
	assert(m != NULL);

	/* The message body size is the number of words multiplied by the size
	 * of each word. */
	return dqlite__message_flip32(m->words) * DQLITE__MESSAGE_WORD_SIZE;
}

int dqlite__message_header_received(struct dqlite__message *m)
{
	int err;
	size_t len;

	assert(m != NULL);
	assert(m->buf2 == NULL);

	len = dqlite__message_body_len(m);

	/* The message body can't be empty. */
	if (len == 0) {
		dqlite__error_printf(&m->error, "empty message body");
		return DQLITE_ERROR;
	}

	/* Check whether we need to allocate the dynamic buffer) */
	if (len > DQLITE__MESSAGE_BUF_LEN) {
		err = dqlite__message_alloc(m, len);
		if (err != 0) {
			return err;
		}
	}

	return 0;
}

void dqlite__message_body_buf(
	struct dqlite__message *m,
	uint8_t **buf,
	size_t *len)
{
	assert(m != NULL);

	if (m->buf2 != NULL) {
		*buf = m->buf2;
	} else {
		*buf = m->buf1;
	}

	*len = dqlite__message_body_len(m);

	assert(*buf);

	/* TODO: abort the connection when the message header says that the body
	 * has zero words */
	/* assert(*len); */

	return;
}

int dqlite__message_body_received(struct dqlite__message *m)
{
	assert(m != NULL);

	return 0;
}

/* Allocate the dynamic buffer. Used for writing bodies larger than the size of
 * the static buffer. */
int dqlite__message_alloc(struct dqlite__message *m, uint32_t words)
{
	size_t len;
	assert(m != NULL);
	assert(words > 0);

	/* The dynamic buffer should have been allocated yet. */
	assert(m->buf2 == NULL);
	assert(m->cap == 0);

	len = words * DQLITE__MESSAGE_WORD_SIZE;

	m->buf2 = (uint8_t*)sqlite3_malloc(len);
	if (m->buf2 == NULL) {
		dqlite__error_oom(&m->error, "failed to allocate message body buffer");
		return DQLITE_NOMEM;
	}

	m->cap = len;

	return 0;
}

static int dqlite__message_write(
	struct dqlite__message *m,
	uint8_t *data,
	size_t len)
{
	size_t offset; /* New offset */
	uint8_t *buf; /* Write buffer */
	size_t cap; /* Size of the write buffer */

	assert(m != NULL);
	assert(data != NULL);
	assert(len > 0);

	/* The header shouldn't have been written out yet */
	assert(m->words == 0);
	assert(m->type == 0);
	assert(m->flags == 0);
	assert(m->extra == 0);

	if (m->buf2 != NULL) {
		/* We allocated a dymanic buffer, let's use it */
		assert(m->cap > 0);

		buf = m->buf2;
		cap = m->cap;
	} else {
		buf = m->buf1;
		cap = DQLITE__MESSAGE_BUF_LEN;
	}

	offset = m->offset + len;

	/* Check that we're not overflowing the buffer. */
	if (offset > cap) {
		dqlite__error_printf(&m->error, "write overflow");
		return DQLITE_OVERFLOW;
	}

	memcpy(buf + m->offset, data, len);
	m->offset = offset;

	return 0;
}

/* Read data from the message body. */
static int dqlite__message_read(
	struct dqlite__message *m,
	uint8_t **data,
	size_t len)
{
	size_t offset; /* New offset */
	uint8_t *buf; /* Read buffer */
	size_t cap; /* Size of the read buffer */
	uint32_t words; /* Words read so far */

	assert(m != NULL);
	assert(data != NULL);
	assert(len > 0);

	/* The header must have been written already */
	assert(m->words > 0);

	if (m->buf2 != NULL) {
		/* We allocated a dymanic buffer, let's use it */
		buf = m->buf2;
	} else {
		buf = m->buf1;
	}

	cap = dqlite__message_body_len(m);

	offset = m->offset + len;

	/* Check that we're not overflowing the buffer. */
	if (offset > cap) {
		dqlite__error_printf(&m->error, "read overflow");
		return DQLITE_OVERFLOW;
	}

	*data = buf + m->offset;
	m->offset = offset;

	/* Calculate the number of words that we read so far */
	words = m->offset / DQLITE__MESSAGE_WORD_SIZE;
	if ((m->offset % DQLITE__MESSAGE_WORD_SIZE) != 0) {
		words++;
	}

	/* Check if reached the end of the message */
	if (words == m->words) {
		return DQLITE_EOM;
	}

	return 0;
}

int dqlite__message_write_text(struct dqlite__message *m, const char *text)
{
	return dqlite__message_write(m, (uint8_t*)text, strlen(text) + 1);
}

int dqlite__message_read_text(struct dqlite__message *m, const char **text)
{
	uint8_t *buf;
	size_t cap;
	size_t len;

	/* The header must have been written already */
	assert(m->words > 0);

	if (m->buf2 != NULL) {
		/* We allocated a dymanic buffer, let's use it */
		buf = m->buf2;
	} else {
		buf = m->buf1;
	}

	cap = dqlite__message_body_len(m);

	/* Find the terminating null byte of the next string, if any. */
	len = strnlen((const char*)buf, cap - m->offset);

	if (len == cap) {
		dqlite__error_printf(&m->error, "no string found");
		return DQLITE_PARSE;
	}

	return dqlite__message_read(m, (uint8_t**)text, len + 1);
}

int dqlite__message_write_int64(struct dqlite__message *m, int64_t value)
{
	value = (int64_t)dqlite__message_flip64((uint64_t)value);
	return dqlite__message_write(m, (uint8_t*)(&value), sizeof(value));
}

int dqlite__message_read_int64(struct dqlite__message *m, int64_t *value)
{
	int err;
	uint8_t *buf;

	assert(m != NULL);
	assert(value != NULL);

	err = dqlite__message_read(m, &buf, sizeof(*value));
	if (err != 0 && err != DQLITE_EOM) {
		return err;
	}

	*value = (int64_t)dqlite__message_flip64(*((uint64_t*)buf));

	return err;
}

int dqlite__message_write_uint64(struct dqlite__message *m, uint64_t value)
{
	value = dqlite__message_flip64(value);
	return dqlite__message_write(m, (uint8_t*)(&value), sizeof(value));
}

int dqlite__message_read_uint64(struct dqlite__message *m, uint64_t *value)
{
	int err;
	uint8_t *buf;

	assert(m != NULL);
	assert(value != NULL);

	err = dqlite__message_read(m, &buf, sizeof(*value));
	if (err != 0 && err != DQLITE_EOM) {
		return err;
	}

	*value = dqlite__message_flip64(*((uint64_t*)buf));

	return err;
}

void dqlite__message_flush(struct dqlite__message *m, uint8_t type, uint8_t flags)
{
	int err;
	uint32_t words;
	size_t padding;

	assert(m != NULL);

	/* The header shouldn't have been written out yet */
	assert(m->words == 0);
	assert(m->type == 0);
	assert(m->flags == 0);
	assert(m->extra == 0);

	/* Something should have been written in the body */
	assert(m->offset > 0 );

	padding = DQLITE__MESSAGE_WORD_SIZE - (m->offset % DQLITE__MESSAGE_WORD_SIZE);
	if (padding != DQLITE__MESSAGE_WORD_SIZE) {
		uint64_t data = 0;
		err = dqlite__message_write(m, (uint8_t*)(&data), padding);

		/* This write can't fail since we allocate only buffers whose
		 * size is a multiple of a word, so if the offset is not at word
		 * boundary, there must be at least 'padding' bytes before the
		 * cap is reached. */
		assert(err == 0);
	} else {
		padding = 0;
	}

	words = (m->offset + padding) / DQLITE__MESSAGE_WORD_SIZE;

	m->words = dqlite__message_flip32(words);
	m->type = type;
	m->flags = flags;

	/* Reset cap and offset */
	m->offset = 0;
	if (m->buf2 != NULL) {
		assert(m->cap > 0);
		m->cap = 0;
	}
}

/* Should be called after a message has been read completely and it has been
 * processed. It resets the internal state so the object can be re-used for
 * receiving another message */
void dqlite__message_processed(struct dqlite__message *m)
{
	assert(m != NULL);

	/* This must come after the header has been received */
	assert(m->words > 0);

	/* Reset the state so we can start reading or writing another
	 * message */
	if (m->buf2 != NULL) {
		sqlite3_free(m->buf2);
	}
	dqlite__message_reset(m);
}
