#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <sqlite3.h>

#include "binary.h"
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
	m->body2.base = NULL;
	m->body2.len = 0;
	m->offset1 = 0;
	m->offset2 = 0;
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

	if (m->body2.base != NULL) {
		sqlite3_free(m->body2.base);
	}

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_MESSAGE);
}

void dqlite__message_header_recv_start(struct dqlite__message *m, uv_buf_t *buf)
{
	assert(m != NULL);
	assert(buf != NULL);

	/* The message header is stored in the first part of the dqlite_message
	 * structure. */
	buf->base = (char*)m;

	/* The length of the message header is fixed */
	buf->len = DQLITE__MESSAGE_HEADER_LEN;
}

int dqlite__message_header_recv_done(struct dqlite__message *m)
{
	assert(m != NULL);

	assert(m->body2.base == NULL);

	/* The message body can't be empty. */
	if (m->words == 0) {
		dqlite__error_printf(&m->error, "empty message body");
		return DQLITE_ERROR;
	}

	return 0;
}

static size_t dqlite__message_body_len(struct dqlite__message *m)
{
	assert(m != NULL);

	/* The message body size is the number of words multiplied by the size
	 * of each word. */
	return dqlite__flip32(m->words) * DQLITE__MESSAGE_WORD_SIZE;
}

/* Allocate the message body dynamic buffer. Used for reading or writing a
 * message body that is larger than the size of the static buffer. */
static int dqlite__message_body_alloc(struct dqlite__message *m, uint32_t words)
{
	size_t len;
	assert(m != NULL);
	assert(words > 0);

	/* The dynamic buffer shouldn't have been allocated yet. */
	assert(m->body2.base == NULL);
	assert(m->body2.len == 0);

	len = words * DQLITE__MESSAGE_WORD_SIZE;

	m->body2.base = (char*)sqlite3_malloc(len);
	if (m->body2.base == NULL) {
		dqlite__error_oom(&m->error, "failed to allocate message body buffer");
		return DQLITE_NOMEM;
	}

	m->body2.len = len;

	return 0;
}

int dqlite__message_body_recv_start(struct dqlite__message *m, uv_buf_t *buf)
{
	int err;

	assert(m != NULL);

	/* The dynamic buffer shouldn't have been allocated yet */
	assert(m->body2.base == NULL);
	assert(m->body2.len == 0);

	/* Check whether we need to allocate the dynamic buffer) */
	if (m->words > DQLITE__MESSAGE_BUF_LEN / DQLITE__MESSAGE_WORD_SIZE) {
		err = dqlite__message_body_alloc(m, m->words);
		if (err != 0) {
			return err;
		}
		buf->base = (char*)m->body2.base;
	} else {
		buf->base = (char*)m->body1;
	}

	buf->len = dqlite__message_body_len(m);

	return 0;
}

static int dqlite__message_get(struct dqlite__message *m, const char **dst, size_t len)
{
	size_t offset;  /* New offset */
	char *src;      /* Read buffer */
	size_t cap;     /* Size of the read buffer */
	uint32_t words; /* Words read so far */

	assert(m != NULL);
	assert(dst != NULL);
	assert(len > 0);

	/* The header must have been written already */
	assert(m->words > 0);

	cap = m->words * DQLITE__MESSAGE_WORD_SIZE;

	if (m->body2.base != NULL) {
		/* We allocated a dymanic buffer, let's use it */
		src = m->body2.base;
		offset = m->offset2;
	} else {
		src = m->body1;
		offset = m->offset1;
	}

	/* Check that we're not overflowing the buffer. */
	if (offset + len > cap) {
		dqlite__error_printf(&m->error, "read overflow");
		return DQLITE_OVERFLOW;
	}

	*dst = src + offset;

	offset = offset + len;

	/* Calculate the number of words that we read so far */
	words = offset / DQLITE__MESSAGE_WORD_SIZE;

	/* Check if reached the end of the message */
	if (words == m->words) {
		return DQLITE_EOM;
	}

	if (m->body2.base == NULL)
		m->offset1 = offset;
	else
		m->offset2 = offset;

	return 0;
}

int dqlite__message_body_get_text(struct dqlite__message *m, text_t *text)
{
	char *src;
	size_t offset;
	size_t cap;
	size_t len;

	/* The header must have been written already */
	assert(m->words > 0);

	/* A text entry must start at word boundary */
	assert((m->offset1 % DQLITE__MESSAGE_WORD_SIZE) == 0);
	assert((m->offset2 % DQLITE__MESSAGE_WORD_SIZE) == 0);

	if (m->body2.base != NULL) {
		/* We allocated a dymanic buffer, let's use it */
		src = m->body2.base;
		offset = m->offset2;
		cap = m->body2.len;
	} else {
		src = m->body1;
		offset = m->offset1;
		cap = DQLITE__MESSAGE_BUF_LEN;
	}

	cap = dqlite__message_body_len(m);

	/* Find the terminating null byte of the next string, if any. */
	src += offset;
	len = strnlen((const char*)src, cap - offset);

	if (len == cap - offset) {
		dqlite__error_printf(&m->error, "no string found");
		return DQLITE_PARSE;
	}

	len++; /* Terminating null byte */

	/* Account for padding */
	if ((len % DQLITE__MESSAGE_WORD_SIZE) != 0) {
		len += DQLITE__MESSAGE_WORD_SIZE - (len % DQLITE__MESSAGE_WORD_SIZE);
	}

	return dqlite__message_get(m, text, len);
}

int dqlite__message_body_get_text_list(struct dqlite__message *m, text_list_t *list)
{
	int err;
	size_t i = 0;
	text_t text;

	assert(m != NULL);
	assert(list != NULL);

	*list = NULL;

	do {
		err = dqlite__message_body_get_text(m, &text);
		if (err == 0 || err == DQLITE_EOM) {
			text_list_t new_list = *list;
			i++;
			new_list = sqlite3_realloc(new_list, sizeof(&text) * (i + 1));
			if (new_list == NULL) {
				sqlite3_free(*list);
				dqlite__error_oom(&m->error, "failed to allocate text list");
				return DQLITE_NOMEM;
			}
			new_list[i - 1] = text;
			new_list[i] = NULL;
			*list = new_list;
		}
	} while (err == 0);

	return err;
}

int dqlite__message_body_get_uint8(struct dqlite__message *m, uint8_t *value)
{
	int err;
	const char *buf;

	assert(m != NULL);
	assert(value != NULL);

	err = dqlite__message_get(m, &buf, sizeof(*value));
	if (err != 0 && err != DQLITE_EOM) {
		return err;
	}

	*value = *((uint8_t*)buf);

	return err;
}

int dqlite__message_body_get_uint32(struct dqlite__message *m, uint32_t *value)
{
	int err;
	const char *buf;

	assert(m != NULL);
	assert(value != NULL);

	/* A uint32 must be at 4-byte boundary */
	assert((m->offset1 % sizeof(*value)) == 0);
	assert((m->offset2 % sizeof(*value)) == 0);

	err = dqlite__message_get(m, &buf, sizeof(*value));
	if (err != 0 && err != DQLITE_EOM) {
		return err;
	}

	*value = dqlite__flip32(*((uint32_t*)buf));

	return err;
}

int dqlite__message_body_get_int64(struct dqlite__message *m, int64_t *value)
{
	int err;
	const char *buf;

	assert(m != NULL);
	assert(value != NULL);

	err = dqlite__message_get(m, &buf, sizeof(*value));
	if (err != 0 && err != DQLITE_EOM) {
		return err;
	}

	*value = (int64_t)dqlite__flip64(*((uint64_t*)buf));

	return err;
}


int dqlite__message_body_get_uint64(struct dqlite__message *m, uint64_t *value)
{
	int err;
	const char *buf;

	assert(m != NULL);
	assert(value != NULL);

	/* A uint64 entry must start at word boundary */
	assert((m->offset1 % DQLITE__MESSAGE_WORD_SIZE) == 0);
	assert((m->offset2 % DQLITE__MESSAGE_WORD_SIZE) == 0);

	err = dqlite__message_get(m, &buf, sizeof(*value));
	if (err != 0 && err != DQLITE_EOM) {
		return err;
	}

	*value = dqlite__flip64(*((uint64_t*)buf));

	return err;
}

void dqlite__message_header_put(struct dqlite__message *m, uint8_t type, uint8_t flags)
{
	assert(m != NULL);

	m->type = type;
	m->flags = flags;
}

static int dqlite__message_body_put(struct dqlite__message *m, const char *src, size_t len, size_t pad)
{
	size_t offset; /* Write offset */
	char *dst;     /* Write buffer to use */

	assert(m != NULL);
	assert(src != NULL);
	assert(len > 0);

	/* Check if we need to use the dynamic buffer. This happens if either:
	 *
	 * a) The dynamic buffer was previously allocated
	 * b) The size of the data to put would exceed the static buffer size
	 */
	if (
		m->body2.base != NULL ||                           /* a) */
		m->offset1 + len + pad > DQLITE__MESSAGE_BUF_LEN  /* b) */
		) {

		/* Check if we need to grow the dynamic buffer */
		if (m->offset2 + len + pad >= m->body2.len) {
			size_t cap;
			char *base;
			/* Overallocate a bit to avoid allocating again at the
			 * next write. */
			cap = m->offset2 + len + pad + 1024;
			base = sqlite3_realloc(m->body2.base, cap);
			if (base == NULL) {
				dqlite__error_oom(&m->error, "failed to allocate dynamic body");
				return DQLITE_NOMEM;
			}
			m->body2.base = base;
			m->body2.len = cap;
		}

		dst = m->body2.base;
		offset = m->offset2;
	} else {
		dst = m->body1;
		offset = m->offset1;
	}

	/* Write the data */
	memcpy(dst + offset, src, len);

	/* Add padding if needed */
	if (pad > 0)
		memset(dst + offset + len, 0, pad);

	if (m->body2.base == NULL)
		m->offset1 += len + pad;
	else
		m->offset2 += len + pad;

	return 0;
}

int dqlite__message_body_put_text(struct dqlite__message *m, text_t text)
{
	size_t pad;
	size_t len;

	assert(m != NULL);
	assert(text != NULL);

	/* Text entries must start at word boundary */
	assert((m->offset1 % DQLITE__MESSAGE_WORD_SIZE) == 0);
	assert((m->offset2 % DQLITE__MESSAGE_WORD_SIZE) == 0);

	len = strlen(text) + 1;

	/* Strings are padded so word-alignment is preserved for the next
	 * write. */
	pad = DQLITE__MESSAGE_WORD_SIZE - (len % DQLITE__MESSAGE_WORD_SIZE);
	if (pad == DQLITE__MESSAGE_WORD_SIZE) {
		pad = 0;
	}

	return dqlite__message_body_put(m, text, len, pad);
}

int dqlite__message_body_put_text_list(struct dqlite__message *m, text_list_t list)
{
	int err;
	size_t i;

	assert(m != NULL);
	assert(list != NULL);

	/* Text lists must start at word boundary */
	assert((m->offset1 % DQLITE__MESSAGE_WORD_SIZE) == 0);
	assert((m->offset2 % DQLITE__MESSAGE_WORD_SIZE) == 0);

	for (i = 0; list[i] != NULL; i++) {
		err = dqlite__message_body_put_text(m, list[i]);
		if (err != 0) {
			return err;
		}
	}

	return 0;
}

int dqlite__message_body_put_uint8(struct dqlite__message *m, uint8_t value)
{
	assert(m != NULL);

	return dqlite__message_body_put(m, (const char*)(&value), sizeof(value), 0);
}

int dqlite__message_body_put_uint32(struct dqlite__message *m, uint32_t value)
{
	assert(m != NULL);

	/* An uint32 must start at 4-byte boundary */
	assert((m->offset1 % sizeof(value)) == 0);
	assert((m->offset2 % sizeof(value)) == 0);

	value = dqlite__flip32(value);
	return dqlite__message_body_put(m, (const char*)(&value), sizeof(value), 0);
}

int dqlite__message_body_put_int64(struct dqlite__message *m, int64_t value)
{
	assert(m != NULL);

	/* An int64 must start at word boundary */
	assert((m->offset1 % DQLITE__MESSAGE_WORD_SIZE) == 0);
	assert((m->offset2 % DQLITE__MESSAGE_WORD_SIZE) == 0);

	value = (int64_t)dqlite__flip64((uint64_t)value);
	return dqlite__message_body_put(m, (const char*)(&value), sizeof(value), 0);
}

int dqlite__message_body_put_uint64(struct dqlite__message *m, uint64_t value)
{
	assert(m != NULL);

	/* An uint64 must start at word boundary */
	assert((m->offset1 % DQLITE__MESSAGE_WORD_SIZE) == 0);
	assert((m->offset2 % DQLITE__MESSAGE_WORD_SIZE) == 0);

	value = dqlite__flip64(value);
	return dqlite__message_body_put(m, (const char*)(&value), sizeof(value), 0);
}

void dqlite__message_send_start(struct dqlite__message *m, uv_buf_t bufs[3])
{
	assert(m != NULL);
	assert(bufs != NULL);

	/* The word count shouldn't have been written out yet */
	assert(m->words == 0);

	/* Something should have been written in the body */
	assert(m->offset1 > 0 );

	/* The number of bytes written should be a multiple of the word size */
	assert((m->offset1 % DQLITE__MESSAGE_WORD_SIZE) == 0);
	assert((m->offset2 % DQLITE__MESSAGE_WORD_SIZE) == 0);

	m->words = dqlite__flip32((m->offset1 + m->offset2) / DQLITE__MESSAGE_WORD_SIZE);

	/* The message header is stored in the first part of the dqlite_message
	 * structure. */
	bufs[0].base = (char*)m;

	/* The length of the message header is fixed */
	bufs[0].len = DQLITE__MESSAGE_HEADER_LEN;

	bufs[1].base = m->body1;
	bufs[1].len = m->offset1;

	bufs[2].base = m->body2.base;
	bufs[2].len = m->offset2;

	return;
}

void dqlite__message_send_reset(struct dqlite__message *m)
{
	assert(m != NULL);

	/* Reset the state so we can start writing another message */
	if (m->body2.base != NULL) {
		sqlite3_free(m->body2.base);
	}
	dqlite__message_reset(m);
}

void dqlite__message_recv_reset(struct dqlite__message *m)
{
	assert(m != NULL);

	/* This must come after the header has been received */
	assert(m->words > 0);

	/* Reset the state so we can start reading another message */
	if (m->body2.base != NULL) {
		sqlite3_free(m->body2.base);
	}
	dqlite__message_reset(m);
}
