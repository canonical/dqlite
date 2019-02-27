#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "binary.h"
#include "lifecycle.h"
#include "message.h"

/* This ensures that doubles are 64-bit long
 *
 * See https://stackoverflow.com/questions/752309/ensuring-c-doubles-are-64-bits
 */
#ifndef __STDC_IEC_559__
#if __SIZEOF_DOUBLE__ != 8
#error "Requires IEEE 754 floating point!"
#endif
#endif

static void dqlite__message_reset(struct dqlite__message *m)
{
	assert(m != NULL);

	m->type       = 0;
	m->flags      = 0;
	m->words      = 0;
	m->extra      = 0;
	m->body2.base = NULL;
	m->body2.len  = 0;
	m->offset1    = 0;
	m->offset2    = 0;
}

void dqlite__message_init(struct dqlite__message *m)
{
	assert(m != NULL);

	/* The statically allocated buffer is aligned to word boundary */
	/* TODO: re-enable this assertion once we figure a way to do make it
	 * work on 32-bit. */
	/* assert((uintptr_t)m->body1 % DQLITE__MESSAGE_WORD_SIZE == 0); */

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
	buf->base = (char *)m;

	/* The length of the message header is fixed */
	buf->len = DQLITE__MESSAGE_HEADER_LEN;
}

int dqlite__message_header_recv_done(struct dqlite__message *m)
{
	assert(m != NULL);

	assert(m->body2.base == NULL);

	m->words = dqlite__flip32(m->words);
	/* The message body can't be empty. */
	if (m->words == 0) {
		dqlite__error_printf(&m->error, "empty message body");
		return DQLITE_PROTO;
	}

	/* The message body can't exeed DQLITE__MESSAGE_MAX_WORDS. */
	if (m->words > DQLITE__MESSAGE_MAX_WORDS) {
		dqlite__error_printf(&m->error, "message body too large");
		return DQLITE_PROTO;
	}

	return 0;
}

static size_t dqlite__message_body_len(struct dqlite__message *m)
{
	assert(m != NULL);
	assert(m->words > 0);

	/* The message body size is the number of words multiplied by the size
	 * of each word. */
	return m->words * DQLITE__MESSAGE_WORD_SIZE;
}

/* Allocate the message body dynamic buffer. Used for reading or writing a
 * message body that is larger than the size of the static buffer. */
static int dqlite__message_body_alloc(struct dqlite__message *m)
{
	size_t len;
	assert(m != NULL);

	/* The dynamic buffer shouldn't have been allocated yet. */
	assert(m->body2.base == NULL);
	assert(m->body2.len == 0);

	len = dqlite__message_body_len(m);

	m->body2.base = sqlite3_malloc(len);
	if (m->body2.base == NULL) {
		dqlite__error_oom(&m->error,
		                  "failed to allocate message body buffer");
		return DQLITE_NOMEM;
	}

	m->body2.len = len;

	return 0;
}

int dqlite__message_body_recv_start(struct dqlite__message *m, uv_buf_t *buf)
{
	int err;

	assert(m != NULL);

	/* The read offsets should be clean. */
	assert(m->offset1 == 0);
	assert(m->offset2 == 0);

	/* Check whether we need to allocate the dynamic buffer) */
	if (m->words > DQLITE__MESSAGE_BUF_WORDS) {
		err = dqlite__message_body_alloc(m);
		if (err != 0) {
			assert(err == DQLITE_NOMEM);
			return err;
		}
		buf->base = m->body2.base;
		buf->len  = m->body2.len;
	} else {
		buf->base = m->body1;
		buf->len  = dqlite__message_body_len(m);
	}

	return 0;
}

/* Return true if the current read or write offset is aligned to the given
 * quantity. */
static int dqlite__message_body_is_offset_aligned(struct dqlite__message *m,
                                                  size_t                  len)
{
	int align; /* Expected offset alignment */

	assert(m != NULL);

	/* Possible aligments should be 1, 2, 4, or 8 bytes. */
	if (len % 8 == 0) {
		align = 8;
	} else if (len % 4 == 0) {
		align = 4;
	} else {
		align = 1;
	}

	return (m->offset1 % align) == 0 && (m->offset2 % align) == 0;
}

static int
dqlite__message_get(struct dqlite__message *m, const char **dst, size_t len)
{
	size_t   offset; /* New offset */
	char *   src;    /* Read buffer */
	size_t   cap;    /* Size of the read buffer */
	uint32_t words;  /* Words read so far */

	assert(m != NULL);
	assert(dst != NULL);
	assert(len > 0);

	/* The header must have been written already */
	assert(m->words > 0);

	/* Check aligment. */
	if (!dqlite__message_body_is_offset_aligned(m, len)) {
		dqlite__error_printf(&m->error, "misaligned read");
		return DQLITE_PARSE;
	}

	cap = m->words * DQLITE__MESSAGE_WORD_SIZE;

	if (m->body2.base != NULL) {
		/* We allocated a dymanic buffer, let's use it */
		src    = m->body2.base;
		offset = m->offset2;
	} else {
		src    = m->body1;
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

	if (m->body2.base == NULL) {
		m->offset1 = offset;
	} else {
		m->offset2 = offset;
	}

	/* Check if reached the end of the message */
	if (words == m->words) {
		return DQLITE_EOM;
	}

	return 0;
}

int dqlite__message_body_get_text(struct dqlite__message *m, text_t *text)
{
	char * src;
	size_t offset;
	size_t cap;
	size_t len;

	/* The header must have been written already */
	assert(m->words > 0);

	if (m->body2.base != NULL) {
		/* We allocated a dymanic buffer, let's use it */
		src    = m->body2.base;
		offset = m->offset2;
	} else {
		/* Otherwise use the static buffer */
		src    = m->body1;
		offset = m->offset1;
	}

	src += offset;
	cap = dqlite__message_body_len(m) - offset;

	/* Find the terminating null byte of the next string, if any. */
	len = strnlen((const char *)src, cap);

	if (len == cap) {
		dqlite__error_printf(&m->error, "no string found");
		return DQLITE_PARSE;
	}

	len++; /* Terminating null byte */

	/* Account for padding */
	if ((len % DQLITE__MESSAGE_WORD_SIZE) != 0) {
		len += DQLITE__MESSAGE_WORD_SIZE -
		       (len % DQLITE__MESSAGE_WORD_SIZE);
	}

	return dqlite__message_get(m, text, len);
}

int dqlite__message_body_get_servers(struct dqlite__message *m,
                                     servers_t *             servers)
{
	int      err;
	size_t   i = 0;
	uint64_t id;
	text_t   address;

	assert(m != NULL);
	assert(servers != NULL);

	*servers = NULL;

	do {
		err = dqlite__message_body_get_uint64(m, &id);
		if (err != 0) {
			dqlite__error_printf(&m->error,
			                     "missing server address");
			err = DQLITE_PROTO;
			break;
		}

		err = dqlite__message_body_get_text(m, &address);
		if (err == 0 || err == DQLITE_EOM) {
			servers_t new_servers = *servers;
			i++;
			new_servers = sqlite3_realloc(
			    new_servers, sizeof(*new_servers) * (i + 1));
			if (new_servers == NULL) {
				if (*servers != NULL) {
					sqlite3_free(*servers);
				}
				dqlite__error_oom(
				    &m->error,
				    "failed to allocate servers list");
				return DQLITE_NOMEM;
			}
			new_servers[i - 1].id      = id;
			new_servers[i - 1].address = address;
			new_servers[i].id          = 0;
			new_servers[i].address     = NULL;
			*servers                   = new_servers;
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

	*value = *((uint8_t *)buf);

	return err;
}

int dqlite__message_body_get_uint32(struct dqlite__message *m, uint32_t *value)
{
	int err;

	const char *buf;

	assert(m != NULL);
	assert(value != NULL);

	err = dqlite__message_get(m, &buf, sizeof(*value));
	if (err != 0 && err != DQLITE_EOM) {
		return err;
	}

	*value = dqlite__flip32(*((uint32_t *)buf));

	return err;
}

int dqlite__message_body_get_uint64(struct dqlite__message *m, uint64_t *value)
{
	int         err;
	const char *buf;

	assert(m != NULL);
	assert(value != NULL);

	err = dqlite__message_get(m, &buf, sizeof(*value));
	if (err != 0 && err != DQLITE_EOM) {
		return err;
	}

	*value = dqlite__flip64(*((uint64_t *)buf));

	return err;
}

int dqlite__message_body_get_int64(struct dqlite__message *m, int64_t *value)
{
	return dqlite__message_body_get_uint64(m, (uint64_t *)value);
}

int dqlite__message_body_get_double(struct dqlite__message *m, double_t *value)
{
	int         err;
	const char *buf;

	assert(m != NULL);
	assert(value != NULL);

	err = dqlite__message_get(m, &buf, sizeof(*value));
	if (err != 0 && err != DQLITE_EOM) {
		return err;
	}

	*((uint64_t *)value) = dqlite__flip64(*((uint64_t *)buf));

	return err;
}

void dqlite__message_header_put(struct dqlite__message *m,
                                uint8_t                 type,
                                uint8_t                 flags)
{
	assert(m != NULL);

	m->type  = type;
	m->flags = flags;
}

static int dqlite__message_body_put(struct dqlite__message *m,
                                    const char *            src,
                                    size_t                  len,
                                    size_t                  pad)
{
	size_t offset; /* Write offset */
	char * dst;    /* Write buffer to use */

	assert(m != NULL);
	assert(src != NULL);
	assert(len > 0);

	/* Check aligment. */
	if (!dqlite__message_body_is_offset_aligned(m, len + pad)) {
		dqlite__error_printf(&m->error, "misaligned write");
		return DQLITE_PROTO;
	}

	/* Check if we need to use the dynamic buffer. This happens if either:
	 *
	 * a) The dynamic buffer was previously allocated
	 * b) The size of the data to put would exceed the static buffer size
	 */
	if (m->body2.base != NULL ||                         /* a) */
	    m->offset1 + len + pad > DQLITE__MESSAGE_BUF_LEN /* b) */
	) {

		/* Check if we need to grow the dynamic buffer */
		if (m->offset2 + len + pad >= m->body2.len) {
			size_t cap;
			char * base;
			/* Overallocate a bit to avoid allocating again at the
			 * next write.
			 *
			 * TODO: this fails if we need more than 1024 additional
			 * bytes. */
			cap  = m->offset2 + len + pad + 1024;
			base = sqlite3_realloc(m->body2.base, cap);
			if (base == NULL) {
				dqlite__error_oom(
				    &m->error,
				    "failed to allocate dynamic body");
				return DQLITE_NOMEM;
			}
			m->body2.base = base;
			m->body2.len  = cap;
		}

		dst    = m->body2.base;
		offset = m->offset2;
	} else {
		dst    = m->body1;
		offset = m->offset1;
	}

	/* Write the data */
	memcpy(dst + offset, src, len);

	/* Add padding if needed */
	if (pad > 0) {
		memset(dst + offset + len, 0, pad);
	}

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

	len = strlen(text) + 1;

	/* Strings are padded so word-alignment is preserved for the next
	 * write. */
	pad = DQLITE__MESSAGE_WORD_SIZE - (len % DQLITE__MESSAGE_WORD_SIZE);
	if (pad == DQLITE__MESSAGE_WORD_SIZE) {
		pad = 0;
	}

	return dqlite__message_body_put(m, text, len, pad);
}

int dqlite__message_body_put_servers(struct dqlite__message *m,
                                     servers_t               servers)
{
	int    err;
	size_t i;

	assert(m != NULL);
	assert(servers != NULL);

	for (i = 0; servers[i].address != NULL; i++) {
		err = dqlite__message_body_put_uint64(m, servers[i].id);
		if (err != 0) {
			return err;
		}

		err = dqlite__message_body_put_text(m, servers[i].address);
		if (err != 0) {
			return err;
		}
	}

	return 0;
}

int dqlite__message_body_put_uint8(struct dqlite__message *m, uint8_t value)
{
	assert(m != NULL);

	return dqlite__message_body_put(
	    m, (const char *)(&value), sizeof(value), 0);
}

int dqlite__message_body_put_uint32(struct dqlite__message *m, uint32_t value)
{
	assert(m != NULL);

	value = dqlite__flip32(value);
	return dqlite__message_body_put(
	    m, (const char *)(&value), sizeof(value), 0);
}

int dqlite__message_body_put_uint64(struct dqlite__message *m, uint64_t value)
{
	assert(m != NULL);

	value = dqlite__flip64(value);
	return dqlite__message_body_put(
	    m, (const char *)(&value), sizeof(value), 0);
}

int dqlite__message_body_put_int64(struct dqlite__message *m, int64_t value)
{
	return dqlite__message_body_put_uint64(m, (uint64_t)value);
}

int dqlite__message_body_put_double(struct dqlite__message *m, double_t value)
{
	uint64_t buf;

	assert(m != NULL);

	/* An uint64 must start at word boundary */
	assert((m->offset1 % DQLITE__MESSAGE_WORD_SIZE) == 0);
	assert((m->offset2 % DQLITE__MESSAGE_WORD_SIZE) == 0);

	assert(sizeof(buf) == sizeof(value));

	memcpy(&buf, &value, sizeof(buf));

	buf = dqlite__flip64(buf);

	return dqlite__message_body_put(
	    m, (const char *)(&buf), sizeof(buf), 0);
}

void dqlite__message_send_start(struct dqlite__message *m, uv_buf_t bufs[3])
{
	assert(m != NULL);
	assert(bufs != NULL);

	/* The word count shouldn't have been written out yet */
	assert(m->words == 0);

	/* Something should have been written in the body */
	assert(m->offset1 > 0);

	/* The number of bytes written should be a multiple of the word size */
	assert((m->offset1 % DQLITE__MESSAGE_WORD_SIZE) == 0);
	assert((m->offset2 % DQLITE__MESSAGE_WORD_SIZE) == 0);

	m->words = dqlite__flip32((m->offset1 + m->offset2) /
	                          DQLITE__MESSAGE_WORD_SIZE);

	/* The message header is stored in the first part of the dqlite_message
	 * structure. */
	bufs[0].base = (char *)m;

	/* The length of the message header is fixed */
	bufs[0].len = DQLITE__MESSAGE_HEADER_LEN;

	bufs[1].base = m->body1;
	bufs[1].len  = m->offset1;

	bufs[2].base = m->body2.base;
	bufs[2].len  = m->offset2;

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

int dqlite__message_has_been_fully_consumed(struct dqlite__message *m)
{
	size_t offset;
	size_t words;

	assert(m != NULL);

	if (m->body2.base != NULL) {
		offset = m->offset2;
	} else {
		offset = m->offset1;
	}

	words = offset / DQLITE__MESSAGE_WORD_SIZE;

	return words == m->words;
}

int dqlite__message_is_large(struct dqlite__message *m)
{
	assert(m != NULL);

	return m->body2.base != NULL;
}
