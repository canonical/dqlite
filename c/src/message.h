#ifndef DQLITE_MESSAGE_H
#define DQLITE_MESSAGE_H

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <endian.h>

#include <uv.h>

#include "dqlite.h"
#include "error.h"
#include "lifecycle.h"

/* The size of the message header, always 8 bytes. */
#define DQLITE__MESSAGE_HEADER_LEN 8

/* The size in bytes of a single word in the message body.
 *
 * Since the 'words' field of dqlite__message is 32-bit, the maximum size of a
 * message body is about 34G.
 */
#define DQLITE__MESSAGE_WORD_SIZE 8

/* The size in bits of a single word in the message body */
#define DQLITE__MESSAGE_WORD_BITS 64

/* Length of the statically allocated message body buffer of dqlite__message. If
 * a message body exeeds this size, a dynamically allocated buffer will be
 * used. */
#define DQLITE__MESSAGE_BUF_LEN   4096
#define DQLITE__MESSAGE_BUF_WORDS DQLITE__MESSAGE_BUF_LEN / DQLITE__MESSAGE_WORD_SIZE

/* Type aliases to used by macro-based definitions in schema.h */
typedef const char*  text_t;
typedef const char** text_list_t;
typedef double double_t;

/*
 * Common header for requests and responses.
 */
struct dqlite__message {
	/* public */
	uint32_t words;     /* Number of 64-bit words in the body (little endian) */
	uint8_t  type;      /* Code identifying the message type */
	uint8_t  flags;     /* Type-specific flags */
	uint16_t extra;     /* Extra space for type-specific data */

	/* read-only */
	dqlite__error error;

	/* private */
	union {
		 /* Pre-allocated body buffer, enough for most cases */
		char       body1  [DQLITE__MESSAGE_BUF_LEN];
		uint64_t __body1__[DQLITE__MESSAGE_BUF_WORDS]; /* Alignment */
	};
	uv_buf_t body2;   /* Dynamically allocated buffer for bodies exeeding body1 */
	size_t   offset1; /* Number of bytes that have been read or written to body1 */
	size_t   offset2; /* Number of bytes that have been read or written to bdoy2 */
};

void dqlite__message_init(struct dqlite__message *m);
void dqlite__message_close(struct dqlite__message *m);

/* Called when starting to receive a message header.
 *
 * It returns a buffer large enough to hold the message header bytes. The buffer
 * must be progressivelly filled with the received data as it gets read from the
 * socket, until it's full.
 */
void dqlite__message_header_recv_start(struct dqlite__message *m, uv_buf_t *buf);

/* Called when the buffer returned by dqlite__message_header_recv_start has been
 * completely filled by reads and the header is complete.
 *
 * Return an error if the header data is invalid.
 */
int dqlite__message_header_recv_done(struct dqlite__message *m);

/* Called when starting to receive a message body, after the message header
 * buffer was filled and dqlite__message_header_recv_done has been called.
 *
 * It returns a buffer large enough to hold the message body. The buffer must be
 * progressivelly filled with the received data as it gets read from the socket,
 * until it's full.
 *
 * Return an error if there is not enough memory to hold the message body.
 */
int dqlite__message_body_recv_start(struct dqlite__message *m, uv_buf_t *buf);

/*
 * APIs for decoding the message body.
 */
int dqlite__message_body_get_text(struct dqlite__message *m, text_t *text);
int dqlite__message_body_get_text_list(struct dqlite__message *m, text_list_t *list);
int dqlite__message_body_get_uint8(struct dqlite__message *m, uint8_t *value);
int dqlite__message_body_get_uint32(struct dqlite__message *m, uint32_t *value);
int dqlite__message_body_get_int64(struct dqlite__message *m, int64_t *value);
int dqlite__message_body_get_uint64(struct dqlite__message *m, uint64_t *value);
int dqlite__message_body_get_double(struct dqlite__message *m, double_t *value);

/* Called after the message body has been read completely and it has been
 * processed. It resets the internal state so the object can be re-used for
 * receiving another message */
void dqlite__message_recv_reset(struct dqlite__message *m);

/* Called when starting to render a message.
 *
 * It sets the message header with the given values.
 */
void dqlite__message_header_put(struct dqlite__message *m, uint8_t type, uint8_t flags);

/*
 * APIs for encoding the message body.
 */
int dqlite__message_body_put_text(struct dqlite__message *m, text_t text);
int dqlite__message_body_put_text_list(struct dqlite__message *m, text_list_t list);
int dqlite__message_body_put_uint8(struct dqlite__message *m, uint8_t value);
int dqlite__message_body_put_uint32(struct dqlite__message *m, uint32_t value);
int dqlite__message_body_put_int64(struct dqlite__message *m, int64_t value);
int dqlite__message_body_put_uint64(struct dqlite__message *m, uint64_t value);
int dqlite__message_body_put_double(struct dqlite__message *m, double_t value);

/* Called when starting to send a message.
 *
 * It returns three buffers: the message header buffer, the statically allocated
 * message body buffer, and optionally a dynamically allocated body buffer (if
 * the body size exeeds the size of the statically allocated body buffer).
 */
void dqlite__message_send_start(struct dqlite__message *m, uv_buf_t bufs[3]);

/* Called after the body has been completely written to the socket.
 *
 * It resets the internal state so the object can be re-used for sending another
 * message.
 */
void dqlite__message_send_reset(struct dqlite__message *m);

#endif /* DQLITE_MESSAGE_H */
