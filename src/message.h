#ifndef DQLITE_MESSAGE_H
#define DQLITE_MESSAGE_H

#include <uv.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "error.h"
#include "lifecycle.h"

/**
 * The size of the message header, always 8 bytes.
 */
#define MESSAGE__HEADER_LEN 8

/**
 * The size in bytes of a single word in the message body.
 *
 * Since the 'words' field of dqlite__message is 32-bit, the maximum size of a
 * message body is about 34G.
 *
 * However this is currently limited to by MESSAGE__MAX_WORDS, mostly to
 * defend against misbehaving clients.
 */
#define MESSAGE__WORD_SIZE 8

/**
 * The size in bits of a single word in the message body
 */
#define MESSAGE__WORD_BITS 64

/**
 * Maximum number of words in a message. This is a reasonable figure for current
 * use cases of dqlite.
 *
 * TODO: make this configurable.
 */
#define MESSAGE__MAX_WORDS (1 << 25) /* ~250M */

/**
 * Length of the statically allocated message body buffer of struct message. If
 * a message body exeeds this size, a dynamically allocated buffer will be
 * used.
 */
#define MESSAGE__BUF_LEN 4096

/**
 * Number of words that the statically allocated message body buffer can
 * hold.
 */
#define MESSAGE__BUF_WORDS (MESSAGE__BUF_LEN / MESSAGE__WORD_SIZE)

/**
 * The maximum number of statement bindings or column rows is 255, which can fit
 * in one byte.
 */
#define MESSAGE__MAX_BINDINGS ((1 << 8) - 1)
#define MESSAGE__MAX_COLUMNS ((1 << 8) - 1)

/**
 * Type aliases to used by macro-based definitions in schema.h
 */
typedef const char *text_t;
typedef double double_t;
typedef dqlite_server_info *servers_t;

/* We rely on the size of double to be 64 bit, since that's what sent over the
 * wire. */
#ifdef static_assert
static_assert(sizeof(double) == sizeof(uint64_t),
	      "Size of 'double' is not 64 bits");
#endif

/**
 * A message serializes dqlite requests and responses.
 */
struct message
{
	/* public */
	uint32_t words; /* Number of 64-bit words in the body (little endian) */
	uint8_t type;   /* Code identifying the message type */
	uint8_t flags;  /* Type-specific flags */
	uint16_t extra; /* Extra space for type-specific data */

	/* read-only */
	dqlite__error error;

	/* private */
	union {
		/* Pre-allocated body buffer, enough for most cases */
		char body1[MESSAGE__BUF_LEN];
		uint64_t __body1__[MESSAGE__BUF_WORDS]; /* Alignment */
	};
	uv_buf_t body2; /* Dynamic buffer for bodies exceeding body1 */
	size_t offset1; /* Bytes that have been read or written to body1 */
	size_t offset2; /* Bytes that have been read or written to bdoy2 */
};

/**
 * Initialize the message.
 */
void message__init(struct message *m);

/**
 * Close the message, releasing any associated resources.
 */
void message__close(struct message *m);

/**
 * Called when starting to receive a message header.
 *
 * It returns a buffer large enough to hold the message header bytes. The buffer
 * must be progressivelly filled with the received data as it gets received from
 * the client, until it's full.
 */
void message__header_recv_start(struct message *m, uv_buf_t *buf);

/**
 * Called when the buffer returned by message__header_recv_start has been
 * completely filled by reads and the header is complete.
 *
 * Return an error if the header data is invalid.
 */
int message__header_recv_done(struct message *m);

/**
 * Called when starting to receive a message body, after the message header
 * buffer was filled and message__header_recv_done has been called.
 *
 * It returns a buffer large enough to hold the message body. The buffer must be
 * progressivelly filled as data gets received from the client, until it's full.
 *
 * Return an error if there is not enough memory to hold the message body.
 */
int message__body_recv_start(struct message *m, uv_buf_t *buf);

/**
 * APIs for decoding the message body.
 *
 * They must be called once the body has been completely received and they
 * return DQLITE_EOM when the end of the body is reached.
 */
int message__body_get_text(struct message *m, text_t *text);
int message__body_get_uint8(struct message *m, uint8_t *value);
int message__body_get_uint32(struct message *m, uint32_t *value);
int message__body_get_uint64(struct message *m, uint64_t *value);
int message__body_get_int64(struct message *m, int64_t *value);
int message__body_get_double(struct message *m, double_t *value);
int message__body_get_servers(struct message *m, servers_t *servers);

/**
 * Called after the message body has been completely decoded and it has been
 * processed. It resets the internal state so the object can be re-used for
 * receiving another message
 */
void message__recv_reset(struct message *m);

/**
 * Called when starting to render a message.
 *
 * It sets the message header with the given values.
 */
void message__header_put(struct message *m, uint8_t type, uint8_t flags);

/**
 * APIs for encoding the message body.
 */
int message__body_put_text(struct message *m, text_t text);
int message__body_put_uint8(struct message *m, uint8_t value);
int message__body_put_uint32(struct message *m, uint32_t value);
int message__body_put_int64(struct message *m, int64_t value);
int message__body_put_uint64(struct message *m, uint64_t value);
int message__body_put_double(struct message *m, double_t value);
int message__body_put_servers(struct message *m, servers_t servers);

/**
 * Called when starting to send a message.
 *
 * It returns three buffers: the message header buffer, the statically allocated
 * message body buffer, and optionally a dynamically allocated body buffer (if
 * the body size exeeds the size of the statically allocated body buffer).
 */
void message__send_start(struct message *m, uv_buf_t bufs[3]);

/**
 * Called after the body has been completely sent to the client.
 *
 * It resets the internal state so the object can be re-used for sending another
 * message.
 */
void message__send_reset(struct message *m);

/**
 Return true if the message has been completely read.
 */
int message__has_been_fully_consumed(struct message *m);

/**
 * Return true if the message is exceeding the static buffer size.
 */
int message__is_large(struct message *m);

#endif /* DQLITE_MESSAGE_H */
