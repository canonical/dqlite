#ifndef DQLITE_MESSAGE_H
#define DQLITE_MESSAGE_H

#include <assert.h>
#include <sys/types.h>
#include <stddef.h>
#include <stdint.h>

#include <uv.h>

#include "../include/dqlite.h"

#include "error.h"
#include "lifecycle.h"

/* The size of the message header, always 8 bytes. */
#define DQLITE__MESSAGE_HEADER_LEN 8

/* The size in bytes of a single word in the message body.
 *
 * Since the 'words' field of dqlite__message is 32-bit, the maximum size of a
 * message body is about 34G.
 *
 * However this is currently limited to by DQLITE__MESSAGE_MAX_WORDS, mostly to
 * defend against misbehaving clients.
 */
#define DQLITE__MESSAGE_WORD_SIZE 8

/* The size in bits of a single word in the message body */
#define DQLITE__MESSAGE_WORD_BITS 64

/* Maximum number of words in a message. This is a reasonable figure for current
 * use cases of dqlite.
 *
 * TODO: make this configurable.
 */
#define DQLITE__MESSAGE_MAX_WORDS (1 << 25) /* ~250M */

/* Length of the statically allocated message body buffer of dqlite__message. If
 * a message body exeeds this size, a dynamically allocated buffer will be
 * used. */
#define DQLITE__MESSAGE_BUF_LEN 4096

/* Number of words that the statically allocated message body buffer can
 * hold. */
#define DQLITE__MESSAGE_BUF_WORDS                                              \
	(DQLITE__MESSAGE_BUF_LEN / DQLITE__MESSAGE_WORD_SIZE)

/* The maximum number of statement bindings or column rows is 255, which can fit
 * in one byte. */
#define DQLITE__MESSAGE_MAX_BINDINGS ((1 << 8) - 1)
#define DQLITE__MESSAGE_MAX_COLUMNS ((1 << 8) - 1)

/* Type aliases to used by macro-based definitions in schema.h */
typedef const char *        text_t;
typedef double              double_t;
typedef dqlite_server_info *servers_t;

/* We rely on the size of double to be 64 bit, since that's what sent over the
 * wire. */
#ifdef static_assert
static_assert(sizeof(double) == sizeof(uint64_t),
              "Size of 'double' is not 64 bits");
#endif

/* A message serializes dqlite requests and responses. */
struct dqlite__message {
	/* public */
	uint32_t words; /* Number of 64-bit words in the body (little endian) */
	uint8_t  type;  /* Code identifying the message type */
	uint8_t  flags; /* Type-specific flags */
	uint16_t extra; /* Extra space for type-specific data */

	/* read-only */
	dqlite__error error;

	/* private */
	union {
		/* Pre-allocated body buffer, enough for most cases */
		char     body1[DQLITE__MESSAGE_BUF_LEN];
		uint64_t __body1__[DQLITE__MESSAGE_BUF_WORDS]; /* Alignment */
	};
	uv_buf_t body2;   /* Dynamic buffer for bodies exceeding body1 */
	size_t   offset1; /* Bytes that have been read or written to body1 */
	size_t   offset2; /* Bytes that have been read or written to bdoy2 */
};

/* Initialize the message. */
void dqlite__message_init(struct dqlite__message *m);

/* Close the message, releasing any associated resources. */
void dqlite__message_close(struct dqlite__message *m);

/* Called when starting to receive a message header.
 *
 * It returns a buffer large enough to hold the message header bytes. The buffer
 * must be progressivelly filled with the received data as it gets received from
 * the client, until it's full. */
void dqlite__message_header_recv_start(struct dqlite__message *m,
                                       uv_buf_t *              buf);

/* Called when the buffer returned by dqlite__message_header_recv_start has been
 * completely filled by reads and the header is complete.
 *
 * Return an error if the header data is invalid. */
int dqlite__message_header_recv_done(struct dqlite__message *m);

/* Called when starting to receive a message body, after the message header
 * buffer was filled and dqlite__message_header_recv_done has been called.
 *
 * It returns a buffer large enough to hold the message body. The buffer must be
 * progressivelly filled as data gets received from the client, until it's full.
 *
 * Return an error if there is not enough memory to hold the message body. */
int dqlite__message_body_recv_start(struct dqlite__message *m, uv_buf_t *buf);

/* APIs for decoding the message body.
 *
 * They must be called once the body has been completely received and they
 * return DQLITE_EOM when the end of the body is reached. */
int dqlite__message_body_get_text(struct dqlite__message *m, text_t *text);
int dqlite__message_body_get_uint8(struct dqlite__message *m, uint8_t *value);
int dqlite__message_body_get_uint32(struct dqlite__message *m, uint32_t *value);
int dqlite__message_body_get_uint64(struct dqlite__message *m, uint64_t *value);
int dqlite__message_body_get_int64(struct dqlite__message *m, int64_t *value);
int dqlite__message_body_get_double(struct dqlite__message *m, double_t *value);
int dqlite__message_body_get_servers(struct dqlite__message *m,
                                     servers_t *             servers);

/* Called after the message body has been completely decoded and it has been
 * processed. It resets the internal state so the object can be re-used for
 * receiving another message */
void dqlite__message_recv_reset(struct dqlite__message *m);

/* Called when starting to render a message.
 *
 * It sets the message header with the given values. */
void dqlite__message_header_put(struct dqlite__message *m,
                                uint8_t                 type,
                                uint8_t                 flags);

/* APIs for encoding the message body. */
int dqlite__message_body_put_text(struct dqlite__message *m, text_t text);
int dqlite__message_body_put_uint8(struct dqlite__message *m, uint8_t value);
int dqlite__message_body_put_uint32(struct dqlite__message *m, uint32_t value);
int dqlite__message_body_put_int64(struct dqlite__message *m, int64_t value);
int dqlite__message_body_put_uint64(struct dqlite__message *m, uint64_t value);
int dqlite__message_body_put_double(struct dqlite__message *m, double_t value);
int dqlite__message_body_put_servers(struct dqlite__message *m,
                                     servers_t               servers);

/* Called when starting to send a message.
 *
 * It returns three buffers: the message header buffer, the statically allocated
 * message body buffer, and optionally a dynamically allocated body buffer (if
 * the body size exeeds the size of the statically allocated body buffer). */
void dqlite__message_send_start(struct dqlite__message *m, uv_buf_t bufs[3]);

/* Called after the body has been completely sent to the client.
 *
 * It resets the internal state so the object can be re-used for sending another
 * message. */
void dqlite__message_send_reset(struct dqlite__message *m);

/* Return true if the message has been completely read. */
int dqlite__message_has_been_fully_consumed(struct dqlite__message *m);

/* Return true if the message is exceeding the static buffer size. */
int dqlite__message_is_large(struct dqlite__message *m);

#endif /* DQLITE_MESSAGE_H */
