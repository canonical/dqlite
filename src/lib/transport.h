/**
 * Asynchronously read and write buffer from and to the network.
 */

#ifndef LIB_TRANSPORT_H_
#define LIB_TRANSPORT_H_

#include <uv.h>

#define TRANSPORT__BADSOCKET 1000

/**
 * Callbacks.
 */
struct transport;
typedef void (*transport_read_cb)(struct transport *t, int status);
typedef void (*transport_write_cb)(struct transport *t, int status);
typedef void (*transport_close_cb)(struct transport *t);

/**
 * Light wrapper around a libuv stream handle, providing a more convenient way
 * to read a certain amount of bytes.
 */
struct transport
{
	void *data;                  /* User defined */
	struct uv_stream_s *stream;  /* Data stream */
	uv_buf_t read;               /* Read buffer */
	uv_write_t write;            /* Write request */
	transport_read_cb read_cb;   /* Read callback */
	transport_write_cb write_cb; /* Write callback */
	transport_close_cb close_cb; /* Close callback */
};

/**
 * Initialize a transport of the appropriate type (TCP or PIPE) attached to the
 * given file descriptor.
 */
int transport__init(struct transport *t, struct uv_stream_s *stream);

/**
 * Start closing by the transport.
 */
void transport__close(struct transport *t, transport_close_cb cb);

/**
 * Read from the transport file descriptor until the given buffer is full.
 */
int transport__read(struct transport *t, uv_buf_t *buf, transport_read_cb cb);

/**
 * Write the given buffer to the transport.
 */
int transport__write(struct transport *t, uv_buf_t *buf, transport_write_cb cb);

/* Create an UV stream object from the given fd. */
int transport__stream(struct uv_loop_s *loop,
		      int fd,
		      struct uv_stream_s **stream);

#endif /* LIB_TRANSPORT_H_ */
