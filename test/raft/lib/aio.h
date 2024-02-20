/* Utilities around the Kernel AIO sub-system. */
#ifndef TEST_AIO_H
#define TEST_AIO_H

#include <linux/aio_abi.h>

/* Fill the AIO subsystem resources by allocating a lot of events to the given
 * context, and leaving only @n events available for subsequent calls to
 * @io_setup.
 *
 * Return -1 if it looks like there is another process already using the AIO
 * subsystem, which would most probably make the calling test flaky because
 * there won't be exactly @n events available anymore. */
int AioFill(aio_context_t *ctx, unsigned n);

/* Destroy the given AIO context. */
void AioDestroy(aio_context_t ctx);

#endif /* TEST_AIO_H */
