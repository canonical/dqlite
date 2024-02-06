/* Wrappers for system calls not yet defined in libc. */

#ifndef SYSCALL_H_
#define SYSCALL_H_

#if HAVE_LINUX_AIO_ABI_H
#include <linux/aio_abi.h>
#include <signal.h>
#include <time.h>
#endif

#if HAVE_LINUX_IO_URING_H
#include <linux/io_uring.h>
#endif

#if HAVE_LINUX_AIO_ABI_H
/* AIO */
int io_setup(unsigned nr_events, aio_context_t *ctx_idp);

int io_destroy(aio_context_t ctx_id);

int io_submit(aio_context_t ctx_id, long nr, struct iocb **iocbpp);

int io_getevents(aio_context_t ctx_id,
                 long min_nr,
                 long nr,
                 struct io_event *events,
                 struct timespec *timeout);
#endif

#if HAVE_LINUX_IO_URING_H
/* uring */
int io_uring_register(int fd,
                      unsigned int opcode,
                      const void *arg,
                      unsigned int nr_args);

int io_uring_setup(unsigned int entries, struct io_uring_params *p);

int io_uring_enter(int fd,
                   unsigned int to_submit,
                   unsigned int min_complete,
                   unsigned int flags,
                   sigset_t *sig);
#endif

#endif /* SYSCALL_ */
