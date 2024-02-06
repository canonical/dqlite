#include "syscall.h"

#if HAVE_LINUX_AIO_ABI_H || HAVE_LINUX_IO_URING_H
#include <sys/syscall.h>
#include <unistd.h>
#endif

#if HAVE_LINUX_AIO_ABI_H
int io_setup(unsigned nr_events, aio_context_t *ctx_idp)
{
    return (int)syscall(__NR_io_setup, nr_events, ctx_idp);
}

int io_destroy(aio_context_t ctx_id)
{
    return (int)syscall(__NR_io_destroy, ctx_id);
}

int io_submit(aio_context_t ctx_id, long nr, struct iocb **iocbpp)
{
    return (int)syscall(__NR_io_submit, ctx_id, nr, iocbpp);
}

int io_getevents(aio_context_t ctx_id,
                 long min_nr,
                 long nr,
                 struct io_event *events,
                 struct timespec *timeout)
{
    return (int)syscall(__NR_io_getevents, ctx_id, min_nr, nr, events, timeout);
}
#endif

#if HAVE_LINUX_IO_URING_H
int io_uring_register(int fd,
                      unsigned int opcode,
                      const void *arg,
                      unsigned int nr_args)
{
    return (int)syscall(__NR_io_uring_register, fd, opcode, arg, nr_args);
}

int io_uring_setup(unsigned int entries, struct io_uring_params *p)
{
    return (int)syscall(__NR_io_uring_setup, entries, p);
}

int io_uring_enter(int fd,
                   unsigned int to_submit,
                   unsigned int min_complete,
                   unsigned int flags,
                   sigset_t *sig)
{
    return (int)syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags,
                        sig, _NSIG / 8);
}
#endif
