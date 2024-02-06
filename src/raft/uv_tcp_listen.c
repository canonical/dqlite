#include <string.h>

#include "assert.h"
#include "byte.h"
#include "heap.h"
#include "uv_ip.h"
#include "uv_tcp.h"

/* The happy path of an incoming connection is:
 *
 * - The connection callback is fired on the listener TCP handle, and the
 *   incoming connection is uv_accept()'ed. We call uv_read_start() to get
 *   notified about received handshake data.
 *
 * - Once the preamble is received, we start waiting for the server address.
 *
 * - Once the server address is received, we fire the receive callback.
 *
 * Possible failure modes are:
 *
 * - The accept process gets canceled in the transport->close() implementation,
 *   by calling tcp_accept_stop(): the incoming TCP connection handle gets
 *   closed, preventing any further handshake data notification, and all
 *   allocated memory gets released in the handle close callback.
 */

/* Hold state for a connection being accepted. */
struct uvTcpHandshake
{
    uint64_t preamble[3]; /* Preamble buffer */
    uv_buf_t address;     /* Address buffer */
    size_t nread;         /* Number of bytes read */
};

/* Hold handshake data for a new connection being established. */
struct uvTcpIncoming
{
    struct UvTcp *t;           /* Transport implementation */
    struct uv_tcp_s *listener; /* The tcp handle, which accepted this socket */
    struct uv_tcp_s *tcp;      /* TCP connection socket handle */
    struct uvTcpHandshake handshake; /* Handshake data */
    queue queue;                     /* Pending accept queue */
};

/* Decode the handshake preamble, containing the protocol version, the ID of the
 * connecting server and the length of its address. Also, allocate the buffer to
 * start reading the server address. */
static int uvTcpDecodePreamble(struct uvTcpHandshake *h)
{
    uint64_t protocol;
    protocol = byteFlip64(h->preamble[0]);
    if (protocol != UV__TCP_HANDSHAKE_PROTOCOL) {
        return RAFT_MALFORMED;
    }
    h->address.len = (size_t)byteFlip64(h->preamble[2]);
    h->address.base = RaftHeapMalloc(h->address.len);
    if (h->address.base == NULL) {
        return RAFT_NOMEM;
    }
    h->nread = 0;
    return 0;
}

/* The accepted TCP client connection has been closed, release all memory
 * associated with accept object. We can get here only if an error occurrent
 * during the handshake or if raft_uv_transport->close() has been invoked. */
static void uvTcpIncomingCloseCb(struct uv_handle_s *handle)
{
    struct uvTcpIncoming *incoming = handle->data;
    struct UvTcp *t = incoming->t;
    QUEUE_REMOVE(&incoming->queue);
    if (incoming->handshake.address.base != NULL) {
        RaftHeapFree(incoming->handshake.address.base);
    }
    RaftHeapFree(incoming->tcp);
    RaftHeapFree(incoming);
    UvTcpMaybeFireCloseCb(t);
}

/* Close an incoming TCP connection which hasn't complete the handshake yet. */
static void uvTcpIncomingAbort(struct uvTcpIncoming *incoming)
{
    struct UvTcp *t = incoming->t;
    /* After uv_close() returns we are guaranteed that no more alloc_cb or
     * read_cb will be called. */
    QUEUE_REMOVE(&incoming->queue);
    QUEUE_PUSH(&t->aborting, &incoming->queue);
    uv_close((struct uv_handle_s *)incoming->tcp, uvTcpIncomingCloseCb);
}

/* Read the address part of the handshake. */
static void uvTcpIncomingAllocCbAddress(struct uv_handle_s *handle,
                                        size_t suggested_size,
                                        uv_buf_t *buf)
{
    struct uvTcpIncoming *incoming = handle->data;
    (void)suggested_size;
    assert(!incoming->t->closing);
    buf->base = incoming->handshake.address.base + incoming->handshake.nread;
    buf->len = incoming->handshake.address.len - incoming->handshake.nread;
}

static void uvTcpIncomingReadCbAddress(uv_stream_t *stream,
                                       ssize_t nread,
                                       const uv_buf_t *buf)
{
    struct uvTcpIncoming *incoming = stream->data;
    char *address;
    raft_id id;
    size_t n;
    int rv;

    (void)buf;
    assert(!incoming->t->closing);

    if (nread == 0) {
        /* Empty read just ignore it. */
        return;
    }
    if (nread < 0) {
        uvTcpIncomingAbort(incoming);
        return;
    }

    /* We shouldn't have read more data than the pending amount. */
    n = (size_t)nread;
    assert(n <= incoming->handshake.address.len - incoming->handshake.nread);

    /* Advance the read window */
    incoming->handshake.nread += n;

    /* If there's more data to read in order to fill the current
     * read buffer, just return, we'll be invoked again. */
    if (incoming->handshake.nread < incoming->handshake.address.len) {
        return;
    }

    /* If we have completed reading the address, let's fire the callback. */
    rv = uv_read_stop(stream);
    assert(rv == 0);
    id = byteFlip64(incoming->handshake.preamble[1]);
    address = incoming->handshake.address.base;
    QUEUE_REMOVE(&incoming->queue);
    incoming->t->accept_cb(incoming->t->transport, id, address,
                           (struct uv_stream_s *)incoming->tcp);
    RaftHeapFree(incoming->handshake.address.base);
    RaftHeapFree(incoming);
}

/* Read the preamble of the handshake. */
static void uvTcpIncomingAllocCbPreamble(struct uv_handle_s *handle,
                                         size_t suggested_size,
                                         uv_buf_t *buf)
{
    struct uvTcpIncoming *incoming = handle->data;
    (void)suggested_size;
    buf->base =
        (char *)incoming->handshake.preamble + incoming->handshake.nread;
    buf->len = sizeof incoming->handshake.preamble - incoming->handshake.nread;
}

static void uvTcpIncomingReadCbPreamble(uv_stream_t *stream,
                                        ssize_t nread,
                                        const uv_buf_t *buf)
{
    struct uvTcpIncoming *incoming = stream->data;
    size_t n;
    int rv;

    (void)buf;

    if (nread == 0) {
        /* Empty read just ignore it. */
        return;
    }
    if (nread < 0) {
        uvTcpIncomingAbort(incoming);
        return;
    }

    /* We shouldn't have read more data than the pending amount. */
    n = (size_t)nread;
    assert(n <=
           sizeof incoming->handshake.preamble - incoming->handshake.nread);

    /* Advance the read window */
    incoming->handshake.nread += n;

    /* If there's more data to read in order to fill the current
     * read buffer, just return, we'll be invoked again. */
    if (incoming->handshake.nread < sizeof incoming->handshake.preamble) {
        return;
    }

    /* If we have completed reading the preamble, let's parse it. */
    rv = uvTcpDecodePreamble(&incoming->handshake);
    if (rv != 0) {
        uvTcpIncomingAbort(incoming);
        return;
    }

    rv = uv_read_stop(stream);
    assert(rv == 0);
    rv = uv_read_start((uv_stream_t *)incoming->tcp,
                       uvTcpIncomingAllocCbAddress, uvTcpIncomingReadCbAddress);
    assert(rv == 0);
}

/* Start reading handshake data for a new incoming connection. */
static int uvTcpIncomingStart(struct uvTcpIncoming *incoming)
{
    int rv;

    memset(&incoming->handshake, 0, sizeof incoming->handshake);

    incoming->tcp = RaftHeapMalloc(sizeof *incoming->tcp);
    if (incoming->tcp == NULL) {
        return RAFT_NOMEM;
    }
    incoming->tcp->data = incoming;

    rv = uv_tcp_init(incoming->t->loop, incoming->tcp);
    assert(rv == 0);

    rv = uv_accept((struct uv_stream_s *)incoming->listener,
                   (struct uv_stream_s *)incoming->tcp);
    if (rv != 0) {
        rv = RAFT_IOERR;
        goto err_after_tcp_init;
    }
    rv = uv_read_start((uv_stream_t *)incoming->tcp,
                       uvTcpIncomingAllocCbPreamble,
                       uvTcpIncomingReadCbPreamble);
    assert(rv == 0);

    return 0;

err_after_tcp_init:
    uv_close((uv_handle_t *)incoming->tcp, (uv_close_cb)RaftHeapFree);
    return rv;
}

#define IS_IN_ARRAY(elem, array, array_size)         \
    (const char *)(elem) >= (const char *)(array) && \
        (const char *)(elem) <                       \
            (const char *)(array) + array_size * sizeof(*array)

/* Called when there's a new incoming connection: create a new tcp_accept object
 * and start receiving handshake data. */
static void uvTcpListenCb(struct uv_stream_s *stream, int status)
{
    struct UvTcp *t = stream->data;
    struct uvTcpIncoming *incoming;
    int rv;

    assert(IS_IN_ARRAY(stream, t->listeners, t->n_listeners));

    if (status != 0) {
        rv = RAFT_IOERR;
        goto err;
    }

    incoming = RaftHeapMalloc(sizeof *incoming);
    if (incoming == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    incoming->t = t;
    incoming->listener = (struct uv_tcp_s *)stream;
    incoming->tcp = NULL;

    QUEUE_PUSH(&t->accepting, &incoming->queue);

    rv = uvTcpIncomingStart(incoming);
    if (rv != 0) {
        goto err_after_accept_alloc;
    }

    return;

err_after_accept_alloc:
    QUEUE_REMOVE(&incoming->queue);
    RaftHeapFree(incoming);
err:
    assert(rv != 0);
}

/* Do bind/listen call on the tcp handle */
static int uvTcpBindListen(struct uv_tcp_s *listener, struct sockaddr *addr)
{
    if (uv_tcp_bind(listener, addr, 0) ||
        uv_listen((uv_stream_t *)listener, 1, uvTcpListenCb)) {
        return RAFT_IOERR;
    }
    return 0;
}

/* Create a tcp handle and do bind/listen for each IP */
static int uvTcpListenOnMultipleIP(struct raft_uv_transport *transport,
                                   struct addrinfo *addr_infos)
{
    struct UvTcp *t;
    struct addrinfo *current;
    unsigned n_listeners;
    int rv;

    t = transport->impl;

    n_listeners = 0;
    for (current = addr_infos; current; current = current->ai_next) {
        ++n_listeners;
    }

    current = addr_infos;
    t->listeners = raft_malloc(n_listeners * sizeof(*t->listeners));
    if (!t->listeners) {
        rv = RAFT_NOMEM;
        goto err;
    }

    t->n_listeners = n_listeners;
    for (n_listeners = 0; n_listeners < t->n_listeners; ++n_listeners) {
        struct uv_tcp_s *listener = &t->listeners[n_listeners];
        listener->data = t;
        if (uv_tcp_init(t->loop, listener) ||
            uvTcpBindListen(listener, current->ai_addr)) {
            rv = RAFT_IOERR;
            goto err;
        }
        current = addr_infos->ai_next;
    }
    return 0;

err:
    if (t->listeners) {
        for (unsigned i = 0; i <= n_listeners; ++i) {
            uv_close((struct uv_handle_s *)&t->listeners[i], NULL);
        }
        raft_free(t->listeners);
        t->listeners = NULL;
        t->n_listeners = 0;
    }
    return rv;
}

/* Ignore duplicate entries from glibc getaddrinfo due to
 * https://bugzilla.redhat.com/show_bug.cgi?id=496300
 * in case of resolving localhost */
static bool uvIsAddressDuplication(struct addrinfo *addr_info)
{
    struct addrinfo *next = addr_info->ai_next;

    /* Check, if we have a list of length 2 */
    if (!next || next->ai_next) {
        return false;
    }
    if (addr_info->ai_addrlen != next->ai_addrlen ||
        bcmp(addr_info->ai_addr, next->ai_addr, addr_info->ai_addrlen)) {
        return false;
    }
    return true;
}

int UvTcpListen(struct raft_uv_transport *transport, raft_uv_accept_cb cb)
{
    struct UvTcp *t;
    struct addrinfo *addr_infos;
    int rv;

    t = transport->impl;
    t->accept_cb = cb;

    if (t->bind_address == NULL) {
        rv = uvIpResolveBindAddresses(t->address, &addr_infos);
    } else {
        rv = uvIpResolveBindAddresses(t->bind_address, &addr_infos);
    }
    if (rv != 0 || !addr_infos) {
        return rv;
    }
    if (addr_infos->ai_next && uvIsAddressDuplication(addr_infos)) {
        rv = uvTcpListenOnMultipleIP(transport, addr_infos->ai_next);
    } else {
        rv = uvTcpListenOnMultipleIP(transport, addr_infos);
    }
    freeaddrinfo(addr_infos);
    return rv;
}

/* Close callback for uvTcp->listener. */
static void uvTcpListenCloseCbListener(struct uv_handle_s *handle)
{
    struct UvTcp *t = handle->data;
    assert(t->closing);
    assert(t->n_listeners);
    assert(t->listeners);
    if (--t->n_listeners == 0) {
        raft_free(t->listeners);
        t->listeners = NULL;
        UvTcpMaybeFireCloseCb(t);
    }
}

void UvTcpListenClose(struct UvTcp *t)
{
    queue *head;
    assert(t->closing);

    while (!QUEUE_IS_EMPTY(&t->accepting)) {
        struct uvTcpIncoming *incoming;
        head = QUEUE_HEAD(&t->accepting);
        incoming = QUEUE_DATA(head, struct uvTcpIncoming, queue);
        uvTcpIncomingAbort(incoming);
    }

    if (t->n_listeners) {
        for (unsigned i = 0; i < t->n_listeners; ++i) {
            uv_close((struct uv_handle_s *)&t->listeners[i],
                     uvTcpListenCloseCbListener);
        }
    }
}
