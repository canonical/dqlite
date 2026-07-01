#include "tcp.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <unistd.h>
#endif
#include <errno.h>
#include <stdio.h>

static void tcpCloseSocket(int socket)
{
#ifdef _WIN32
    closesocket((SOCKET)socket);
#else
    close(socket);
#endif
}

static const char *tcpSocketError(void)
{
#ifdef _WIN32
    static char message[64];
    snprintf(message, sizeof message, "WSA error %d", WSAGetLastError());
    return message;
#else
    return strerror(errno);
#endif
}

static int tcpAccept(int socket, struct sockaddr *addr, socklen_t *size)
{
#ifdef _WIN32
    return (int)accept((SOCKET)socket, addr, size);
#else
    return accept4(socket, addr, size, SOCK_CLOEXEC);
#endif
}

static int tcpSend(int socket, const void *buf, int len)
{
#ifdef _WIN32
    return send((SOCKET)socket, buf, len, 0);
#else
    return (int)write(socket, buf, (size_t)len);
#endif
}

void TcpServerInit(struct TcpServer *s)
{
    struct sockaddr_in addr;
    socklen_t size = sizeof addr;
    int rv;

    /* Initialize the socket address structure. */
    memset(&addr, 0, size);

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = 0; /* Get a random free port */

    /* Create the server socket. */
    s->socket = socket(AF_INET, SOCK_STREAM, 0);
    if (s->socket == -1) {
        munit_errorf("tcp server: socket(): %s", tcpSocketError());
    }

    /* Bind the socket. */
    rv = bind(s->socket, (struct sockaddr *)&addr, size);
    if (rv == -1) {
        munit_errorf("tcp server: bind(): %s", tcpSocketError());
    }

    /* Start listening. */
    rv = listen(s->socket, 1);
    if (rv == -1) {
        munit_errorf("tcp server: listen(): %s", tcpSocketError());
    }

    /* Get the actual addressed assigned by the kernel and save it back in the
     * relevant field. */
    rv = getsockname(s->socket, (struct sockaddr *)&addr, &size);
    if (rv != 0) {
        munit_errorf("tcp: getsockname(): %s", tcpSocketError());
    }

    s->port = htons(addr.sin_port);
    sprintf(s->address, "127.0.0.1:%d", s->port);
}

void TcpServerClose(struct TcpServer *s)
{
    if (s->socket == -1) {
        return;
    }

    tcpCloseSocket(s->socket);
}

int TcpServerAccept(struct TcpServer *s)
{
    int socket;
    struct sockaddr_in address;
    socklen_t size;

    size = sizeof(address);

    socket = tcpAccept(s->socket, (struct sockaddr *)&address, &size);
    if (socket < 0) {
        munit_errorf("tcp server: accept(): %s", tcpSocketError());
    }

    return socket;
}

void TcpServerCloseAccepted(int socket)
{
    tcpCloseSocket(socket);
}

void TcpServerStop(struct TcpServer *s)
{
    tcpCloseSocket(s->socket);
    s->socket = -1;
}

void test_tcp_setup(const MunitParameter params[], struct test_tcp *t)
{
    (void)params;
    t->server.socket = -1;
    t->client.socket = -1;
}

void test_tcp_tear_down(struct test_tcp *t)
{
    if (t->server.socket != -1) {
        tcpCloseSocket(t->server.socket);
    }

    if (t->client.socket != -1) {
        tcpCloseSocket(t->client.socket);
    }
}

void test_tcp_listen(struct test_tcp *t)
{
    struct sockaddr_in addr;
    socklen_t size = sizeof addr;
    int rv;

    /* Initialize the socket address structure. */
    memset(&addr, 0, size);

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = 0; /* Get a random free port */

    /* Create the server socket. */
    t->server.socket = socket(AF_INET, SOCK_STREAM, 0);
    if (t->server.socket == -1) {
        munit_errorf("tcp: socket(): %s", tcpSocketError());
    }

    /* Bind the socket. */
    rv = bind(t->server.socket, (struct sockaddr *)&addr, size);
    if (rv == -1) {
        munit_errorf("tcp: bind(): %s", tcpSocketError());
    }

    /* Start listening. */
    rv = listen(t->server.socket, 1);
    if (rv == -1) {
        munit_errorf("tcp: listen(): %s", tcpSocketError());
    }

    /* Get the actual addressed assigned by the kernel and save it back in
     * the relevant test_socket__server field (pointed to by address). */
    rv = getsockname(t->server.socket, (struct sockaddr *)&addr, &size);
    if (rv != 0) {
        munit_errorf("tcp: getsockname(): %s", tcpSocketError());
    }

    sprintf(t->server.address, "127.0.0.1:%d", htons(addr.sin_port));
}

const char *test_tcp_address(struct test_tcp *t)
{
    return t->server.address;
}

void test_tcp_connect(struct test_tcp *t, int port)
{
    struct sockaddr_in addr;
    int rv;

    /* Create the client socket. */
    t->client.socket = socket(AF_INET, SOCK_STREAM, 0);
    if (t->client.socket == -1) {
        munit_errorf("tcp: socket(): %s", tcpSocketError());
    }

    /* Initialize the socket address structure. */
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(port);

    /* Connect */
    rv = connect(t->client.socket, (struct sockaddr *)&addr, sizeof addr);
    if (rv == -1) {
        munit_errorf("tcp: connect(): %s", tcpSocketError());
    }
}

void test_tcp_close(struct test_tcp *t)
{
    tcpCloseSocket(t->client.socket);
    t->client.socket = -1;
}

void test_tcp_stop(struct test_tcp *t)
{
    tcpCloseSocket(t->server.socket);
    t->server.socket = -1;
}

void test_tcp_send(struct test_tcp *t, const void *buf, int len)
{
    int rv;

    rv = tcpSend(t->client.socket, buf, len);
    if (rv == -1) {
        munit_errorf("tcp: write(): %s", tcpSocketError());
    }
    if (rv != len) {
        munit_errorf("tcp: write(): only %d bytes written", rv);
    }
}

int test_tcp_accept(struct test_tcp *t)
{
    int socket;
    struct sockaddr_in address;
    socklen_t size;

    size = sizeof(address);

    socket = tcpAccept(t->server.socket, (struct sockaddr *)&address, &size);
    if (socket < 0) {
        munit_errorf("tcp: accept(): %s", tcpSocketError());
    }

    return socket;
}
