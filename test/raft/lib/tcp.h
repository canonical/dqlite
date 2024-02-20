/* Test TCP utilities.
 *
 * This module sports helpers to create server or client sockets, and
 * send/receive data through them.
 */

#ifndef TEST_TCP_H
#define TEST_TCP_H

#include "munit.h"

/* Macro helpers. */
#define FIXTURE_TCP_SERVER struct TcpServer server
#define SETUP_TCP_SERVER TcpServerInit(&f->server)
#define TEAR_DOWN_TCP_SERVER TcpServerClose(&f->server)

#define TCP_SERVER_STOP TcpServerStop(&f->server)
#define TCP_SERVER_PORT f->server.port
#define TCP_SERVER_ADDRESS f->server.address

#define FIXTURE_TCP struct test_tcp tcp
#define SETUP_TCP test_tcp_setup(params, &f->tcp)
#define TEAR_DOWN_TCP test_tcp_tear_down(&f->tcp)

#define TCP_CLIENT_CONNECT(PORT) test_tcp_connect(&f->tcp, PORT)
#define TCP_CLIENT_SEND(BUF, N) test_tcp_send(&f->tcp, BUF, N)
#define TCP_CLIENT_CLOSE test_tcp_close(&f->tcp)

struct TcpServer
{
    int socket; /* Socket listening to incoming connections */
    int port;
    char address[128]; /* IPv4 address of the server, with port */
};

void TcpServerInit(struct TcpServer *s);
void TcpServerClose(struct TcpServer *s);

/* Accept inbound client connection and return the relevant socket. */
int TcpServerAccept(struct TcpServer *s);

/* Close the server socket. */
void TcpServerStop(struct TcpServer *s);

struct TcpClient
{
    int socket; /* Socket connected to a server. */
};

void TcpClientInit(struct TcpClient *s);
void TcpClientClose(struct TcpClient *s);

/* Object that can be used to setup and control a TCP server and/or client. */
struct test_tcp
{
    struct
    {
        int socket;        /* Socket listening to incoming connections */
        char address[128]; /* IPv4 address of the server, with port */
    } server;
    struct
    {
        int socket; /* Socket connected to another host */
    } client;
};

/**
 * Bind the server socket of the given test TCP host to localhost and start
 * listening to it.
 */
void test_tcp_setup(const MunitParameter params[], struct test_tcp *t);

void test_tcp_tear_down(struct test_tcp *t);

/**
 * Start listening to a random free port on localhost.
 */
void test_tcp_listen(struct test_tcp *t);

/**
 * Return the address of the server socket created with @test_tcp_listen.
 */
const char *test_tcp_address(struct test_tcp *t);

/**
 * Connect the client socket to the given port on localhost.
 */
void test_tcp_connect(struct test_tcp *t, int port);

/**
 * Close the client socket.
 */
void test_tcp_close(struct test_tcp *t);

/**
 * Send data using the client socket.
 */
void test_tcp_send(struct test_tcp *t, const void *buf, int len);

/**
 * Accept inbound client connection and return the relevant socket.
 */
int test_tcp_accept(struct test_tcp *t);

/**
 * Close the server socket.
 */
void test_tcp_stop(struct test_tcp *t);

#endif /* TEST_TCP_H */
