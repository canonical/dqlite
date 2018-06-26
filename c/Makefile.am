ACLOCAL_AMFLAGS = -I m4
AUTOMAKE_OPTIONS = foreign subdir-objects

lib_LTLIBRARIES =
check_PROGRAMS =
EXTRA_DIST =
noinst_HEADERS =
include_HEADERS =

EXTRA_DIST += dqlite.pc.in
pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = dqlite.pc

AM_CFLAGS = -I$(top_srcdir)/include -Wall
if DEBUG
  AM_CFLAGS += -g -O3 -fprofile-arcs -ftest-coverage -DDQLITE_DEBUG
else
  AM_CFLAGS += -O3
endif

AM_CFLAGS += $(SQLITE_CFLAGS) $(UV_CFLAGS)

lib_LTLIBRARIES += libdqlite.la
libdqlite_la_LDFLAGS = $(SQLITE_LIBS) $(UV_LIBS) -version-info 0:1:0
libdqlite_la_SOURCES = \
  src/binary.h \
  src/conn.c \
  src/conn.h \
  src/db.c \
  src/db.h \
  src/error.c \
  src/error.h \
  src/fsm.c \
  src/fsm.h \
  src/gateway.c \
  src/gateway.h \
  src/lifecycle.c \
  src/lifecycle.h \
  src/log.h \
  src/message.c \
  src/message.h \
  src/queue.c \
  src/queue.h \
  src/registry.h \
  src/request.c \
  src/request.h \
  src/response.c \
  src/response.h \
  src/schema.h \
  src/server.c \
  src/stmt.c \
  src/stmt.h \
  src/vfs.c
include_HEADERS += include/dqlite.h

# Tests
check_PROGRAMS += \
	dqlite-test
dqlite_test_SOURCES = \
  test/client.c \
  test/client.h \
  test/cluster.c \
  test/cluster.h \
  test/log.c \
  test/log.h \
  test/message.c \
  test/message.h \
  test/replication.c \
  test/replication.h \
  test/runner.c \
  test/server.c \
  test/server.h \
  test/socket.c \
  test/socket.h \
  test/suite.c \
  test/suite.h \
  test/test_conn.c \
  test/test_conn.h \
  test/test_db.c \
  test/test_db.h \
  test/test_error.c \
  test/test_error.h \
  test/test_gateway.c \
  test/test_gateway.h \
  test/test_integration.c \
  test/test_integration.h \
  test/test_message.c \
  test/test_message.h \
  test/test_queue.c \
  test/test_queue.h \
  test/test_registry.c \
  test/test_registry.h \
  test/test_request.c \
  test/test_request.h \
  test/test_response.c \
  test/test_response.h \
  test/test_schema.c \
  test/test_schema.h \
  test/test_server.c \
  test/test_server.h \
  test/test_stmt.c \
  test/test_stmt.h \
  test/test_vfs.c \
  test/test_vfs.h
dqlite_test_CFLAGS = \
  -I$(top_srcdir)/include \
  -I$(top_srcdir)/test \
  -DDQLITE_DEBUG \
  -Wall -g -O0
dqlite_test_CFLAGS += $(SQLITE3_CFLAGS) $(UV_CFLAGS)
dqlite_test_LDADD = libdqlite.la
dqlite_test_LDFLAGS = -lcunit -lpthread $(SQLITE_LIBS) $(UV_LIBS)
TESTS = dqlite-test

cov-reset:
	@lcov --directory src --zerocounters

cov-clean:
	@rm -fr coverage
	@find . -name "*.gcda" -o -name '*.gcno' -delete

cov-report:
	@mkdir -p coverage
	@lcov --compat-libtool --directory src --capture --output-file coverage/dqlite.info
	@genhtml -o coverage/ coverage/dqlite.info

clean-local:
	@make cov-clean

check: cov-reset
	@make cov-report
