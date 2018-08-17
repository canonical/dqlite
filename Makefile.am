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
  AM_CFLAGS += -O0 -g -fprofile-arcs -ftest-coverage -DDQLITE_DEBUG
else
  AM_CFLAGS += -O2
endif
if EXPERIMENTAL
  AM_CFLAGS += -DDQLITE_EXPERIMENTAL
endif

AM_CFLAGS += $(SQLITE_CFLAGS) $(UV_CFLAGS)
if EXPERIMENTAL
  AM_CFLAGS += $(ZLIB_CFLAGS) $(CO_CFLAGS)
endif

lib_LTLIBRARIES += libdqlite.la
libdqlite_la_LDFLAGS = $(SQLITE_LIBS) $(UV_LIBS) -version-info 0:1:0
if EXPERIMENTAL
  libdqlite_la_LDFLAGS += $(ZLIB_LIBS) $(CO_LIBS)
endif
libdqlite_la_SOURCES = \
  src/binary.h \
  src/conn.c \
  src/conn.h \
  src/db.c \
  src/db.h \
  src/error.c \
  src/error.h \
  src/file.c \
  src/format.c \
  src/format.h \
  src/fsm.c \
  src/fsm.h \
  src/gateway.c \
  src/gateway.h \
  src/lifecycle.c \
  src/lifecycle.h \
  src/log.h \
  src/options.c \
  src/options.h \
  src/message.c \
  src/message.h \
  src/metrics.c \
  src/metrics.h \
  src/queue.c \
  src/queue.h \
  src/registry.h \
  src/replication.c \
  src/replication.h \
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
  test/case.c \
  test/case.h \
  test/client.c \
  test/client.h \
  test/cluster.c \
  test/cluster.h \
  test/fs.c \
  test/fs.h \
  test/leak.c \
  test/leak.h \
  test/log.c \
  test/log.h \
  test/mem.c \
  test/mem.h \
  test/message.c \
  test/message.h \
  test/munit.c \
  test/munit.h \
  test/replication.c \
  test/replication.h \
  test/runner.c \
  test/server.c \
  test/server.h \
  test/socket.c \
  test/socket.h \
  test/test_conn.c \
  test/test_db.c \
  test/test_error.c \
  test/test_file.c \
  test/test_format.c \
  test/test_gateway.c \
  test/test_integration.c \
  test/test_message.c \
  test/test_queue.c \
  test/test_registry.c \
  test/test_replication.c \
  test/test_request.c \
  test/test_response.c \
  test/test_schema.c \
  test/test_server.c \
  test/test_stmt.c \
  test/test_uv.c \
  test/test_vfs.c
dqlite_test_CFLAGS = $(AM_CFLAGS)
dqlite_test_CFLAGS += -I$(top_srcdir)/test -DMUNIT_NO_FORK
dqlite_test_LDADD = libdqlite.la
dqlite_test_LDFLAGS = -lpthread $(SQLITE_LIBS) $(UV_LIBS)
if EXPERIMENTAL
  dqlite_test_LDFLAGS += $(ZLIB_LIBS) $(CO_LIBS)
endif
TESTS = dqlite-test

cov-reset:
if DEBUG
	@lcov --directory src --zerocounters
endif

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
if DEBUG
	@make cov-report
endif
