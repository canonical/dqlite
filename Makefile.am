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
AM_CFLAGS += $(SQLITE_CFLAGS) $(UV_CFLAGS)
AM_LDFLAGS = $(SQLITE_LIBS) $(UV_LIBS)
if DEBUG
  AM_CFLAGS += -g -DDQLITE_DEBUG
else
  AM_CFLAGS += -O2
endif
if COVERAGE
  AM_CFLAGS += -O0 -fprofile-arcs -ftest-coverage
endif
if SANITIZE
  AM_CFLAGS += -fsanitize=address
endif
if EXPERIMENTAL
  AM_CFLAGS += -DDQLITE_EXPERIMENTAL
  AM_CFLAGS += $(CO_CFLAGS) $(RAFT_FLAGS)
  AM_LDFLAGS += $(CO_LIBS) $(RAFT_LIBS)
endif

lib_LTLIBRARIES += libdqlite.la
libdqlite_la_LDFLAGS = $(AM_LDFLAGS) -version-info 0:1:0
libdqlite_la_SOURCES = \
  src/conn.c \
  src/db_.c \
  src/error.c \
  src/file.c \
  src/format.c \
  src/gateway.c \
  src/lib/fsm.c \
  src/lib/logger.c \
  src/lifecycle.c \
  src/message.c \
  src/metrics.c \
  src/options.c \
  src/queue.c \
  src/request.c \
  src/response.c \
  src/server.c \
  src/stmt.c \
  src/vfs.c
if EXPERIMENTAL
  libdqlite_la_SOURCES += \
  src/command.c \
  src/db.c \
  src/fsm.c \
  src/leader.c \
  src/registry.c \
  src/replication.c \
  src/tx.c
endif
include_HEADERS += include/dqlite.h

# Tests
check_PROGRAMS += \
  unit-test
test_lib_SOURCES = \
  test/case.c \
  test/client.c \
  test/cluster.c \
  test/fs.c \
  test/leak.c \
  test/lib/fault.c \
  test/lib/heap.c \
  test/lib/logger.c \
  test/lib/message.c \
  test/lib/munit.c \
  test/lib/socket.c \
  test/lib/sqlite.c \
  test/lib/uv.c \
  test/log.c \
  test/mem.c \
  test/replication.c \
  test/server.c
unit_test_SOURCES = $(libdqlite_la_SOURCES) $(test_lib_SOURCES)
unit_test_SOURCES += \
  test/test_conn.c \
  test/test_db.c \
  test/test_error.c \
  test/test_file.c \
  test/test_format.c \
  test/test_integration.c \
  test/test_message.c \
  test/test_queue.c \
  test/test_request.c \
  test/test_response.c \
  test/test_schema.c \
  test/test_server.c \
  test/test_stmt.c \
  test/test_vfs.c\
  test/unit/ext/test_uv.c \
  test/unit/lib/test_registry.c \
  test/unit/lib/test_serialization.c \
  test/unit/test_gateway.c \
  test/unit/main.c
if EXPERIMENTAL
  unit_test_SOURCES += \
  test/lib/raft.c \
  test/unit/ext/test_co.c \
  test/unit/test_command.c \
  test/unit/test_leader.c \
  test/unit/test_registry.c \
  test/unit/test_replication.c
endif
unit_test_CFLAGS = $(AM_CFLAGS)
unit_test_CFLAGS += -I$(top_srcdir)/test
unit_test_CFLAGS += -DDQLITE_TEST -DMUNIT_TEST_NAME_LEN=60
unit_test_LDFLAGS = -lpthread $(AM_LDFLAGS)
TESTS = unit-test

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
