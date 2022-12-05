ACLOCAL_AMFLAGS = -I m4
AM_CFLAGS += $(CODE_COVERAGE_CFLAGS)
AM_CFLAGS += $(SQLITE_CFLAGS) $(UV_CFLAGS) $(RAFT_CFLAGS) $(PTHREAD_CFLAGS)
AM_LDFLAGS = $(SQLITE_LIBS) $(UV_LIBS) $(RAFT_LIBS) $(PTHREAD_LIBS)

include_HEADERS = include/dqlite.h

lib_LTLIBRARIES = libdqlite.la
libdqlite_la_LDFLAGS = $(AM_LDFLAGS) -version-info 0:1:0
libdqlite_la_SOURCES = \
  src/bind.c \
  src/client.c \
  src/command.c \
  src/conn.c \
  src/db.c \
  src/dqlite.c \
  src/error.c \
  src/format.c \
  src/fsm.c \
  src/gateway.c \
  src/leader.c \
  src/lib/addr.c \
  src/lib/buffer.c \
  src/lib/fs.c \
  src/lib/transport.c \
  src/logger.c \
  src/message.c \
  src/metrics.c \
  src/config.c \
  src/query.c \
  src/registry.c \
  src/request.c \
  src/response.c \
  src/server.c \
  src/stmt.c \
  src/tracing.c \
  src/transport.c \
  src/translate.c \
  src/tuple.c \
  src/vfs.c

check_PROGRAMS = \
  unit-test \
  integration-test
TESTS = unit-test integration-test

check_LTLIBRARIES = libtest.la

libtest_la_CFLAGS = $(AM_CFLAGS) -DMUNIT_TEST_NAME_LEN=60 -Wno-unknown-warning-option -Wno-unused-result -Wno-conversion -Wno-uninitialized -Wno-maybe-uninitialized -Wno-strict-prototypes -Wno-old-style-definition
libtest_la_SOURCES = \
  test/lib/endpoint.c \
  test/lib/fault.c \
  test/lib/fs.c \
  test/lib/heap.c \
  test/lib/logger.c \
  test/lib/munit.c \
  test/lib/raft_heap.c \
  test/lib/server.c \
  test/lib/sqlite.c \
  test/lib/uv.c

unit_test_SOURCES = $(libdqlite_la_SOURCES)
unit_test_SOURCES += \
  test/test_error.c \
  test/test_integration.c \
  test/unit/ext/test_uv.c \
  test/unit/lib/test_addr.c \
  test/unit/lib/test_buffer.c \
  test/unit/lib/test_byte.c \
  test/unit/lib/test_registry.c \
  test/unit/lib/test_serialize.c \
  test/unit/lib/test_transport.c \
  test/unit/test_command.c \
  test/unit/test_conn.c \
  test/unit/test_gateway.c \
  test/unit/test_concurrency.c \
  test/unit/test_registry.c \
  test/unit/test_replication.c \
  test/unit/test_request.c \
  test/unit/test_tuple.c \
  test/unit/test_vfs.c \
  test/unit/main.c
unit_test_CFLAGS = $(AM_CFLAGS) -Wno-unknown-warning-option -Wno-uninitialized -Wno-maybe-uninitialized -Wno-float-equal -Wno-conversion
unit_test_LDFLAGS = $(AM_LDFLAGS)
unit_test_LDADD = libtest.la

integration_test_SOURCES = \
  test/integration/test_client.c \
  test/integration/test_cluster.c \
  test/integration/test_fsm.c \
  test/integration/test_membership.c \
  test/integration/test_node.c \
  test/integration/test_vfs.c \
  test/integration/main.c
integration_test_CFLAGS = $(AM_CFLAGS) -Wno-conversion
integration_test_LDFLAGS = $(AM_LDFLAGS) -no-install
integration_test_LDADD = libtest.la libdqlite.la

if DEBUG_ENABLED
  AM_CFLAGS += -g
else
  AM_CFLAGS += -O2
endif
if SANITIZE_ENABLED
  AM_CFLAGS += -fsanitize=address
endif
if BACKTRACE_ENABLED
  AM_CFLAGS += -DDQLITE_ASSERT_WITH_BACKTRACE
  AM_LDFLAGS += -lbacktrace
endif

if CODE_COVERAGE_ENABLED

include $(top_srcdir)/aminclude_static.am

CODE_COVERAGE_DIRECTORY=./src
CODE_COVERAGE_OUTPUT_DIRECTORY=coverage
CODE_COVERAGE_OUTPUT_FILE=coverage.info
CODE_COVERAGE_IGNORE_PATTERN="/usr/include/*"
CODE_COVERAGE_BRANCH_COVERAGE=1
CODE_COVERAGE_LCOV_OPTIONS=$(CODE_COVERAGE_LCOV_OPTIONS_DEFAULT) --rc lcov_excl_br_line="assert\("

clean-local: code-coverage-clean
distclean-local: code-coverage-dist-clean

endif # CODE_COVERAGE_ENABLED

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = dqlite.pc
