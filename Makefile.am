ACLOCAL_AMFLAGS = -I m4

AM_CFLAGS += $(CODE_COVERAGE_CFLAGS)
AM_CFLAGS += $(SQLITE_CFLAGS) $(UV_CFLAGS) $(PTHREAD_CFLAGS)
AM_LDFLAGS = $(UV_LIBS) $(PTHREAD_LIBS)

if DQLITE_NEXT_ENABLED
AM_CFLAGS += -DDQLITE_NEXT
endif

if !BUILD_RAFT_ENABLED
AM_CFLAGS += $(RAFT_CFLAGS) -DUSE_SYSTEM_RAFT
AM_LDFLAGS += $(RAFT_LIBS)
endif

if DEBUG_ENABLED
AM_CFLAGS += -O0
else
AM_CFLAGS += -O2
endif

if SANITIZE_ENABLED
AM_CFLAGS += -fsanitize=address
endif

if BACKTRACE_ENABLED
AM_CFLAGS += -DDQLITE_ASSERT_WITH_BACKTRACE -DRAFT_ASSERT_WITH_BACKTRACE
AM_LDFLAGS += -lbacktrace
endif

include_HEADERS = include/dqlite.h

basic_dqlite_sources = \
  src/bind.c \
  src/client/protocol.c \
  src/command.c \
  src/conn.c \
  src/db.c \
  src/dqlite.c \
  src/error.c \
  src/format.c \
  src/fsm.c \
  src/gateway.c \
  src/id.c \
  src/leader.c \
  src/lib/addr.c \
  src/lib/buffer.c \
  src/lib/fs.c \
  src/lib/sm.c \
  src/lib/threadpool.c \
  src/lib/transport.c \
  src/logger.c \
  src/message.c \
  src/metrics.c \
  src/config.c \
  src/query.c \
  src/registry.c \
  src/request.c \
  src/response.c \
  src/roles.c \
  src/server.c \
  src/stmt.c \
  src/tracing.c \
  src/transport.c \
  src/translate.c \
  src/tuple.c \
  src/vfs.c \
  src/vfs2.c

lib_LTLIBRARIES = libdqlite.la
libdqlite_la_CFLAGS = $(AM_CFLAGS) -fvisibility=hidden -DRAFT_API=''
libdqlite_la_LDFLAGS = $(AM_LDFLAGS) -version-info 0:1:0
libdqlite_la_SOURCES = $(basic_dqlite_sources)

if BUILD_RAFT_ENABLED
libraft_la_SOURCES = \
  src/raft/byte.c \
  src/raft/callbacks.c \
  src/raft/client.c \
  src/raft/compress.c \
  src/raft/configuration.c \
  src/raft/convert.c \
  src/raft/election.c \
  src/raft/entry.c \
  src/raft/err.c \
  src/raft/fixture.c \
  src/raft/flags.c \
  src/raft/heap.c \
  src/raft/lifecycle.c \
  src/raft/log.c \
  src/raft/membership.c \
  src/raft/progress.c \
  src/raft/raft.c \
  src/raft/recv.c \
  src/raft/recv_append_entries.c \
  src/raft/recv_append_entries_result.c \
  src/raft/recv_request_vote.c \
  src/raft/recv_request_vote_result.c \
  src/raft/recv_install_snapshot.c \
  src/raft/recv_timeout_now.c \
  src/raft/replication.c \
  src/raft/snapshot.c \
  src/raft/start.c \
  src/raft/state.c \
  src/raft/syscall.c \
  src/raft/tick.c \
  src/raft/uv.c \
  src/raft/uv_append.c \
  src/raft/uv_encoding.c \
  src/raft/uv_finalize.c \
  src/raft/uv_fs.c \
  src/raft/uv_ip.c \
  src/raft/uv_list.c \
  src/raft/uv_metadata.c \
  src/raft/uv_os.c \
  src/raft/uv_prepare.c \
  src/raft/uv_recv.c \
  src/raft/uv_segment.c \
  src/raft/uv_send.c \
  src/raft/uv_snapshot.c \
  src/raft/uv_tcp.c \
  src/raft/uv_tcp_listen.c \
  src/raft/uv_tcp_connect.c \
  src/raft/uv_truncate.c \
  src/raft/uv_work.c \
  src/raft/uv_writer.c

libdqlite_la_SOURCES += $(libraft_la_SOURCES)
endif # BUILD_RAFT_ENABLED

check_PROGRAMS = unit-test integration-test

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

unit_test_SOURCES = $(basic_dqlite_sources)
unit_test_SOURCES += \
  test/test_error.c \
  test/test_integration.c \
  test/unit/ext/test_uv.c \
  test/unit/ext/test_uv_pool.c \
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
  test/unit/test_role_management.c \
  test/unit/test_sm.c \
  test/unit/test_tuple.c \
  test/unit/test_vfs.c \
  test/unit/test_vfs2.c \
  test/unit/main.c
unit_test_CFLAGS = $(AM_CFLAGS) -Wno-unknown-warning-option -Wno-uninitialized -Wno-maybe-uninitialized -Wno-float-equal -Wno-conversion
unit_test_LDFLAGS = $(AM_LDFLAGS)
unit_test_LDADD = libtest.la

if BUILD_RAFT_ENABLED
unit_test_LDADD += libraft.la
endif

integration_test_SOURCES = \
  test/integration/test_client.c \
  test/integration/test_cluster.c \
  test/integration/test_fsm.c \
  test/integration/test_membership.c \
  test/integration/test_node.c \
  test/integration/test_role_management.c \
  test/integration/test_server.c \
  test/integration/test_vfs.c \
  test/integration/main.c
integration_test_CFLAGS = $(AM_CFLAGS) -Wno-conversion
integration_test_LDFLAGS = $(AM_LDFLAGS) -no-install
integration_test_LDADD = libtest.la libdqlite.la

if BUILD_RAFT_ENABLED
check_LTLIBRARIES += libraft.la

check_PROGRAMS += \
  raft-core-unit-test \
  raft-core-integration-test \
  raft-uv-unit-test \
  raft-uv-integration-test \
  raft-core-fuzzy-test

libtest_la_SOURCES += \
  test/raft/lib/addrinfo.c \
  test/raft/lib/fault.c \
  test/raft/lib/fsm.c \
  test/raft/lib/heap.c \
  test/raft/lib/munit.c \
  test/raft/lib/tcp.c \
  test/raft/lib/cluster.c \
  test/raft/lib/aio.c \
  test/raft/lib/dir.c \
  test/raft/lib/tcp.c \
  test/raft/lib/loop.c

libraft_la_CFLAGS = $(AM_CFLAGS)
libraft_la_LDFLAGS = $(UV_LIBS)

raft_core_unit_test_SOURCES = \
  $(libraft_la_SOURCES) \
  src/lib/sm.c \
  src/lib/threadpool.c \
  src/tracing.c \
  test/raft/unit/main_core.c \
  test/raft/unit/test_byte.c \
  test/raft/unit/test_compress.c \
  test/raft/unit/test_configuration.c \
  test/raft/unit/test_err.c \
  test/raft/unit/test_flags.c \
  test/raft/unit/test_log.c \
  test/raft/unit/test_queue.c \
  test/raft/unit/test_snapshot.c

raft_core_unit_test_CFLAGS = $(AM_CFLAGS) -Wno-conversion
raft_core_unit_test_LDADD = libtest.la

raft_core_integration_test_SOURCES = \
  src/tracing.c \
  src/lib/sm.c \
  src/lib/threadpool.c \
  test/raft/integration/main_core.c \
  test/raft/integration/test_apply.c \
  test/raft/integration/test_assign.c \
  test/raft/integration/test_barrier.c \
  test/raft/integration/test_bootstrap.c \
  test/raft/integration/test_digest.c \
  test/raft/integration/test_election.c \
  test/raft/integration/test_fixture.c \
  test/raft/integration/test_heap.c \
  test/raft/integration/test_init.c \
  test/raft/integration/test_membership.c \
  test/raft/integration/test_recover.c \
  test/raft/integration/test_replication.c \
  test/raft/integration/test_snapshot.c \
  test/raft/integration/test_start.c \
  test/raft/integration/test_strerror.c \
  test/raft/integration/test_tick.c \
  test/raft/integration/test_transfer.c \
  test/raft/integration/test_voter_contacts.c
raft_core_integration_test_CFLAGS = $(AM_CFLAGS) -Wno-conversion
raft_core_integration_test_LDFLAGS = -no-install
raft_core_integration_test_LDADD = libtest.la libraft.la

raft_core_fuzzy_test_SOURCES = \
  src/lib/sm.c \
  src/lib/threadpool.c \
  src/tracing.c \
  test/raft/fuzzy/main_core.c \
  test/raft/fuzzy/test_election.c \
  test/raft/fuzzy/test_liveness.c \
  test/raft/fuzzy/test_membership.c \
  test/raft/fuzzy/test_replication.c
raft_core_fuzzy_test_CFLAGS = $(AM_CFLAGS) -Wno-conversion
raft_core_fuzzy_test_LDFLAGS = -no-install
raft_core_fuzzy_test_LDADD = libtest.la libraft.la

raft_uv_unit_test_SOURCES = \
  src/tracing.c \
  src/raft/err.c \
  src/raft/heap.c \
  src/raft/syscall.c \
  src/raft/uv_fs.c \
  src/raft/uv_os.c \
  src/raft/uv_writer.c \
  test/raft/unit/main_uv.c \
  test/raft/unit/test_uv_fs.c \
  test/raft/unit/test_uv_os.c \
  test/raft/unit/test_uv_writer.c
raft_uv_unit_test_CFLAGS = $(AM_CFLAGS) -Wno-conversion
raft_uv_unit_test_LDADD = libtest.la $(UV_LIBS)

# The integration/uv test is not linked to libraft, but built
# directly against the libraft sources in order to test some
# non-visible, non-API functions.
raft_uv_integration_test_SOURCES = \
  $(libraft_la_SOURCES) \
  src/tracing.c \
  src/lib/sm.c \
  src/lib/threadpool.c \
  test/raft/integration/main_uv.c \
  test/raft/integration/test_uv_init.c \
  test/raft/integration/test_uv_append.c \
  test/raft/integration/test_uv_bootstrap.c \
  test/raft/integration/test_uv_load.c \
  test/raft/integration/test_uv_recover.c \
  test/raft/integration/test_uv_recv.c \
  test/raft/integration/test_uv_send.c \
  test/raft/integration/test_uv_set_term.c \
  test/raft/integration/test_uv_tcp_connect.c \
  test/raft/integration/test_uv_tcp_listen.c \
  test/raft/integration/test_uv_snapshot_put.c \
  test/raft/integration/test_uv_truncate.c \
  test/raft/integration/test_uv_truncate_snapshot.c \
  test/raft/integration/test_uv_work.c
raft_uv_integration_test_CFLAGS = $(AM_CFLAGS) -Wno-type-limits -Wno-conversion
raft_uv_integration_test_LDFLAGS = -no-install
raft_uv_integration_test_LDADD = libtest.la $(UV_LIBS)

if LZ4_AVAILABLE
libdqlite_la_CFLAGS += -DLZ4_AVAILABLE $(LZ4_CFLAGS)
libdqlite_la_LDFLAGS += $(LZ4_LIBS)
raft_core_unit_test_CFLAGS += -DLZ4_AVAILABLE $(LZ4_CFLAGS)
raft_core_unit_test_LDFLAGS = $(LZ4_LIBS)
libraft_la_CFLAGS += -DLZ4_AVAILABLE $(LZ4_CFLAGS)
libraft_la_LDFLAGS += $(LZ4_LIBS)
raft_uv_integration_test_CFLAGS += -DLZ4_AVAILABLE
raft_uv_integration_test_LDFLAGS += $(LZ4_LIBS)
endif

if LZ4_ENABLED
libdqlite_la_CFLAGS += -DLZ4_ENABLED
raft_uv_integration_test_CFLAGS += -DLZ4_ENABLED
raft_core_unit_test_CFLAGS += -DLZ4_ENABLED
libraft_la_CFLAGS += -DLZ4_ENABLED
endif

endif # BUILD_RAFT_ENABLED

if BUILD_SQLITE_ENABLED
noinst_LTLIBRARIES = libsqlite3.la
libsqlite3_la_SOURCES = sqlite3.c
libsqlite3_la_CFLAGS = -g3

unit_test_LDADD += libsqlite3.la
libdqlite_la_LIBADD = libsqlite3.la
else
AM_LDFLAGS += $(SQLITE_LIBS)
endif

TESTS = $(check_PROGRAMS)

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
