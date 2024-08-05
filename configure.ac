AC_PREREQ(2.60)
AC_INIT([libdqlite], [1.16.7], [https://github.com/canonical/dqlite])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([ac])

AM_INIT_AUTOMAKE([subdir-objects -Wall -Werror -Wno-portability foreign])
AM_SILENT_RULES([yes])

# Without this line, AC_PROG_CC boneheadedly adds `-g -O2` to our CFLAGS.
AC_SUBST(CFLAGS, "")
AC_PROG_CC
AC_USE_SYSTEM_EXTENSIONS

AX_PTHREAD

LT_INIT

# TODO: eventually enable this
# AX_CHECK_COMPILE_FLAG([-Weverything], AM_CFLAGS+=" -Weverything")

# Whether to enable debugging code.
AC_ARG_ENABLE(debug, AS_HELP_STRING([--enable-debug[=ARG]], [enable debugging [default=no]]))
AM_CONDITIONAL(DEBUG_ENABLED, test "x$enable_debug" = "xyes")

# Whether to enable memory sanitizer.
AC_ARG_ENABLE(sanitize, AS_HELP_STRING([--enable-sanitize[=ARG]], [enable code sanitizers [default=no]]))
AM_CONDITIONAL(SANITIZE_ENABLED, test x"$enable_sanitize" = x"yes")
AM_COND_IF(SANITIZE_ENABLED,
  AX_CHECK_COMPILE_FLAG([-fsanitize=address],
   [true],
   [AC_MSG_ERROR([address sanitizer not supported])]))

AC_ARG_ENABLE(backtrace, AS_HELP_STRING([--enable-backtrace[=ARG]], [print backtrace on assertion failure [default=no]]))
AM_CONDITIONAL(BACKTRACE_ENABLED, test "x$enable_backtrace" = "xyes")

AC_ARG_ENABLE(build-sqlite, AS_HELP_STRING([--enable-build-sqlite[=ARG]], [build libsqlite3 from sqlite3.c in the build root [default=no]]))
AM_CONDITIONAL(BUILD_SQLITE_ENABLED, test "x$enable_build_sqlite" = "xyes")

AC_ARG_ENABLE(build-raft, AS_HELP_STRING([--enable-build-raft[=ARG]], [use the bundled raft sources instead of linking to libraft [default=no]]))
AM_CONDITIONAL(BUILD_RAFT_ENABLED, test "x$enable_build_raft" = "xyes")

AC_ARG_ENABLE(dqlite-next, AS_HELP_STRING([--enable-dqlite-next[=ARG]], [build with the experimental dqlite backend [default=no]]))
AM_CONDITIONAL(DQLITE_NEXT_ENABLED, test "x$enable_dqlite_next" = "xyes")
AS_IF([test "x$enable_build_raft" != "xyes" -a "x$enable_dqlite_next" = "xyes"], [AC_MSG_ERROR([dqlite-next requires bundled raft])], [])

# Whether to enable code coverage.
AX_CODE_COVERAGE

# Checks for header files.
AC_CHECK_HEADERS([linux/io_uring.h linux/aio_abi.h])

# Checks for library functions and definitions.
AC_CHECK_DECLS(RWF_NOWAIT, [], [AC_MSG_ERROR(Linux kernel >= 4.14 required.)], [#include <linux/aio_abi.h>])

# Enable large file support. This is mandatory in order to interoperate with
# libuv, which enables large file support by default, making the size of 'off_t'
# on 32-bit architecture be 8 bytes instead of the normal 4.
AC_SYS_LARGEFILE

# Checks for libraries
PKG_CHECK_MODULES(SQLITE, [sqlite3 >= 3.22.0], [], [])
PKG_CHECK_MODULES(UV, [libuv >= 1.34.0], [], [])
AS_IF([test "x$enable_build_raft" != "xyes"], [PKG_CHECK_MODULES(RAFT, [raft >= 0.18.1], [], [])], [])


# Allow not linking to liblz4 even if it's present.
AC_ARG_WITH([lz4], AS_HELP_STRING([--without-lz4], [never link to liblz4]))
AS_IF([test "x$enable_build_raft" = "xyes"],
      # Building raft
      [AS_IF([test "x$with_lz4" != "xno"],
	     [PKG_CHECK_MODULES(LZ4, [liblz4 >= 1.7.1], [have_lz4=yes], [have_lz4=no])],
	     [have_lz4=no])
       AS_IF([test "x$with_lz4" != "xno" -a "x$have_lz4" = "xno"],
	     [AC_MSG_ERROR([liblz4 required but not found])],
	     [])],
      # Not building raft
      [AS_IF([test "x$with_lz4" = "xyes"],
	     [AC_MSG_ERROR([linking lz4 doesn't make sense unless building raft])],
	     [])
      have_lz4=no])

AM_CONDITIONAL(LZ4_AVAILABLE, test "x$have_lz4" = "xyes")

AC_ARG_ENABLE(lz4, AS_HELP_STRING([--disable-lz4], [when building with lz4, do not compress snapshots by default]))
AS_IF([test "x$enable_lz4" != "x" -a "x$have_lz4" = "xno"],
      [AC_MSG_ERROR([snapshot compression (either by default or not) requires liblz4])],
      [])
AM_CONDITIONAL(LZ4_ENABLED, test "x$enable_lz4" != "xno" -a "x$have_lz4" = "xyes")

CC_CHECK_FLAGS_APPEND([AM_CFLAGS],[CFLAGS],[ \
  -std=c11 \
  -g3 \
  --mcet \
  -fcf-protection \
  --param=ssp-buffer-size=4 \
  -pipe \
  -fno-strict-aliasing \
  -fdiagnostics-color \
  -fexceptions \
  -fstack-clash-protection \
  -fstack-protector-strong \
  -fasynchronous-unwind-tables \
  -fdiagnostics-show-option \
  -Wall \
  -Wextra \
  -Wimplicit-fallthrough=5 \
  -Wcast-align \
  -Wstrict-prototypes \
  -Wlogical-op \
  -Wmissing-include-dirs \
  -Wold-style-definition \
  -Winit-self \
  -Wfloat-equal \
  -Wsuggest-attribute=noreturn \
  -Wformat=2 \
  -Wshadow \
  -Wendif-labels \
  -Wdate-time \
  -Wnested-externs \
  -Wconversion \
  -Wno-format-nonliteral \
  -Werror \
])
# To enable:
#
#  -Wpedantic \
AC_SUBST(AM_CFLAGS)

AC_CONFIG_FILES([dqlite.pc Makefile])
AC_OUTPUT
