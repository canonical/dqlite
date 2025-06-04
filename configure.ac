AC_PREREQ(2.60)
AC_INIT([libdqlite], [1.18.0], [https://github.com/canonical/dqlite])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([ac])

AM_INIT_AUTOMAKE([subdir-objects -Wall -Werror -Wno-portability foreign])
AM_SILENT_RULES([yes])

AC_SUBST(AM_CFLAGS)
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

AC_ARG_ENABLE([build-raft],
  [AS_HELP_STRING([--enable-build-raft[=ARG]],
		  [use the bundled raft sources instead of linking libraft (default is yes, required)])],
  [enable_build_raft=$enableval],
  [enable_build_raft=yes])
AS_IF([test "x$enable_build_raft" != "xyes"],
      AC_MSG_ERROR([linking to a separately-built libraft is no longer supported]),
      [])

AC_ARG_WITH(static-deps,
	    AS_HELP_STRING([--with-static-deps[=ARG]],
			   [skip building a shared library and link test binaries statically]))
AM_CONDITIONAL(WITH_STATIC_DEPS, test "x$with_static_deps" = "xyes")

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
PKG_CHECK_MODULES(SQLITE, [sqlite3 >= 3.34.0], [], [])
PKG_CHECK_MODULES(UV, [libuv >= 1.34.0], [], [])

# Allow not linking to liblz4 even if it's present.
AC_ARG_WITH([lz4], AS_HELP_STRING([--without-lz4], [never link to liblz4]))
AS_IF([test "x$with_lz4" != "xno"],
     [PKG_CHECK_MODULES(LZ4, [liblz4 >= 1.7.1], [have_lz4=yes], [have_lz4=no])],
     [have_lz4=no])
AS_IF([test "x$with_lz4" = "xyes" -a "x$have_lz4" = "xno"],
     [AC_MSG_ERROR([liblz4 required but not found])],
     [])

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
  -fstack-clash-protection \
  -fstack-protector-strong \
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
