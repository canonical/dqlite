AC_PREREQ(2.60)
AC_INIT([libdqlite], [1.13.0], [https://github.com/canonical/dqlite])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([ac])

AM_INIT_AUTOMAKE([subdir-objects -Wall -Werror -Wno-portability foreign])
AM_SILENT_RULES([yes])

AC_PROG_CC_STDC
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

# Whether to enable code coverage.
AX_CODE_COVERAGE

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h fcntl.h stdint.h stdlib.h string.h sys/socket.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT8_T

# Enable large file support. This is mandatory in order to interoperate with
# libuv, which enables large file support by default, making the size of 'off_t'
# on 32-bit architecture be 8 bytes instead of the normal 4.
AC_SYS_LARGEFILE

# Checks for libraries
PKG_CHECK_MODULES(SQLITE, [sqlite3 >= 3.22.0], [], [])
PKG_CHECK_MODULES(UV, [libuv >= 1.8.0], [], [])
PKG_CHECK_MODULES(RAFT, [raft >= 0.16.0], [], [])

CC_CHECK_FLAGS_APPEND([AM_CFLAGS],[CFLAGS],[ \
  -std=c11 \
  -g \
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
  -Werror \
])
# To enable:
#
#  -Wpedantic \
AC_SUBST(AM_CFLAGS)

AC_CONFIG_FILES([dqlite.pc Makefile])
AC_OUTPUT
