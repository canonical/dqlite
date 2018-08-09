AC_PREREQ(2.60)

AC_INIT(libdqlite, 0.1, [])
AC_CONFIG_SRCDIR(include/dqlite.h)
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([ac])

AC_CANONICAL_BUILD()
AC_CANONICAL_HOST()
AC_CANONICAL_TARGET()

AM_INIT_AUTOMAKE([subdir-objects -Wall -Werror foreign])
AM_SILENT_RULES([yes])
AC_CONFIG_HEADERS([config.h])

AC_USE_SYSTEM_EXTENSIONS

LT_INIT

# Checks for programs.
AC_LANG([C])
AC_PROG_CC
#AM_PROG_AR
#AC_PROG_LIBTOOL

AC_PROG_INSTALL
AC_PROG_LN_S

AC_ARG_ENABLE(debug,
  AS_HELP_STRING(
    [--enable-debug],
    [enable debugging, default: no]),
    [case "${enableval}" in
      yes) debug=true ;;
      no)  debug=false ;;
      *)   AC_MSG_ERROR([bad value ${enableval} for --enable-debug]) ;;
    esac],
    [debug=false])
AM_CONDITIONAL(DEBUG, test x"$debug" = x"true")
AM_COND_IF(DEBUG,
AC_DEFINE(DEBUG, 1, [Define to 0 if this is a release build]),
AC_DEFINE(DEBUG, 0, [Define to 1 or higher if this is a debug build]))

AC_ARG_ENABLE(experimental,
  AS_HELP_STRING(
    [--enable-experimental],
    [enable experimental features, default: no]),
    [case "${enableval}" in
      yes) experimental=true ;;
      no)  experimental=false ;;
      *)   AC_MSG_ERROR([bad value ${enableval} for --enable-experimental]) ;;
    esac],
    [experimental=false])
AM_CONDITIONAL(EXPERIMENTAL, test x"$experimental" = x"true")

# Checks for libraries
PKG_CHECK_MODULES(SQLITE, [sqlite3 >= 3.22.0], [], [])
PKG_CHECK_MODULES(UV, [libuv >= 1.8.0], [], [])

AM_COND_IF(EXPERIMENTAL,
  [
  PKG_CHECK_MODULES(ZLIB, [zlib], [], [])
  PKG_CHECK_MODULES(CO, [libco], [], [])
  AC_DEFINE(EXPERIMENTAL, 1, [Define to 0 to exclude experimental features])
  ], [
  AC_DEFINE(EXPERIMENTAL, 0, [Define to 1 to include experimental features])
  ])

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h fcntl.h stdint.h stdlib.h string.h sys/socket.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([socket strerror])

#AC_ENABLE_SHARED
#AC_DISABLE_STATIC
AC_CONFIG_FILES([
	Makefile
	dqlite.pc
])
AC_OUTPUT
