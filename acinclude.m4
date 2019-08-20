# ===========================================================================
#   http://www.gnu.org/software/autoconf-archive/ax_check_compile_flag.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_CHECK_COMPILE_FLAG(FLAG, [ACTION-SUCCESS], [ACTION-FAILURE], [EXTRA-FLAGS])
#
# DESCRIPTION
#
#   Check whether the given FLAG works with the current language's compiler
#   or gives an error.  (Warnings, however, are ignored)
#
#   ACTION-SUCCESS/ACTION-FAILURE are shell commands to execute on
#   success/failure.
#
#   If EXTRA-FLAGS is defined, it is added to the current language's default
#   flags (e.g. CFLAGS) when the check is done.  The check is thus made with
#   the flags: "CFLAGS EXTRA-FLAGS FLAG".  This can for example be used to
#   force the compiler to issue an error when a bad flag is given.
#
#   NOTE: Implementation based on AX_CFLAGS_GCC_OPTION. Please keep this
#   macro in sync with AX_CHECK_{PREPROC,LINK}_FLAG.
#
# LICENSE
#
#   Copyright (c) 2008 Guido U. Draheim <guidod@gmx.de>
#   Copyright (c) 2011 Maarten Bosmans <mkbosmans@gmail.com>
#
#   This program is free software: you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the
#   Free Software Foundation, either version 3 of the License, or (at your
#   option) any later version.
#
#   This program is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
#   Public License for more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program. If not, see <http://www.gnu.org/licenses/>.
#
#   As a special exception, the respective Autoconf Macro's copyright owner
#   gives unlimited permission to copy, distribute and modify the configure
#   scripts that are the output of Autoconf when processing the Macro. You
#   need not follow the terms of the GNU General Public License when using
#   or distributing such scripts, even though portions of the text of the
#   Macro appear in them. The GNU General Public License (GPL) does govern
#   all other use of the material that constitutes the Autoconf Macro.
#
#   This special exception to the GPL applies to versions of the Autoconf
#   Macro released by the Autoconf Archive. When you make and distribute a
#   modified version of the Autoconf Macro, you may extend this special
#   exception to the GPL to apply to your modified version as well.

serial 2

AC_DEFUN([AX_CHECK_COMPILE_FLAG],
[AC_PREREQ(2.59)dnl for _AC_LANG_PREFIX
AS_VAR_PUSHDEF([CACHEVAR],[ax_cv_check_[]_AC_LANG_ABBREV[]flags_$4_$1])dnl
AC_CACHE_CHECK([whether _AC_LANG compiler accepts $1], CACHEVAR, [
  ax_check_save_flags=$[]_AC_LANG_PREFIX[]FLAGS
  _AC_LANG_PREFIX[]FLAGS="$[]_AC_LANG_PREFIX[]FLAGS $4 $1"
  AC_COMPILE_IFELSE([AC_LANG_PROGRAM()],
    [AS_VAR_SET(CACHEVAR,[yes])],
    [AS_VAR_SET(CACHEVAR,[no])])
  _AC_LANG_PREFIX[]FLAGS=$ax_check_save_flags])
AS_IF([test x"AS_VAR_GET(CACHEVAR)" = xyes],
  [m4_default([$2], :)],
  [m4_default([$3], :)])
AS_VAR_POPDEF([CACHEVAR])dnl
])dnl AX_CHECK_COMPILE_FLAGS

# ===========================================================================
#     https://www.gnu.org/software/autoconf-archive/ax_file_escapes.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_FILE_ESCAPES
#
# DESCRIPTION
#
#   Writes the specified data to the specified file.
#
# LICENSE
#
#   Copyright (c) 2008 Tom Howard <tomhoward@users.sf.net>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

serial 8

AC_DEFUN([AX_FILE_ESCAPES],[
AX_DOLLAR="\$"
AX_SRB="\\135"
AX_SLB="\\133"
AX_BS="\\\\"
AX_DQ="\""
])

# ===========================================================================
#   https://www.gnu.org/software/autoconf-archive/ax_ac_print_to_file.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_AC_PRINT_TO_FILE([FILE],[DATA])
#
# DESCRIPTION
#
#   Writes the specified data to the specified file when Autoconf is run. If
#   you want to print to a file when configure is run use AX_PRINT_TO_FILE
#   instead.
#
# LICENSE
#
#   Copyright (c) 2009 Allan Caffee <allan.caffee@gmail.com>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

serial 10

AC_DEFUN([AX_AC_PRINT_TO_FILE],[
m4_esyscmd(
AC_REQUIRE([AX_FILE_ESCAPES])
[
printf "%s" "$2" > "$1"
])
])

# ===========================================================================
#   https://www.gnu.org/software/autoconf-archive/ax_am_macros_static.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_AM_MACROS_STATIC
#
# DESCRIPTION
#
#   Adds support for macros that create Automake rules. You must manually
#   add the following line
#
#     include $(top_srcdir)/aminclude_static.am
#
#   to your Makefile.am files.
#
# LICENSE
#
#   Copyright (c) 2009 Tom Howard <tomhoward@users.sf.net>
#   Copyright (c) 2009 Allan Caffee <allan.caffee@gmail.com>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

serial 11

AC_DEFUN([AMINCLUDE_STATIC],[aminclude_static.am])

AC_DEFUN([AX_AM_MACROS_STATIC],
[
AX_AC_PRINT_TO_FILE(AMINCLUDE_STATIC,[
# ]AMINCLUDE_STATIC[ generated automatically by Autoconf
# from AX_AM_MACROS_STATIC on ]m4_esyscmd([LC_ALL=C date])[
])
])

# ===========================================================================
#   https://www.gnu.org/software/autoconf-archive/ax_ac_append_to_file.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_AC_APPEND_TO_FILE([FILE],[DATA])
#
# DESCRIPTION
#
#   Appends the specified data to the specified Autoconf is run. If you want
#   to append to a file when configure is run use AX_APPEND_TO_FILE instead.
#
# LICENSE
#
#   Copyright (c) 2009 Allan Caffee <allan.caffee@gmail.com>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

serial 10

AC_DEFUN([AX_AC_APPEND_TO_FILE],[
AC_REQUIRE([AX_FILE_ESCAPES])
m4_esyscmd(
AX_FILE_ESCAPES
[
printf "%s" "$2" >> "$1"
])
])

# ===========================================================================
#  https://www.gnu.org/software/autoconf-archive/ax_add_am_macro_static.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_ADD_AM_MACRO_STATIC([RULE])
#
# DESCRIPTION
#
#   Adds the specified rule to $AMINCLUDE.
#
# LICENSE
#
#   Copyright (c) 2009 Tom Howard <tomhoward@users.sf.net>
#   Copyright (c) 2009 Allan Caffee <allan.caffee@gmail.com>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

serial 8

AC_DEFUN([AX_ADD_AM_MACRO_STATIC],[
  AC_REQUIRE([AX_AM_MACROS_STATIC])
  AX_AC_APPEND_TO_FILE(AMINCLUDE_STATIC,[$1])
])

# ===========================================================================
#    https://www.gnu.org/software/autoconf-archive/ax_check_gnu_make.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_CHECK_GNU_MAKE([run-if-true],[run-if-false])
#
# DESCRIPTION
#
#   This macro searches for a GNU version of make. If a match is found:
#
#     * The makefile variable `ifGNUmake' is set to the empty string, otherwise
#       it is set to "#". This is useful for including a special features in a
#       Makefile, which cannot be handled by other versions of make.
#     * The makefile variable `ifnGNUmake' is set to #, otherwise
#       it is set to the empty string. This is useful for including a special
#       features in a Makefile, which can be handled
#       by other versions of make or to specify else like clause.
#     * The variable `_cv_gnu_make_command` is set to the command to invoke
#       GNU make if it exists, the empty string otherwise.
#     * The variable `ax_cv_gnu_make_command` is set to the command to invoke
#       GNU make by copying `_cv_gnu_make_command`, otherwise it is unset.
#     * If GNU Make is found, its version is extracted from the output of
#       `make --version` as the last field of a record of space-separated
#       columns and saved into the variable `ax_check_gnu_make_version`.
#     * Additionally if GNU Make is found, run shell code run-if-true
#       else run shell code run-if-false.
#
#   Here is an example of its use:
#
#   Makefile.in might contain:
#
#     # A failsafe way of putting a dependency rule into a makefile
#     $(DEPEND):
#             $(CC) -MM $(srcdir)/*.c > $(DEPEND)
#
#     @ifGNUmake@ ifeq ($(DEPEND),$(wildcard $(DEPEND)))
#     @ifGNUmake@ include $(DEPEND)
#     @ifGNUmake@ else
#     fallback code
#     @ifGNUmake@ endif
#
#   Then configure.in would normally contain:
#
#     AX_CHECK_GNU_MAKE()
#     AC_OUTPUT(Makefile)
#
#   Then perhaps to cause gnu make to override any other make, we could do
#   something like this (note that GNU make always looks for GNUmakefile
#   first):
#
#     if  ! test x$_cv_gnu_make_command = x ; then
#             mv Makefile GNUmakefile
#             echo .DEFAULT: > Makefile ;
#             echo \  $_cv_gnu_make_command \$@ >> Makefile;
#     fi
#
#   Then, if any (well almost any) other make is called, and GNU make also
#   exists, then the other make wraps the GNU make.
#
# LICENSE
#
#   Copyright (c) 2008 John Darrington <j.darrington@elvis.murdoch.edu.au>
#   Copyright (c) 2015 Enrico M. Crisostomo <enrico.m.crisostomo@gmail.com>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

serial 12

AC_DEFUN([AX_CHECK_GNU_MAKE],dnl
  [AC_PROG_AWK
  AC_CACHE_CHECK([for GNU make],[_cv_gnu_make_command],[dnl
    _cv_gnu_make_command="" ;
dnl Search all the common names for GNU make
    for a in "$MAKE" make gmake gnumake ; do
      if test -z "$a" ; then continue ; fi ;
      if "$a" --version 2> /dev/null | grep GNU 2>&1 > /dev/null ; then
        _cv_gnu_make_command=$a ;
        AX_CHECK_GNU_MAKE_HEADLINE=$("$a" --version 2> /dev/null | grep "GNU Make")
        ax_check_gnu_make_version=$(echo ${AX_CHECK_GNU_MAKE_HEADLINE} | ${AWK} -F " " '{ print $(NF); }')
        break ;
      fi
    done ;])
dnl If there was a GNU version, then set @ifGNUmake@ to the empty string, '#' otherwise
  AS_VAR_IF([_cv_gnu_make_command], [""], [AS_VAR_SET([ifGNUmake], ["#"])],   [AS_VAR_SET([ifGNUmake], [""])])
  AS_VAR_IF([_cv_gnu_make_command], [""], [AS_VAR_SET([ifnGNUmake], [""])],   [AS_VAR_SET([ifnGNUmake], ["#"])])
  AS_VAR_IF([_cv_gnu_make_command], [""], [AS_UNSET(ax_cv_gnu_make_command)], [AS_VAR_SET([ax_cv_gnu_make_command], [${_cv_gnu_make_command}])])
  AS_VAR_IF([_cv_gnu_make_command], [""],[$2],[$1])
  AC_SUBST([ifGNUmake])
  AC_SUBST([ifnGNUmake])
])

# ===========================================================================
#     https://www.gnu.org/software/autoconf-archive/ax_code_coverage.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_CODE_COVERAGE()
#
# DESCRIPTION
#
#   Defines CODE_COVERAGE_CPPFLAGS, CODE_COVERAGE_CFLAGS,
#   CODE_COVERAGE_CXXFLAGS and CODE_COVERAGE_LIBS which should be included
#   in the CPPFLAGS, CFLAGS CXXFLAGS and LIBS/LIBADD variables of every
#   build target (program or library) which should be built with code
#   coverage support. Also add rules using AX_ADD_AM_MACRO_STATIC; and
#   $enable_code_coverage which can be used in subsequent configure output.
#   CODE_COVERAGE_ENABLED is defined and substituted, and corresponds to the
#   value of the --enable-code-coverage option, which defaults to being
#   disabled.
#
#   Test also for gcov program and create GCOV variable that could be
#   substituted.
#
#   Note that all optimization flags in CFLAGS must be disabled when code
#   coverage is enabled.
#
#   Usage example:
#
#   configure.ac:
#
#     AX_CODE_COVERAGE
#
#   Makefile.am:
#
#     include $(top_srcdir)/aminclude_static.am
#
#     my_program_LIBS = ... $(CODE_COVERAGE_LIBS) ...
#     my_program_CPPFLAGS = ... $(CODE_COVERAGE_CPPFLAGS) ...
#     my_program_CFLAGS = ... $(CODE_COVERAGE_CFLAGS) ...
#     my_program_CXXFLAGS = ... $(CODE_COVERAGE_CXXFLAGS) ...
#
#     clean-local: code-coverage-clean
#     distclean-local: code-coverage-dist-clean
#
#   This results in a "check-code-coverage" rule being added to any
#   Makefile.am which do "include $(top_srcdir)/aminclude_static.am"
#   (assuming the module has been configured with --enable-code-coverage).
#   Running `make check-code-coverage` in that directory will run the
#   module's test suite (`make check`) and build a code coverage report
#   detailing the code which was touched, then print the URI for the report.
#
#   This code was derived from Makefile.decl in GLib, originally licensed
#   under LGPLv2.1+.
#
# LICENSE
#
#   Copyright (c) 2012, 2016 Philip Withnall
#   Copyright (c) 2012 Xan Lopez
#   Copyright (c) 2012 Christian Persch
#   Copyright (c) 2012 Paolo Borelli
#   Copyright (c) 2012 Dan Winship
#   Copyright (c) 2015,2018 Bastien ROUCARIES
#
#   This library is free software; you can redistribute it and/or modify it
#   under the terms of the GNU Lesser General Public License as published by
#   the Free Software Foundation; either version 2.1 of the License, or (at
#   your option) any later version.
#
#   This library is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
#   General Public License for more details.
#
#   You should have received a copy of the GNU Lesser General Public License
#   along with this program. If not, see <https://www.gnu.org/licenses/>.

serial 34

m4_define(_AX_CODE_COVERAGE_RULES,[
AX_ADD_AM_MACRO_STATIC([
# Code coverage
#
# Optional:
#  - CODE_COVERAGE_DIRECTORY: Top-level directory for code coverage reporting.
#    Multiple directories may be specified, separated by whitespace.
#    (Default: \$(top_builddir))
#  - CODE_COVERAGE_OUTPUT_FILE: Filename and path for the .info file generated
#    by lcov for code coverage. (Default:
#    \$(PACKAGE_NAME)-\$(PACKAGE_VERSION)-coverage.info)
#  - CODE_COVERAGE_OUTPUT_DIRECTORY: Directory for generated code coverage
#    reports to be created. (Default:
#    \$(PACKAGE_NAME)-\$(PACKAGE_VERSION)-coverage)
#  - CODE_COVERAGE_BRANCH_COVERAGE: Set to 1 to enforce branch coverage,
#    set to 0 to disable it and leave empty to stay with the default.
#    (Default: empty)
#  - CODE_COVERAGE_LCOV_SHOPTS_DEFAULT: Extra options shared between both lcov
#    instances. (Default: based on $CODE_COVERAGE_BRANCH_COVERAGE)
#  - CODE_COVERAGE_LCOV_SHOPTS: Extra options to shared between both lcov
#    instances. (Default: $CODE_COVERAGE_LCOV_SHOPTS_DEFAULT)
#  - CODE_COVERAGE_LCOV_OPTIONS_GCOVPATH: --gcov-tool pathtogcov
#  - CODE_COVERAGE_LCOV_OPTIONS_DEFAULT: Extra options to pass to the
#    collecting lcov instance. (Default: $CODE_COVERAGE_LCOV_OPTIONS_GCOVPATH)
#  - CODE_COVERAGE_LCOV_OPTIONS: Extra options to pass to the collecting lcov
#    instance. (Default: $CODE_COVERAGE_LCOV_OPTIONS_DEFAULT)
#  - CODE_COVERAGE_LCOV_RMOPTS_DEFAULT: Extra options to pass to the filtering
#    lcov instance. (Default: empty)
#  - CODE_COVERAGE_LCOV_RMOPTS: Extra options to pass to the filtering lcov
#    instance. (Default: $CODE_COVERAGE_LCOV_RMOPTS_DEFAULT)
#  - CODE_COVERAGE_GENHTML_OPTIONS_DEFAULT: Extra options to pass to the
#    genhtml instance. (Default: based on $CODE_COVERAGE_BRANCH_COVERAGE)
#  - CODE_COVERAGE_GENHTML_OPTIONS: Extra options to pass to the genhtml
#    instance. (Default: $CODE_COVERAGE_GENHTML_OPTIONS_DEFAULT)
#  - CODE_COVERAGE_IGNORE_PATTERN: Extra glob pattern of files to ignore
#
# The generated report will be titled using the \$(PACKAGE_NAME) and
# \$(PACKAGE_VERSION). In order to add the current git hash to the title,
# use the git-version-gen script, available online.
# Optional variables
# run only on top dir
if CODE_COVERAGE_ENABLED
 ifeq (\$(abs_builddir), \$(abs_top_builddir))
CODE_COVERAGE_DIRECTORY ?= \$(top_builddir)
CODE_COVERAGE_OUTPUT_FILE ?= \$(PACKAGE_NAME)-\$(PACKAGE_VERSION)-coverage.info
CODE_COVERAGE_OUTPUT_DIRECTORY ?= \$(PACKAGE_NAME)-\$(PACKAGE_VERSION)-coverage

CODE_COVERAGE_BRANCH_COVERAGE ?=
CODE_COVERAGE_LCOV_SHOPTS_DEFAULT ?= \$(if \$(CODE_COVERAGE_BRANCH_COVERAGE),\
--rc lcov_branch_coverage=\$(CODE_COVERAGE_BRANCH_COVERAGE))
CODE_COVERAGE_LCOV_SHOPTS ?= \$(CODE_COVERAGE_LCOV_SHOPTS_DEFAULT)
CODE_COVERAGE_LCOV_OPTIONS_GCOVPATH ?= --gcov-tool \"\$(GCOV)\"
CODE_COVERAGE_LCOV_OPTIONS_DEFAULT ?= \$(CODE_COVERAGE_LCOV_OPTIONS_GCOVPATH)
CODE_COVERAGE_LCOV_OPTIONS ?= \$(CODE_COVERAGE_LCOV_OPTIONS_DEFAULT)
CODE_COVERAGE_LCOV_RMOPTS_DEFAULT ?=
CODE_COVERAGE_LCOV_RMOPTS ?= \$(CODE_COVERAGE_LCOV_RMOPTS_DEFAULT)
CODE_COVERAGE_GENHTML_OPTIONS_DEFAULT ?=\
\$(if \$(CODE_COVERAGE_BRANCH_COVERAGE),\
--rc genhtml_branch_coverage=\$(CODE_COVERAGE_BRANCH_COVERAGE))
CODE_COVERAGE_GENHTML_OPTIONS ?= \$(CODE_COVERAGE_GENHTML_OPTIONS_DEFAULT)
CODE_COVERAGE_IGNORE_PATTERN ?=

GITIGNOREFILES := \$(GITIGNOREFILES) \$(CODE_COVERAGE_OUTPUT_FILE) \$(CODE_COVERAGE_OUTPUT_DIRECTORY)
code_coverage_v_lcov_cap = \$(code_coverage_v_lcov_cap_\$(V))
code_coverage_v_lcov_cap_ = \$(code_coverage_v_lcov_cap_\$(AM_DEFAULT_VERBOSITY))
code_coverage_v_lcov_cap_0 = @echo \"  LCOV   --capture\" \$(CODE_COVERAGE_OUTPUT_FILE);
code_coverage_v_lcov_ign = \$(code_coverage_v_lcov_ign_\$(V))
code_coverage_v_lcov_ign_ = \$(code_coverage_v_lcov_ign_\$(AM_DEFAULT_VERBOSITY))
code_coverage_v_lcov_ign_0 = @echo \"  LCOV   --remove /tmp/*\" \$(CODE_COVERAGE_IGNORE_PATTERN);
code_coverage_v_genhtml = \$(code_coverage_v_genhtml_\$(V))
code_coverage_v_genhtml_ = \$(code_coverage_v_genhtml_\$(AM_DEFAULT_VERBOSITY))
code_coverage_v_genhtml_0 = @echo \"  GEN   \" \"\$(CODE_COVERAGE_OUTPUT_DIRECTORY)\";
code_coverage_quiet = \$(code_coverage_quiet_\$(V))
code_coverage_quiet_ = \$(code_coverage_quiet_\$(AM_DEFAULT_VERBOSITY))
code_coverage_quiet_0 = --quiet

# sanitizes the test-name: replaces with underscores: dashes and dots
code_coverage_sanitize = \$(subst -,_,\$(subst .,_,\$(1)))

# Use recursive makes in order to ignore errors during check
check-code-coverage:
	-\$(AM_V_at)\$(MAKE) \$(AM_MAKEFLAGS) -k check
	\$(AM_V_at)\$(MAKE) \$(AM_MAKEFLAGS) code-coverage-capture

# Capture code coverage data
code-coverage-capture: code-coverage-capture-hook
	\$(code_coverage_v_lcov_cap)\$(LCOV) \$(code_coverage_quiet) \$(addprefix --directory ,\$(CODE_COVERAGE_DIRECTORY)) --capture --output-file \"\$(CODE_COVERAGE_OUTPUT_FILE).tmp\" --test-name \"\$(call code_coverage_sanitize,\$(PACKAGE_NAME)-\$(PACKAGE_VERSION))\" --no-checksum --compat-libtool \$(CODE_COVERAGE_LCOV_SHOPTS) \$(CODE_COVERAGE_LCOV_OPTIONS)
	\$(code_coverage_v_lcov_ign)\$(LCOV) \$(code_coverage_quiet) \$(addprefix --directory ,\$(CODE_COVERAGE_DIRECTORY)) --remove \"\$(CODE_COVERAGE_OUTPUT_FILE).tmp\" \"/tmp/*\" \$(CODE_COVERAGE_IGNORE_PATTERN) --output-file \"\$(CODE_COVERAGE_OUTPUT_FILE)\" \$(CODE_COVERAGE_LCOV_SHOPTS) \$(CODE_COVERAGE_LCOV_RMOPTS)
	-@rm -f \"\$(CODE_COVERAGE_OUTPUT_FILE).tmp\"
	\$(code_coverage_v_genhtml)LANG=C \$(GENHTML) \$(code_coverage_quiet) \$(addprefix --prefix ,\$(CODE_COVERAGE_DIRECTORY)) --output-directory \"\$(CODE_COVERAGE_OUTPUT_DIRECTORY)\" --title \"\$(PACKAGE_NAME)-\$(PACKAGE_VERSION) Code Coverage\" --legend --show-details \"\$(CODE_COVERAGE_OUTPUT_FILE)\" \$(CODE_COVERAGE_GENHTML_OPTIONS)
	@echo \"file://\$(abs_builddir)/\$(CODE_COVERAGE_OUTPUT_DIRECTORY)/index.html\"

code-coverage-clean:
	-\$(LCOV) --directory \$(top_builddir) -z
	-rm -rf \"\$(CODE_COVERAGE_OUTPUT_FILE)\" \"\$(CODE_COVERAGE_OUTPUT_FILE).tmp\" \"\$(CODE_COVERAGE_OUTPUT_DIRECTORY)\"
	-find . \\( -name \"*.gcda\" -o -name \"*.gcno\" -o -name \"*.gcov\" \\) -delete

code-coverage-dist-clean:

A][M_DISTCHECK_CONFIGURE_FLAGS := \$(A][M_DISTCHECK_CONFIGURE_FLAGS) --disable-code-coverage
 else # ifneq (\$(abs_builddir), \$(abs_top_builddir))
check-code-coverage:

code-coverage-capture: code-coverage-capture-hook

code-coverage-clean:

code-coverage-dist-clean:
 endif # ifeq (\$(abs_builddir), \$(abs_top_builddir))
else #! CODE_COVERAGE_ENABLED
# Use recursive makes in order to ignore errors during check
check-code-coverage:
	@echo \"Need to reconfigure with --enable-code-coverage\"
# Capture code coverage data
code-coverage-capture: code-coverage-capture-hook
	@echo \"Need to reconfigure with --enable-code-coverage\"

code-coverage-clean:

code-coverage-dist-clean:

endif #CODE_COVERAGE_ENABLED
# Hook rule executed before code-coverage-capture, overridable by the user
code-coverage-capture-hook:

.PHONY: check-code-coverage code-coverage-capture code-coverage-dist-clean code-coverage-clean code-coverage-capture-hook
])
])

AC_DEFUN([_AX_CODE_COVERAGE_ENABLED],[
	AX_CHECK_GNU_MAKE([],[AC_MSG_ERROR([not using GNU make that is needed for coverage])])
	AC_REQUIRE([AX_ADD_AM_MACRO_STATIC])
	# check for gcov
	AC_CHECK_TOOL([GCOV],
		  [$_AX_CODE_COVERAGE_GCOV_PROG_WITH],
		  [:])
	AS_IF([test "X$GCOV" = "X:"],
	      [AC_MSG_ERROR([gcov is needed to do coverage])])
	AC_SUBST([GCOV])

	dnl Check if gcc is being used
	AS_IF([ test "$GCC" = "no" ], [
		AC_MSG_ERROR([not compiling with gcc, which is required for gcov code coverage])
	      ])

	AC_CHECK_PROG([LCOV], [lcov], [lcov])
	AC_CHECK_PROG([GENHTML], [genhtml], [genhtml])

	AS_IF([ test x"$LCOV" = x ], [
		AC_MSG_ERROR([To enable code coverage reporting you must have lcov installed])
	      ])

	AS_IF([ test x"$GENHTML" = x ], [
		AC_MSG_ERROR([Could not find genhtml from the lcov package])
	])

	dnl Build the code coverage flags
	dnl Define CODE_COVERAGE_LDFLAGS for backwards compatibility
	CODE_COVERAGE_CPPFLAGS="-DNDEBUG"
	CODE_COVERAGE_CFLAGS="-O0 -g -fprofile-arcs -ftest-coverage"
	CODE_COVERAGE_CXXFLAGS="-O0 -g -fprofile-arcs -ftest-coverage"
	CODE_COVERAGE_LIBS="-lgcov"

	AC_SUBST([CODE_COVERAGE_CPPFLAGS])
	AC_SUBST([CODE_COVERAGE_CFLAGS])
	AC_SUBST([CODE_COVERAGE_CXXFLAGS])
	AC_SUBST([CODE_COVERAGE_LIBS])
])

AC_DEFUN([AX_CODE_COVERAGE],[
	dnl Check for --enable-code-coverage

	# allow to override gcov location
	AC_ARG_WITH([gcov],
	  [AS_HELP_STRING([--with-gcov[=GCOV]], [use given GCOV for coverage (GCOV=gcov).])],
	  [_AX_CODE_COVERAGE_GCOV_PROG_WITH=$with_gcov],
	  [_AX_CODE_COVERAGE_GCOV_PROG_WITH=gcov])

	AC_MSG_CHECKING([whether to build with code coverage support])
	AC_ARG_ENABLE([code-coverage],
	  AS_HELP_STRING([--enable-code-coverage],
	  [Whether to enable code coverage support]),,
	  enable_code_coverage=no)

	AM_CONDITIONAL([CODE_COVERAGE_ENABLED], [test "x$enable_code_coverage" = xyes])
	AC_SUBST([CODE_COVERAGE_ENABLED], [$enable_code_coverage])
	AC_MSG_RESULT($enable_code_coverage)

	AS_IF([ test "x$enable_code_coverage" = xyes ], [
		_AX_CODE_COVERAGE_ENABLED
	      ])

	_AX_CODE_COVERAGE_RULES
])

# ===========================================================================
#    https://www.gnu.org/software/autoconf-archive/ax_compare_version.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_COMPARE_VERSION(VERSION_A, OP, VERSION_B, [ACTION-IF-TRUE], [ACTION-IF-FALSE])
#
# DESCRIPTION
#
#   This macro compares two version strings. Due to the various number of
#   minor-version numbers that can exist, and the fact that string
#   comparisons are not compatible with numeric comparisons, this is not
#   necessarily trivial to do in a autoconf script. This macro makes doing
#   these comparisons easy.
#
#   The six basic comparisons are available, as well as checking equality
#   limited to a certain number of minor-version levels.
#
#   The operator OP determines what type of comparison to do, and can be one
#   of:
#
#    eq  - equal (test A == B)
#    ne  - not equal (test A != B)
#    le  - less than or equal (test A <= B)
#    ge  - greater than or equal (test A >= B)
#    lt  - less than (test A < B)
#    gt  - greater than (test A > B)
#
#   Additionally, the eq and ne operator can have a number after it to limit
#   the test to that number of minor versions.
#
#    eq0 - equal up to the length of the shorter version
#    ne0 - not equal up to the length of the shorter version
#    eqN - equal up to N sub-version levels
#    neN - not equal up to N sub-version levels
#
#   When the condition is true, shell commands ACTION-IF-TRUE are run,
#   otherwise shell commands ACTION-IF-FALSE are run. The environment
#   variable 'ax_compare_version' is always set to either 'true' or 'false'
#   as well.
#
#   Examples:
#
#     AX_COMPARE_VERSION([3.15.7],[lt],[3.15.8])
#     AX_COMPARE_VERSION([3.15],[lt],[3.15.8])
#
#   would both be true.
#
#     AX_COMPARE_VERSION([3.15.7],[eq],[3.15.8])
#     AX_COMPARE_VERSION([3.15],[gt],[3.15.8])
#
#   would both be false.
#
#     AX_COMPARE_VERSION([3.15.7],[eq2],[3.15.8])
#
#   would be true because it is only comparing two minor versions.
#
#     AX_COMPARE_VERSION([3.15.7],[eq0],[3.15])
#
#   would be true because it is only comparing the lesser number of minor
#   versions of the two values.
#
#   Note: The characters that separate the version numbers do not matter. An
#   empty string is the same as version 0. OP is evaluated by autoconf, not
#   configure, so must be a string, not a variable.
#
#   The author would like to acknowledge Guido Draheim whose advice about
#   the m4_case and m4_ifvaln functions make this macro only include the
#   portions necessary to perform the specific comparison specified by the
#   OP argument in the final configure script.
#
# LICENSE
#
#   Copyright (c) 2008 Tim Toolan <toolan@ele.uri.edu>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

serial 13

dnl #########################################################################
AC_DEFUN([AX_COMPARE_VERSION], [
  AC_REQUIRE([AC_PROG_AWK])

  # Used to indicate true or false condition
  ax_compare_version=false

  # Convert the two version strings to be compared into a format that
  # allows a simple string comparison.  The end result is that a version
  # string of the form 1.12.5-r617 will be converted to the form
  # 0001001200050617.  In other words, each number is zero padded to four
  # digits, and non digits are removed.
  AS_VAR_PUSHDEF([A],[ax_compare_version_A])
  A=`echo "$1" | sed -e 's/\([[0-9]]*\)/Z\1Z/g' \
                     -e 's/Z\([[0-9]]\)Z/Z0\1Z/g' \
                     -e 's/Z\([[0-9]][[0-9]]\)Z/Z0\1Z/g' \
                     -e 's/Z\([[0-9]][[0-9]][[0-9]]\)Z/Z0\1Z/g' \
                     -e 's/[[^0-9]]//g'`

  AS_VAR_PUSHDEF([B],[ax_compare_version_B])
  B=`echo "$3" | sed -e 's/\([[0-9]]*\)/Z\1Z/g' \
                     -e 's/Z\([[0-9]]\)Z/Z0\1Z/g' \
                     -e 's/Z\([[0-9]][[0-9]]\)Z/Z0\1Z/g' \
                     -e 's/Z\([[0-9]][[0-9]][[0-9]]\)Z/Z0\1Z/g' \
                     -e 's/[[^0-9]]//g'`

  dnl # In the case of le, ge, lt, and gt, the strings are sorted as necessary
  dnl # then the first line is used to determine if the condition is true.
  dnl # The sed right after the echo is to remove any indented white space.
  m4_case(m4_tolower($2),
  [lt],[
    ax_compare_version=`echo "x$A
x$B" | sed 's/^ *//' | sort -r | sed "s/x${A}/false/;s/x${B}/true/;1q"`
  ],
  [gt],[
    ax_compare_version=`echo "x$A
x$B" | sed 's/^ *//' | sort | sed "s/x${A}/false/;s/x${B}/true/;1q"`
  ],
  [le],[
    ax_compare_version=`echo "x$A
x$B" | sed 's/^ *//' | sort | sed "s/x${A}/true/;s/x${B}/false/;1q"`
  ],
  [ge],[
    ax_compare_version=`echo "x$A
x$B" | sed 's/^ *//' | sort -r | sed "s/x${A}/true/;s/x${B}/false/;1q"`
  ],[
    dnl Split the operator from the subversion count if present.
    m4_bmatch(m4_substr($2,2),
    [0],[
      # A count of zero means use the length of the shorter version.
      # Determine the number of characters in A and B.
      ax_compare_version_len_A=`echo "$A" | $AWK '{print(length)}'`
      ax_compare_version_len_B=`echo "$B" | $AWK '{print(length)}'`

      # Set A to no more than B's length and B to no more than A's length.
      A=`echo "$A" | sed "s/\(.\{$ax_compare_version_len_B\}\).*/\1/"`
      B=`echo "$B" | sed "s/\(.\{$ax_compare_version_len_A\}\).*/\1/"`
    ],
    [[0-9]+],[
      # A count greater than zero means use only that many subversions
      A=`echo "$A" | sed "s/\(\([[0-9]]\{4\}\)\{m4_substr($2,2)\}\).*/\1/"`
      B=`echo "$B" | sed "s/\(\([[0-9]]\{4\}\)\{m4_substr($2,2)\}\).*/\1/"`
    ],
    [.+],[
      AC_WARNING(
        [invalid OP numeric parameter: $2])
    ],[])

    # Pad zeros at end of numbers to make same length.
    ax_compare_version_tmp_A="$A`echo $B | sed 's/./0/g'`"
    B="$B`echo $A | sed 's/./0/g'`"
    A="$ax_compare_version_tmp_A"

    # Check for equality or inequality as necessary.
    m4_case(m4_tolower(m4_substr($2,0,2)),
    [eq],[
      test "x$A" = "x$B" && ax_compare_version=true
    ],
    [ne],[
      test "x$A" != "x$B" && ax_compare_version=true
    ],[
      AC_WARNING([invalid OP parameter: $2])
    ])
  ])

  AS_VAR_POPDEF([A])dnl
  AS_VAR_POPDEF([B])dnl

  dnl # Execute ACTION-IF-TRUE / ACTION-IF-FALSE.
  if test "$ax_compare_version" = "true" ; then
    m4_ifvaln([$4],[$4],[:])dnl
    m4_ifvaln([$5],[else $5])dnl
  fi
]) dnl AX_COMPARE_VERSION
