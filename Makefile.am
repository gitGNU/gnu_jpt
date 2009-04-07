bin_PROGRAMS = jpt-control djpt-control djptd
lib_LTLIBRARIES = libjpt.la libdjpt.la
include_HEADERS = libjpt/jpt.h djpt/djpt.h

noinst_PROGRAMS = stress-test
noinst_LTLIBRARIES = libjpt-common.la libdjpt-common.la

SUBDIRS = tests $(MAYBE_PHP)
DIST_SUBDIRS = tests

EXTRA_DIST =  \
  php/Makefile.fragments \
  php/Makefile.global \
  php/Makefile.objects \
  php/acinclude.m4 \
  php/aclocal.m4 \
  php/build/shtool \
  php/build/mkdep.awk \
  php/build/scan_makefile_in.awk \
  php/build/libtool.m4 \
  php/config.guess \
  php/config.h.in \
  php/config.m4 \
  php/config.nice \
  php/config.status \
  php/config.sub \
  php/configure \
  php/configure.in \
  php/install-sh \
  php/libtool \
  php/ltmain.sh \
  php/missing \
  php/mkinstalldirs \
  php/package.xml \
  php/php-djpt.c \
  php/php-djpt.h \
  php/run-tests.php \
  php/tests/djpt-init.phpt

AM_CFLAGS = -g -Wall -pthread
AM_CPPFLAGS = -D_XOPEN_SOURCE=500 -D_GNU_SOURCE -D_REENTRANT -D_FILE_OFFSET_BITS=64 -DBINDIR=\"$(bindir)\" -I$(srcdir)/libjpt -I$(srcdir)/djpt

jpt_control_SOURCES = jpt-control.c
jpt_control_LDADD = libjpt.la

djpt_control_SOURCES = djpt-control.c
djpt_control_LDADD = libdjpt.la

stress_test_SOURCES = stress-test.c
stress_test_LDADD = libjpt.la

libjpt_la_SOURCES = 

libjpt_common_la_SOURCES = \
	libjpt/backup.c libjpt/disktable.c libjpt/jpt_internal.h \
	libjpt/memtable.c libjpt/io.c libjpt/jpt.c libjpt/patricia.c \
	libjpt/patricia.h libjpt/script.c

libjpt_la_LDFLAGS = -no-undefined -version-info 1:0:1
libjpt_la_LIBADD = libjpt-common.la

libdjpt_common_la_SOURCES = djpt/djpt_common.c 
libdjpt_la_SOURCES = djpt/djpt.c djpt/djpt_internal.h djpt/djpt.h
djptd_SOURCES = djpt/djpt_internal.h djpt/djptd.c

djptd_LDADD = libdjpt-common.la libjpt-common.la
libdjpt_la_LIBADD = libdjpt-common.la -ljpt

libdjpt_la_LDFLAGS = -no-undefined -version-info 0:0:0

.x.c:
	ctanglex $<
