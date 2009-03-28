bin_PROGRAMS = jpt-control djptd
lib_LTLIBRARIES = libjpt.la libdjpt.la
include_HEADERS = libjpt/jpt.h djpt/djpt.h

noinst_PROGRAMS = stress-test
noinst_LTLIBRARIES = libjpt-common.la libdjpt-common.la

SUBDIRS = tests $(MAYBE_PHP)

AM_CFLAGS = -g -Wall -pthread
AM_CPPFLAGS = -D_XOPEN_SOURCE=500 -D_GNU_SOURCE -D_REENTRANT -D_FILE_OFFSET_BITS=64 -DBINDIR=\"$(bindir)\" -I$(srcdir)/libjpt

jpt_control_SOURCES = jpt-control.c
jpt_control_LDADD = libjpt.la

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
