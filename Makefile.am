bin_PROGRAMS = jpt-control
lib_LTLIBRARIES = libjpt.la
include_HEADERS = jpt.h

noinst_PROGRAMS = stress-test

AM_CFLAGS = -g -Wall -pthread
AM_CPPFLAGS = -D_XOPEN_SOURCE=500 -D_GNU_SOURCE -D_REENTRANT -D_FILE_OFFSET_BITS=64 -Ilibjpt

jpt_control_SOURCES = jpt-control.c
jpt_control_LDADD = -ljpt

stress_test_SOURCES = stress-test.c
stress_test_LDADD = -ljpt

libjpt_la_SOURCES = libjpt/jpt_backup.c libjpt/jpt_disktable.c \
	libjpt/jpt_internal.h libjpt/jpt_memtable.c libjpt/jpt_io.c libjpt/jpt.c \
	libjpt/patricia.c libjpt/patricia.h
