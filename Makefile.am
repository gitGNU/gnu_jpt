bin_PROGRAMS = jpt-control
lib_LTLIBRARIES = libjpt.la
include_HEADERS = jpt.h

noinst_PROGRAMS = stress-test

AM_CFLAGS = -g -Wall -pthread
AM_CPPFLAGS = -D_XOPEN_SOURCE=500 -D_GNU_SOURCE -D_REENTRANT -D_FILE_OFFSET_BITS=64

jpt_control_SOURCES = jpt-control.c
jpt_control_LDADD = -ljpt

stress_test_SOURCES = stress-test.c
stress_test_LDADD = -ljpt

libjpt_la_SOURCES = jpt_backup.c jpt_disktable.c jpt_internal.h jpt_memtable.c jpt_io.c jpt.c patricia.c patricia.h
