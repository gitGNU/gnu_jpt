check_PROGRAMS = \
  test-00 \
  test-01 \
  test-backup-00 \
  test-column-scan-00 \
  test-journal-00 \
  test-scan-00

EXTRA_DIST = common.h

AM_CPPFLAGS = -I$(srcdir)/../libjpt/
AM_CFLAGS = -g
LDADD = ../libjpt.la

TESTS = $(check_PROGRAMS)
