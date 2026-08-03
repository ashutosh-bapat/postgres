#-------------------------------------------------------------------------
#
# Makefile for src/test/buffermgr
#
# Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/buffermgr/Makefile
#
#-------------------------------------------------------------------------

EXTRA_INSTALL = contrib/pg_buffercache \
                                contrib/amcheck \
                                src/test/modules/injection_points \
                                src/test/modules/test_shmem

EXTENSION = buffermgr_test
DATA = buffermgr_test--1.0.sql

REGRESS = buffer_resize

# Custom configuration for buffer manager tests
TEMP_CONFIG = $(srcdir)/buffermgr_test.conf

export enable_injection_points

subdir = src/test/buffermgr
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean:
	rm -rf tmp_check
