# contrib/palloc_test/Makefile

MODULE_big = palloc_test
OBJS = \
	$(WIN32RES) \
	palloc_test.o \

EXTENSION = palloc_test
DATA = palloc_test--1.1.sql
PGFILEDESC = "palloc_test - performance test of memory contexts"

REGRESS = palloc_test 

#SHLIB_LINK += $(filter -lxslt, $(LIBS)) -lxml2

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/palloc_test
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
