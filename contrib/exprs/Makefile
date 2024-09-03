# contrib/exprs/Makefile

MODULE_big = exprs
OBJS = \
	$(WIN32RES) \
	exprs.o \

EXTENSION = exprs
DATA = exprs--1.1.sql
PGFILEDESC = "exprs - contrib module for running experiments"

REGRESS = exprs

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/exprs
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
