# src/bin/pg_waldump/nls.mk
CATALOG_NAME     = pg_waldump
AVAIL_LANGUAGES  = cs de el es fr ja ko ru sv tr uk vi zh_CN
GETTEXT_FILES    = $(FRONTEND_COMMON_GETTEXT_FILES) pg_waldump.c
GETTEXT_TRIGGERS = $(FRONTEND_COMMON_GETTEXT_TRIGGERS)
GETTEXT_FLAGS    = $(FRONTEND_COMMON_GETTEXT_FLAGS)
