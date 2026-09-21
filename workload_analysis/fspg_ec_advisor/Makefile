EXTENSION = fspg_ec_advisor
DATA = fspg_ec_advisor--1.0.sql
PYTHON ?= python3
.DEFAULT_GOAL := all

.PHONY: python-test
python-test:
	$(PYTHON) -m unittest discover -s dispatcher/tests -v

ifeq ($(strip $(MAKECMDGOALS)),python-test)
else
PG_CONFIG ?= pg_config
PG_CONFIG_PATH := $(shell command -v $(PG_CONFIG) 2>/dev/null)
PG_SOURCE_ROOT_FROM_PWD := $(abspath $(PWD)/../..)

# A shell entered through the contrib symlink retains its logical PWD even
# though GNU Make resolves CURDIR to this checkout's physical path.
ifneq ($(wildcard $(PG_SOURCE_ROOT_FROM_PWD)/src/Makefile.global),)
PG_SOURCE_ROOT ?= $(PG_SOURCE_ROOT_FROM_PWD)
else
PG_SOURCE_ROOT ?= $(abspath $(dir $(PG_CONFIG_PATH))/../..)
endif

REGRESS = fspg_ec_advisor fspg_ec_advisor_config
REGRESS_CONFIG = test/fspg_ec_advisor.conf
EXTRA_INSTALL = contrib/pg_stat_statements

# Citus is optional at runtime. The local PostgreSQL source tree includes it,
# so make check covers those branches automatically. Set WITH_CITUS_TESTS=1 or
# 0 to override auto-detection in other environments.
WITH_CITUS_TESTS ?= $(if $(wildcard $(PG_SOURCE_ROOT)/contrib/citus/src/backend/distributed/citus.control),1,0)
ifneq ($(filter 1 yes true,$(WITH_CITUS_TESTS)),)
REGRESS += fspg_ec_advisor_citus
REGRESS_CONFIG = test/fspg_ec_advisor_citus.conf
EXTRA_INSTALL += contrib/citus
endif

REGRESS_OPTS = --temp-config $(REGRESS_CONFIG)

# Through the contrib symlink, use PostgreSQL's in-tree PGXS implementation and
# its temporary-install `make check` target. Set USE_PGXS=1 to force installed
# PGXS for standalone package builds and `make installcheck`.
ifneq ($(filter 1 yes true,$(USE_PGXS)),)
PGXS ?= $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else ifneq ($(wildcard $(PG_SOURCE_ROOT)/src/Makefile.global),)
subdir = contrib/fspg_ec_advisor
top_builddir = $(PG_SOURCE_ROOT)
top_srcdir = $(PG_SOURCE_ROOT)
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PGXS ?= $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif
endif