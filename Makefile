# pg_quota extension Makefile

MODULE_big = pg_quota

EXTENSION = pg_quota
DATA = pg_quota--1.0.sql
PGFILEDESC = "pg_quota extension"

OBJS = pg_quota.o enforcement.o fs_model.o

REGRESS = test_quotas
REGRESS_OPTS = --temp-config=quota_test.conf --load-extension=pg_quota
# Disabled because these tests require setting shared_preload_libraries.
NO_INSTALLCHECK = 1

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# FIXME: This overrides the 'check' target from the pgxs makefile.
# That produces a warning: overriding recipe for target 'check'.
# I don't how to do this better. We could use a different makefile
# target, but 'check' is the canonical name for this.
check:
	$(pg_regress_check) $(REGRESS_OPTS) $(REGRESS)  --dbname=quotatestdb
