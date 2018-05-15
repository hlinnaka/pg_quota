/* -------------------------------------------------------------------------
 *
 * pg_quota.c
 *		Background worker that tracks disk space usage.
 *
 * This file contains the code for initialization of the module, and
 * the background worker's main loop. We launch one background worker for
 * each database.
 *
 * Copyright (c) 2013-2018, PostgreSQL Global Development Group
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type_d.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/relfilenodemap.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#include "pg_quota.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		pg_quota_worker_main(Datum) pg_attribute_noreturn();

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int	pg_quota_refresh_naptime = 10;
static int	pg_quota_restart_interval = 5;
static char	*pg_quota_databases = "postgres";

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_quota_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pg_quota_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Load quotas from configuration table.
 */
static void
load_quotas(void)
{
	int			ret;
	TupleDesc	tupdesc;
	int			i;
	RangeVar   *rv;
	Relation	rel;

	rv = makeRangeVar("quota", "config", -1);
	rel = heap_openrv_extended(rv, AccessShareLock, true);
	if (!rel)
	{
		/* configuration table is missing. */
		elog(LOG, "configuration table \"pg_quota.quotas\" is missing in database \"%s\"",
			 get_database_name(MyDatabaseId));
		return;
	}

	ret = SPI_execute("select roleid, quota int8 from quota.config", true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != 2 ||
		TupleDescAttr(tupdesc, 0)->atttypid != OIDOID ||
		TupleDescAttr(tupdesc, 1)->atttypid != INT8OID)
		elog(ERROR, "query must yield two columns, oid and int8");

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		Datum		dat;
		Oid			roleid;
		int64		quota;
		bool		isnull;

		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (isnull)
			continue;
		roleid = DatumGetObjectId(dat);

		dat = SPI_getbinval(tup, tupdesc, 2, &isnull);
		if (isnull)
			continue;
		quota = DatumGetInt64(dat);

		/* Update the model with this */
		UpdateQuota(roleid, quota);
	}

	heap_close(rel, NoLock);
}

/*
 * get_relfilenode_owner
 *
 *		Returns the owner OID associated with a given relation.
 */
Oid
get_relfilenode_owner(RelFileNode *rnode)
{
	Oid			relid;
	HeapTuple	tp;

	Assert(rnode->dbNode == MyDatabaseId);
	relid = RelidByRelfilenode(rnode->spcNode, rnode->relNode);
	if (!OidIsValid(relid))
	{
		elog(DEBUG1, "could not find pg_class entry for relation %u/%u/%u",
			 rnode->dbNode, rnode->spcNode, rnode->relNode);
		return InvalidOid;
	}

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
		Oid			result;

		result = reltup->relowner;
		ReleaseSysCache(tp);
		return result;
	}
	else
	{
		elog(DEBUG1, "could not find owner for relation %u", relid);
		return InvalidOid;
	}
}

/*
 * Main entry point for the background worker.
 */
void
pg_quota_worker_main(Datum main_arg)
{
	char	   *dbname = MyBgworkerEntry->bgw_extra;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_quota_sighup);
	pqsignal(SIGTERM, pg_quota_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(dbname, NULL, 0);

	elog(LOG, "%s initialized",
		 MyBgworkerEntry->bgw_name);

	/*
	 * Initialize the model and set the latch to refresh the model for the first
	 * time without waiting.
	 */
	init_fs_model();
	SetLatch(MyLatch);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   pg_quota_refresh_naptime * 1000L,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Rescan the data directory.
		 */
		pgstat_report_activity(STATE_RUNNING, "scanning datadir");
		refresh_fs_model();

		/*
		 * Start a transaction on which we can run queries.  Note that each
		 * StartTransactionCommand() call should be preceded by a
		 * SetCurrentStatementStartTimestamp() call, which sets both the time
		 * for the statement we're about the run, and also the transaction
		 * start time.  Also, each other query sent to SPI should probably be
		 * preceded by SetCurrentStatementStartTimestamp(), so that statement
		 * start time is always up to date.
		 *
		 * The SPI_connect() call lets us run queries through the SPI manager,
		 * and the PushActiveSnapshot() call creates an "active" snapshot
		 * which is necessary for queries to have MVCC data to work on.
		 *
		 * The pgstat_report_activity() call makes our activity visible
		 * through the pgstat views.
		 */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		pgstat_report_activity(STATE_RUNNING, "scanning pg_class");

		/*
		 * If there are any relfilenodes for which we don't know the owner, look
		 * them up.
		 */
		UpdateOrphans();

		pgstat_report_activity(STATE_RUNNING, "loading quota configuration");
		load_quotas();

		/*
		 * And finish our transaction.
		 */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_stat(false);
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	proc_exit(1);
}

/*
 * Entrypoint of this module.
 *
 * Register the background workers for each database that uses quotas.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;
	ListCell   *lc;
	char	   *dbstr;
	List	   *dblist;

	/* This initialization must happen at postmaster startup. */
	if (!process_shared_preload_libraries_in_progress)
		return;

	init_fs_model_shmem();
	init_quota_enforcement();

	/* Get the configuration */
	DefineCustomIntVariable("pg_quota.refresh_naptime",
							"Duration between each full scan of datadir (in seconds).",
							NULL,
							&pg_quota_refresh_naptime,
							5,
							1,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_quota.restart_interval",
							"How long to wait aftera a worker crash before restart (in seconds).",
							NULL,
							&pg_quota_restart_interval,
							BGW_DEFAULT_RESTART_INTERVAL,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	/*
	 * we'd really want this to be GUC_LIST_QUOTE, but alas, an extension cannot
	 * use that.
	 */
	DefineCustomStringVariable("pg_quota.databases",
							   "List of databases to enforce quotas for.",
							   NULL,
							   &pg_quota_databases,
							   "postgres",
							   PGC_POSTMASTER, GUC_LIST_INPUT,
							   NULL,
							   NULL,
							   NULL);

	/* set up common data for all our workers */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = pg_quota_restart_interval;
	sprintf(worker.bgw_library_name, "pg_quota");
	sprintf(worker.bgw_function_name, "pg_quota_worker_main");
	worker.bgw_notify_pid = 0;

	/*
	 * Now register a background worker for each database listed in
	 * pg_quota.databases.
	 */

	/* Need a modifiable copy of namespace_search_path string */
	dbstr = pstrdup(pg_quota_databases);

	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(dbstr, ',', &dblist))
	{
		/* syntax error in database name list */
		/* XXX ereport */
		elog(ERROR, "invalid list syntax in pg_quota.databases setting");
	}

	foreach(lc, dblist)
	{
		char	   *dbname = (char *) lfirst(lc);

		snprintf(worker.bgw_name, BGW_MAXLEN, "pg_quota worker for \"%s\"", dbname);
		snprintf(worker.bgw_type, BGW_MAXLEN, "pg_quota worker");
		snprintf(worker.bgw_extra, BGW_EXTRALEN, "%s", dbname);

		RegisterBackgroundWorker(&worker);
	}
}
