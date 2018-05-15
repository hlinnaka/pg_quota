/* -------------------------------------------------------------------------
 *
 * fs_model.c
 *		In-memory data structures to track disk space usage of all relations.
 *
 * Copyright (c) 2013-2018, PostgreSQL Global Development Group
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"
#include "catalog/pg_tablespace_d.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "pg_quota.h"

#define MAX_DB_ROLE_ENTRIES 1024

PG_FUNCTION_INFO_V1(get_quota_status);

typedef struct FileSizeEntry FileSizeEntry;
typedef struct RelSizeEntry RelSizeEntry;
typedef struct RoleSizeEntry RoleSizeEntry;
typedef struct RoleSizeEntryKey RoleSizeEntryKey;

/*
 * Shared memory structure.
 *
 * In shared memory, we keep a hash table of RoleSizeEntrys. It's keyed by
 * role and database OID, and protected by shared->lock. It holds the
 * current total disk space usage, and quota, for each role and database.
 */
struct RoleSizeEntryKey
{
	/* hash key consists of role and database OID */
	Oid			rolid;
	Oid			dbid;
};

struct RoleSizeEntry
{
	RoleSizeEntryKey key;

	off_t		totalsize;	/* current total space usage */
	int64		quota;		/* quota from config table, or -1 for no quota */
};

static HTAB *role_totals_map;

typedef struct
{
	LWLock	   *lock;		/* protects role_totals_map */
} pg_quota_shared_state;

static pg_quota_shared_state *shared;

/*
 * Local memory structures, in the background worker process.
 *
 * There are two hash tables, to track every relation and the files belonging
 * to them.
 *
 * The first hash table, path_to_fsentry_map, contains one FileSizeEntry for
 * every relation file in the data directory. It holds the current size of each
 * file.
 *
 * The second hash table contains one RelSizeEntry for each relation. It holds
 * the owner of each relation.
 *
 * Each background worker only tracks files belonging to the database the worker
 * is assigned to.
 */
struct FileSizeEntry
{
	char		path[MAXPGPATH]; /* XXX: this is overly large for a hash key */

	off_t		filesize;		/* current size of the file. */

	RelSizeEntry *parent;		/* relation this file belongs to. */

	int			generation;		/* generation stamp, to detect removed files */
};

static HTAB *path_to_fsentry_map;

struct RelSizeEntry
{
	RelFileNode rnode;

	Oid			owner;

	int			numfiles;		/* ref count of FileSizeEntrys for this rel */
	off_t		totalsize;

	dlist_node	orphan_node;	/* link in orphanRels, if owner == InvalidOid */
};

static HTAB *relfilenode_to_relentry_map;

/* List of RelSizeEntrys without owner. */
static dlist_head orphanRels;

/* Memory context to hold the in-memory model. */
static MemoryContext FsModelContext;

/*
 * Current "generation", used to detect entries for files that have been
 * deleted.
 */
static int generation;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static Size pg_quota_memsize(void);
static void pg_quota_shmem_startup(void);

static bool isRelDataFile(const char *path, RelFileNode *rnode);
static void RemoveFileSize(FileSizeEntry *fsentry);
static void UpdateFileSize(RelFileNode *rnode, char *filename, off_t newsize);

/*
 * Does it look like a relation data file?
 *
 * Returns the relfilenode in *rnode, if so.
 *
 * Adapted from pg_rewind's similar function.
 */
static bool
isRelDataFile(const char *path, RelFileNode *rnode)
{
	int			nmatch;
	bool		matched;

	/*----
	 * Relation data files can be in one of the following directories:
	 *
	 * global/
	 *		shared relations
	 *
	 * base/<db oid>/
	 *		regular relations, default tablespace
	 *
	 * pg_tblspc/<tblspc oid>/<tblspc version>/
	 *		within a non-default tablespace (the name of the directory
	 *		depends on version)
	 *
	 * And the relation data files themselves have a filename like:
	 *
	 * <oid>.<segment number>
	 *
	 * We don't care about the segment number here.
	 *
	 *----
	 */
	rnode->spcNode = InvalidOid;
	rnode->dbNode = InvalidOid;
	rnode->relNode = InvalidOid;
	matched = false;

	nmatch = sscanf(path, "global/%u", &rnode->relNode);
	if (nmatch == 1)
	{
		rnode->spcNode = GLOBALTABLESPACE_OID;
		rnode->dbNode = InvalidOid;
		matched = true;
	}
	else
	{
		nmatch = sscanf(path, "base/%u/%u",
						&rnode->dbNode, &rnode->relNode);
		if (nmatch == 2)
		{
			rnode->spcNode = DEFAULTTABLESPACE_OID;
			matched = true;
		}
		else
		{
			nmatch = sscanf(path, "pg_tblspc/%u/" TABLESPACE_VERSION_DIRECTORY "/%u/%u",
							&rnode->spcNode, &rnode->dbNode, &rnode->relNode);
			if (nmatch == 3)
				matched = true;
		}
	}

	/*
	 * Note: The sscanf tests above can match files that have extra
	 * characters at the end. Non-main forks, and non-0 segments in
	 * particular. We'll count them all as part of the relation.
	 */

	return matched;
}

/*
 * Per-worker initialization. Create local hashes.
 */
void
init_fs_model(void)
{
	HASHCTL		hash_ctl;
	HASH_SEQ_STATUS iter;
	RoleSizeEntry *rolentry;

	if (FsModelContext)
		MemoryContextDelete(FsModelContext);

	FsModelContext = AllocSetContextCreate(TopMemoryContext,
										   "Disk quotas FS model context",
										   ALLOCSET_DEFAULT_SIZES);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = MAXPGPATH;
	hash_ctl.entrysize = sizeof(FileSizeEntry);
	hash_ctl.hcxt = FsModelContext;

	path_to_fsentry_map = hash_create("path to FileSizeEntry map",
									  1024,
									  &hash_ctl,
									  HASH_ELEM | HASH_CONTEXT);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(RelFileNode);
	hash_ctl.entrysize = sizeof(RelSizeEntry);
	hash_ctl.hcxt = FsModelContext;

	relfilenode_to_relentry_map =
	  hash_create("relfilenode to RelSizeEntry map",
				  1024,
				  &hash_ctl,
				  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	memset(&orphanRels, 0, sizeof(orphanRels));

	/*
	 * Remove any old entries for this database from the shared memory hash
	 * table, in case an old worker died and left them behind.
	 */
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, role_totals_map);

	while ((rolentry = hash_seq_search(&iter)) != NULL)
	{
		/* only reset entries for current db */
		if (rolentry->key.dbid == MyDatabaseId)
		{
			(void) hash_search(role_totals_map,
							   (void *) rolentry,
							   HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(shared->lock);
}

void
init_fs_model_shmem(void)
{
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgss_shmem_startup().
	 */
	RequestAddinShmemSpace(pg_quota_memsize());
	RequestNamedLWLockTranche("pg_quota", 1);

	/*
	 * Install startup hook to initialize our shared memory.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_quota_shmem_startup;
}

/* Estimate shared memory space needed. */
static Size
pg_quota_memsize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(pg_quota_shared_state));
	size = add_size(size, hash_estimate_size(MAX_DB_ROLE_ENTRIES,
											 sizeof(RoleSizeEntry)));
	return size;
}

/*
 * Initialize shared memory.
 */
static void
pg_quota_shmem_startup(void)
{
	HASHCTL		hash_ctl;
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	shared = NULL;
	role_totals_map = NULL;

	/*
	 * The RoleSizeEntry hash table is kept in shared memory, so that backends
	 * can do lookups in it.
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	shared = ShmemInitStruct("pg_quota",
							 sizeof(pg_quota_shared_state),
							 &found);
	if (!found)
	{
		shared->lock = &(GetNamedLWLockTranche("pg_quota"))->lock;
	}

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(RoleSizeEntryKey);
	hash_ctl.entrysize = sizeof(RoleSizeEntry);
	role_totals_map = ShmemInitHash("role OID to RoleSizeEntry map",
									MAX_DB_ROLE_ENTRIES,
									MAX_DB_ROLE_ENTRIES,
									&hash_ctl,
									HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}

static void
RemoveFileSize(FileSizeEntry *fsentry)
{
	RelSizeEntry *relentry = fsentry->parent;
	int64		filesize = fsentry->filesize;
	Oid			owner = relentry->owner;
	bool		found;

	/* Remove the FileSizeEntry. */
	(void) hash_search(path_to_fsentry_map,
					   (void *) fsentry->path,
					   HASH_REMOVE, &found);
	Assert(found);

	/*
	 * Update the parent relation. If this was the last file of this relation,
	 * remove the entry for the relation altogether.
	 */
	relentry->totalsize -= filesize;
	relentry->numfiles--;
	if (relentry->numfiles == 0)
	{
		Assert(relentry->totalsize == 0);
		if (relentry->owner == InvalidOid)
			dlist_delete(&relentry->orphan_node);
		(void) hash_search(relfilenode_to_relentry_map,
						   (void *) relentry,
						   HASH_REMOVE, &found);
		Assert(found);
	}

	/*
	 * If we know the owner of this file, update its totals too.
	 */
	if (OidIsValid(owner) && filesize != 0)
	{
		RoleSizeEntry *rolentry;
		RoleSizeEntryKey key;

		LWLockAcquire(shared->lock, LW_SHARED);

		key.rolid = owner;
		key.dbid = MyDatabaseId;
		rolentry = (RoleSizeEntry *) hash_search(role_totals_map,
												 (void *) &key,
												 HASH_FIND, NULL);
		if (rolentry)
			rolentry->totalsize -= filesize;
		else
		{
			/* shouldn't happen */
			elog(DEBUG1, "role total not found, corrupt map?");
		}

		LWLockRelease(shared->lock);
	}
}

/*
 * Update the model with the size of one file.
 */
static void
UpdateFileSize(RelFileNode *rnode, char *path, off_t newsize)
{
	RelSizeEntry *relentry;
	FileSizeEntry *fsentry;
	bool		found;
	off_t		oldsize;

	/* Find or create entry for this relation */
	relentry = (RelSizeEntry *) hash_search(relfilenode_to_relentry_map,
											(void *) rnode,
											HASH_ENTER, &found);
	if (!found)
	{
		relentry->owner = InvalidOid;
		dlist_push_head(&orphanRels, &relentry->orphan_node);

		relentry->numfiles = 0;
		relentry->totalsize = 0;
	}

	/* Find or create entry for this file */
	fsentry = (FileSizeEntry *) hash_search(path_to_fsentry_map,
											(void *) path,
											HASH_ENTER, &found);
	if (!found)
	{
		fsentry->parent = relentry;
		relentry->numfiles++;
		fsentry->filesize = 0;
	}
	Assert(relentry->numfiles > 0);
	Assert(fsentry->parent == relentry);

	/* Update file size */
	oldsize = fsentry->filesize;
	fsentry->filesize = newsize;

	/* also touch 'generation', to remember that we saw this file to exist */
	fsentry->generation = generation;

	/*
	 * If the file size changed, must also update the totals for the relation
	 * and the owner.
	 */
	if (newsize != oldsize)
	{
		relentry->totalsize += (newsize - oldsize);

		if (relentry->owner)
		{
			RoleSizeEntry *rolentry;
			RoleSizeEntryKey key;

			LWLockAcquire(shared->lock, LW_EXCLUSIVE);

			key.rolid = relentry->owner;
			key.dbid = MyDatabaseId;
			rolentry = (RoleSizeEntry *) hash_search(role_totals_map,
													 (void *) &key,
													 HASH_ENTER, &found);
			if (!found)
			{
				rolentry->totalsize = 0;
				rolentry->quota = -1;
			}

			rolentry->totalsize += (newsize - oldsize);

			LWLockRelease(shared->lock);
		}
	}
}

/*
 * helper function for refresh_fs_model(), to scan one directory.
 */
static void
RebuildRelSizeMapDir(char *dirpath)
{
	DIR		   *dirdesc;
	struct dirent *dirent;
	char		path[MAXPGPATH];

	dirdesc = AllocateDir(dirpath);

	while((dirent = ReadDirExtended(dirdesc, dirpath, DEBUG1)) != NULL)
	{
		struct stat statbuf;
		RelFileNode rnode;

		if (strcmp(dirent->d_name, ".") == 0 ||
			strcmp(dirent->d_name, "..") == 0)
			continue;

		snprintf(path, MAXPGPATH, "%s/%s", dirpath, dirent->d_name);

		/*
		 * Only count relation files. (Or perhaps we should count other files
		 * towards the database owner?)
		 */
		if (!isRelDataFile(path, &rnode))
			continue;

		/* Also ignore system relations */
		if (rnode.relNode < FirstNormalObjectId)
			continue;

		if (stat(path, &statbuf) != 0)
		{
			ereport(DEBUG1,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", path)));
			continue;
		}

		UpdateFileSize(&rnode, dirent->d_name, statbuf.st_size);
	}

	FreeDir(dirdesc);
}


/*
 * Scan file system, to update the model with all files.
 */
void
refresh_fs_model(void)
{
	DIR		   *dirdesc;
	struct dirent *dirent;
	char		path[MAXPGPATH];
	HASH_SEQ_STATUS iter;
	FileSizeEntry *fsentry;

	/*
	 * Bump the generation counter first, so that we can detect removed files.
	 */
	generation++;

	/* global/<relid> */
	/* ignore shared relations */

	/* base/<dbid>/<relid> */
	dirdesc = AllocateDir("base");
	while ((dirent = ReadDirExtended(dirdesc, "base", DEBUG1)) != NULL)
	{
		Oid			dbid;

		if (strcmp(dirent->d_name, ".") == 0 ||
			strcmp(dirent->d_name, "..") == 0)
			continue;

		if (sscanf(dirent->d_name, "%u", &dbid) != 1)
			continue;

		snprintf(path, MAXPGPATH, "base/%s", dirent->d_name);
		RebuildRelSizeMapDir(path);
	}
	FreeDir(dirdesc);

	/*
	 * pg_tblspc/<tblspc oid>/<tblspc version>/
	 *		within a non-default tablespace (the name of the directory
	 *		depends on version)
	 */
	/* TODO */

	/*
	 * Finally, remove files that no longer exist.
	 */
	hash_seq_init(&iter, path_to_fsentry_map);

	while ((fsentry = hash_seq_search(&iter)) != NULL)
	{
		if (fsentry->generation != generation)
		{
			/*
			 * We didn't see this file during this scan, so it doesn't
			 * exist anymore.
			 */
			RemoveFileSize(fsentry);
		}
	}
}

/*
 * Update the owner of a relation in the model.
 */
void
UpdateRelOwner(RelFileNode *rnode, Oid owner)
{
	RelSizeEntry *relentry;
	RoleSizeEntry *rolentry;
	RoleSizeEntryKey key;
	bool		found;

	relentry = (RelSizeEntry *) hash_search(relfilenode_to_relentry_map,
											(void *) rnode,
											HASH_FIND, &found);
	if (!found)
		return;

	if (relentry->owner == owner)
		return;

	/* Subtract the old size from the old owner's total. */
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);

	key.rolid = relentry->owner;
	key.dbid = MyDatabaseId;
	rolentry = (RoleSizeEntry *) hash_search(role_totals_map,
											 (void *) &key,
											 HASH_FIND, &found);
	Assert(found == (relentry->owner != InvalidOid));
	if (found)
		rolentry->totalsize -= relentry->totalsize;
	LWLockRelease(shared->lock);

	relentry->owner = owner;
	if (relentry->owner == InvalidOid)
		dlist_delete(&relentry->orphan_node);

	if (owner != InvalidOid)
	{
		/* Link to new parent, creating it if it doesn't exist yet. */
		key.rolid = owner;
		key.dbid = MyDatabaseId;

		LWLockAcquire(shared->lock, LW_EXCLUSIVE);

		rolentry = (RoleSizeEntry *) hash_search(role_totals_map,
												 (void *) &key,
												 HASH_ENTER, &found);
		if (!found)
		{
			rolentry->totalsize = 0;
			rolentry->quota = -1;	/* -1 means no quota */
		}
		rolentry->totalsize += relentry->totalsize;
	}
	else
		dlist_push_head(&orphanRels, &relentry->orphan_node);

	LWLockRelease(shared->lock);
}

/*
 * Update the quota for a role.
 *
 * This update the quota field in the in-memory model. This is used when the
 * quotas are loaded from the cofiguration table.
 */
void
UpdateQuota(Oid owner, int64 newquota)
{
	RoleSizeEntry *rolentry;
	RoleSizeEntryKey key;
	bool		found;

	LWLockAcquire(shared->lock, LW_EXCLUSIVE);

	key.rolid = owner;
	key.dbid = MyDatabaseId;
	rolentry = (RoleSizeEntry *) hash_search(role_totals_map,
											 (void *) &key,
											 HASH_ENTER, &found);
	if (!found)
		rolentry->totalsize = 0;

	rolentry->quota = newquota;

	LWLockRelease(shared->lock);
}

/*
 * Scan the list of relations that without owner information, and get their
 * owners.
 */
void
UpdateOrphans(void)
{
	dlist_mutable_iter iter;

	dlist_foreach_modify(iter, &orphanRels)
	{
		RelSizeEntry *relentry = (RelSizeEntry *)
			dlist_container(RelSizeEntry, orphan_node, iter.cur);
		Oid			owner;

		owner = get_relfilenode_owner(&relentry->rnode);
		if (owner)
		{
			UpdateRelOwner(&relentry->rnode, owner);

			/* Note: UpdateRelOwner() unlinks the entry from this list */

			elog(DEBUG1, "updated owner of relation %u/%u/%u to %u",
				 relentry->rnode.dbNode, relentry->rnode.spcNode, relentry->rnode.relNode, owner);
		}
	}
}


/* ---------------------------------------------------------------------------
 * Functions for use in backend processes.
 * ---------------------------------------------------------------------------
 */

/*
 * Returns 'true', if the quota for 'owner' has not been exceeded yet.
 */
bool
CheckQuota(Oid owner)
{
	RoleSizeEntry *rolentry;
	RoleSizeEntryKey key;
	bool		result;

	if (!role_totals_map)
		return true;

	LWLockAcquire(shared->lock, LW_SHARED);

	key.rolid = owner;
	key.dbid = MyDatabaseId;
	rolentry = (RoleSizeEntry *) hash_search(role_totals_map,
											 (void *) &key,
											 HASH_FIND, NULL);
	if (rolentry &&
		rolentry->quota >= 0 &&
		rolentry->totalsize > rolentry->quota)
	{
		/* User has a quota, and it's been exceeded. */
		result = false;
	}
	else
	{
		result = true;
	}

	LWLockRelease(shared->lock);

	return result;
}

/*
 * Function to implement the quota.status view.
 */
Datum
get_quota_status(PG_FUNCTION_ARGS)
{
#define GET_QUOTA_STATUS_COLS	3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS iter;
	RoleSizeEntry *rolentry;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (role_totals_map)
	{
		LWLockAcquire(shared->lock, LW_SHARED);

		hash_seq_init(&iter, role_totals_map);
		while ((rolentry = hash_seq_search(&iter)) != NULL)
		{
			/* for each row */
			Datum		values[GET_QUOTA_STATUS_COLS];
			bool		nulls[GET_QUOTA_STATUS_COLS];

			/* Ignore entries for other databases. */
			if (rolentry->key.dbid != MyDatabaseId)
				continue;

			values[0] = rolentry->key.rolid;
			nulls[0] = false;
			values[1] = rolentry->totalsize;
			nulls[1] = false;
			if (rolentry->quota != -1)
			{
				values[2] = rolentry->quota;
				nulls[2] = false;
			}
			else
			{
				values[2] = (Datum) 0;
				nulls[2] = true;
			}

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}

		LWLockRelease(shared->lock);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
