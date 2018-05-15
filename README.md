Overview
========

pg_quota is an extension that provides disk space quotas for PostgreSQL.
Using pg_quota, you can limit the amount of disk space that a user can use.

The disk space used by each relation is attributed to the relation's owner.
Space used for by other things, like temporary files or temporary relations,
catalog objects, etc. is ignored.

Limitations
-----------

* Quotas are counted against the direct owner of each relation. You cannot
  have quotas on groups, for example.

* The owner of each relation is determined by the effects of committed
  transactions only. Uncommitted transactions are not taken into account.
  For example, if you change the owner of a table with ALTER TABLE, the
  table is counted towards the old owner's quota until the transaction
  commits. Likewise, if you create a table and load it with data in the
  same transaction, it is not counted towards the user's quota until the
  transaction commits.

* Tracking disk usage is implemented by periodically scanning through the
  data directory. That can be slow, if you have a lot of tables or
  partitions. It also means that there is a significant delay before
  changes to disk usage is reflected in the quota status.

* Quotas are only checked at the beginning of INSERT and COPY statements.
  As long as the user has not exceeded the quota at the beginning of the
  statement, the INSERT or COPY is allowed to go through, even if it
  causes the quota to be exceeded.


As a consequence of the above limitation , quotas are rather "soft". It's
not hard to exceed them if you try.


Usage
=====

GUCs:

pg_quota.refresh_naptime:
    Delay between scans of the data directory.

pg_quota.databases:
    List of databases to enforce quotas on.

In each database that you want to use the quotas on, install the extension,
and add the database name to disk_quotas.databases setting. It cannot be
changed while the server is running, server restart is required. A background
worker process is launched for each database, so if you wish to use quotas on
many databases, you might need to bump up max_worker_processes.

The extension comes with a configuration table, "disk_quotas.disk_quotas".
Insert quota configuration into the table, e.g:

    INSERT INTO quota.config VALUES ('alice'::regrole, pg_size_bytes('10 GB'));

You can view the quotas in effect, and current disk space usage with:

    SELECT rolname,
           pg_size_pretty(space_used) as used,
           pg_size_pretty(quota) as quota
    FROM quota.status;

NULL in 'quota' means no quota is set for the role.

Example:

     rolname |  used  | quota 
    ---------+--------+-------
     foouser | 360 kB | 
     heikki  | 46 MB  | 13 MB
    (2 rows)



Design
======

There is a background worker process for each database, for which quotas
are activated. The worker process maintains an in-memory model of every
relation file and their owner.

A configuration table to hold the quotas.

A shared memory hash table containing the current total disk space usage,
and the quota loaded from the configuration table.



There are two different problems:

1. Keeping the in-memory tree "model" of all the files and their owners up to
   date.

2. Enforcing the quota. When quota is exceeded, prevent operations that
   extend files.


Keeping the model up-to-date
----------------------------

To bootstrap, when the background worker starts, it scans the whole data
directory, and adds all files to the model. It then scans pg_class, and fills
in the owner of each file in the model.

The model is refreshed every X seconds, by scanning the data directory and
pg_class again, like at startup. To detect deleted files, we keep track of
when we last saw each file. Every scan increments a "generation" counter, and
every file we encounter is stamped with the current generation. If, after
scanning the data directory, there are any files in the model with an older
generation stamp, we know that it has been deleted.

TODO:
In order to react more quickly to changes, we should use something like Linux
inotify to detect changes to files continuously. The current polling approach
doesn't scale very well if you have hundreds of thousands of files in the
data directory. Another alternative would be to have the backends themselves
notify the worker process, whenever a file is extended or shrunk (there is no
convenient "hook" location for that, currently). Or perhaps use logical
decoding, although that would not work for unlogged tables.


Enforcing the quota
-------------------

The disk_quota module hooks into the ExecutorCheckPerms_hook, which gets
executed whenever an INSERT or COPY operation starts. If the table owner's
quota has been exceeded, you get an error.

There are some limitations to this approach:

* The quota is only checked at the beginning of the statement. If you have a
  quota of 1 GB, and use COPY to load 10 GB of data, it will succeed as long
  as you are below the quota at the beginning of the operation.

* The quota is not enforced at UPDATEs, or utility commands like CREATE INDEX.
  (If an UPDATE or CREATE INDEX exceeds the quota, any subsequent INSERTs or
  COPYs will fail, though.)

TODO:
It would be nice to have a hook directly in smgrextend() or similar lower
level function, so that we could check the quota whenever a relation file is
extended, for any command.
