
--
CREATE USER quotatest_user NOLOGIN;

CREATE TABLE qt (t text);
ALTER TABLE qt OWNER TO quotatest_user;

INSERT INTO qt SELECT repeat('x', 100) FROM generate_series(1, 100000);

-- Wait a little, to make sure the background worker has picked up the new size.
select pg_sleep(5);

-- Display the table size.
select pg_size_pretty(pg_total_relation_size('qt'));

-- The "disk space used" as shown in quota_status should match
SELECT rolname,
       pg_size_pretty(space_used) as used,
       pg_size_pretty(quota) as quota
FROM quota.status
WHERE rolname::text like 'quotatest%';



-- Set a quota for the user.
INSERT INTO quota.config VALUES ('quotatest_user'::regrole, pg_size_bytes('20 MB'));

-- Wait a little, to give the worker a chance to pick up the new quota.
select pg_sleep(5);

SELECT rolname,
       pg_size_pretty(space_used) as used,
       pg_size_pretty(quota) as quota
FROM quota.status
WHERE rolname::text like 'quotatest%';

-- Now insert enough data that the quota is exceeded.
INSERT INTO qt SELECT repeat('x', 100) FROM generate_series(1, 100000);

-- and again wait a little, so that the worker picks up the new file size
select pg_sleep(5);

SELECT rolname,
       pg_size_pretty(space_used) as used,
       pg_size_pretty(quota) as quota
FROM quota.status
WHERE rolname::text like 'quotatest%';

-- Try to insert again. This should fail, because the quota is exceeded.
INSERT INTO qt SELECT repeat('x', 100) FROM generate_series(1, 100000);


-- Free up the space, by truncating the table. Now it should work again.
TRUNCATE qt;

-- and again wait a little, so that the worker picks up the new file size
select pg_sleep(5);

SELECT rolname,
       pg_size_pretty(space_used) as used,
       pg_size_pretty(quota) as quota
FROM quota.status
WHERE rolname::text like 'quotatest%';

INSERT INTO qt SELECT repeat('x', 100) FROM generate_series(1, 100000);
