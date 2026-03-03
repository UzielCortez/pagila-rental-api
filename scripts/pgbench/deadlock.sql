\set id1 random(1, 5)
\set id2 random(1, 5)

BEGIN;

UPDATE inventory SET last_update = NOW() WHERE inventory_id = :id1;
SELECT pg_sleep(0.1);
UPDATE inventory SET last_update = NOW() WHERE inventory_id = :id2;

COMMIT;