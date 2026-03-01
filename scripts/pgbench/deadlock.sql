\set id1 random(1, 100)
\set id2 random(101, 200)

BEGIN;

UPDATE inventory 
SET last_update = NOW() 
WHERE inventory_id = :id1;
\sleep 100 ms

UPDATE inventory 
SET last_update = NOW() 
WHERE inventory_id = :id2;

COMMIT;