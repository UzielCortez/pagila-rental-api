
\set inventory_id 1
\set customer_id random(1, 100)
\set staff_id 1

BEGIN;

SELECT inventory_id FROM inventory WHERE inventory_id = :inventory_id FOR UPDATE;

INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
SELECT NOW(), :inventory_id, :customer_id, :staff_id
WHERE NOT EXISTS (
    SELECT 1 FROM rental WHERE inventory_id = :inventory_id AND return_date IS NULL
);
COMMIT;