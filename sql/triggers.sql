CREATE TABLE IF NOT EXISTS audit_log (
    log_id SERIAL PRIMARY KEY,
    event_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name TEXT,
    operation TEXT,
    old_data JSONB,
    new_data JSONB,
    user_name TEXT
);

CREATE OR REPLACE FUNCTION audit_changes() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO audit_log(table_name, operation, old_data, user_name)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD), session_user);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO audit_log(table_name, operation, old_data, new_data, user_name)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW), session_user);
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO audit_log(table_name, operation, new_data, user_name)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(NEW), session_user);
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_rental ON rental;

CREATE TRIGGER trg_audit_rental
AFTER INSERT OR UPDATE OR DELETE ON rental
FOR EACH ROW EXECUTE FUNCTION audit_changes();

CREATE OR REPLACE FUNCTION check_active_rentals() 
RETURNS TRIGGER AS $$
DECLARE
    active_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO active_count
    FROM rental
    WHERE customer_id = NEW.customer_id 
    AND return_date IS NULL;

    IF active_count >= 3 THEN
        RAISE EXCEPTION 'ALERTA DE NEGOCIO: El cliente % ya tiene % rentas activas. Límite superado.', NEW.customer_id, active_count;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_limit_rentals ON rental;

CREATE TRIGGER trg_limit_rentals
BEFORE INSERT ON rental
FOR EACH ROW EXECUTE FUNCTION check_active_rentals();