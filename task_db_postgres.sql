create schema cts /*change tracking system*/;

CREATE OR REPLACE FUNCTION cts.log_users_changes()
RETURNS TRIGGER AS $$
BEGIN
    --name
    IF OLD.name IS DISTINCT FROM NEW.name THEN
        INSERT INTO cts.users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (NEW.id, 'admin_postgres', 'name', OLD.name, NEW.name);
    END IF;
    
    --email
    IF OLD.email IS DISTINCT FROM NEW.email THEN
        INSERT INTO cts.users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (NEW.id, 'admin_postgres', 'email', OLD.email, NEW.email);
    END IF;
    
    --role
    IF OLD.role IS DISTINCT FROM NEW.role THEN
        INSERT INTO cts.users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (NEW.id, 'admin_postgres', 'role', OLD.role, NEW.role);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_change_trigger
AFTER UPDATE ON cts.users
FOR EACH ROW
EXECUTE FUNCTION cts.log_users_changes();

CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE OR REPLACE FUNCTION cts.users_audit_export_changes()
RETURNS void AS $$
DECLARE
    file_path TEXT;
BEGIN
    file_path := concat('/tmp/users_audit_export_',
                        to_char(NOW() - INTERVAL '1 day', 'YYYYMMDD_HH24MI'),
                        '.csv');

    EXECUTE format($f$
        COPY (
            SELECT *
            FROM cts.users_audit
            WHERE changed_at::date = CURRENT_DATE - INTERVAL '1 day'
            ORDER BY changed_at
        )
        TO %L
        WITH (FORMAT CSV, HEADER, DELIMITER ',');
    $f$, file_path);
END;
$$ LANGUAGE plpgsql;

SELECT cron.schedule(
    'users_audit_export',
    '0 3 * * *',
    'SELECT cts.users_audit_export_changes();'
);

SELECT * FROM cron.job