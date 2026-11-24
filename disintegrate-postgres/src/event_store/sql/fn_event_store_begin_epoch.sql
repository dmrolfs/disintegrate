CREATE OR REPLACE FUNCTION event_store_begin_epoch()
RETURNS void AS $$
DECLARE
    id BIGINT;
    db_id INT;
BEGIN
    -- On a sequence that has never been used, last_value is 1 and is_called is false, so the next id drawn is 1 and
    -- the marker would equal the writer's own id. The marker must be below it, so use 0 there.
    SELECT CASE WHEN is_called THEN last_value ELSE 0 END INTO id FROM seq_event_event_id;
    SELECT oid INTO db_id FROM pg_database WHERE datname = current_database();

    PERFORM pg_try_advisory_xact_lock_shared(db_id, 0);
    PERFORM pg_try_advisory_xact_lock_shared(1, (id & 0xFFFFFFFF)::bit(32)::integer);
    PERFORM pg_try_advisory_xact_lock_shared(2, (id >> 32)::bit(32)::integer);
END;
$$ LANGUAGE plpgsql;
