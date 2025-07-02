defmodule Plausible.Repo.Migrations.FixCarboniteTriggertNames do
  use Ecto.Migration

  def up do
    # Update stored procedures with correct Carbonite trigger names
    execute """
    CREATE OR REPLACE FUNCTION carbonite_disable_table_triggers(target_table TEXT)
    RETURNS void AS $$
    DECLARE
        env_name TEXT;
    BEGIN
        -- Get current environment (default to 'development' if not set)
        env_name := COALESCE(current_setting('app.environment', true), 'development');
        
        -- Disable the Carbonite trigger for this table
        BEGIN
            EXECUTE format('ALTER TABLE %I DISABLE TRIGGER capture_changes_into_carbonite_default_trigger', target_table);
        EXCEPTION WHEN undefined_object THEN
            -- Trigger doesn't exist, that's fine
        END;
        
        -- Record the state change
        INSERT INTO carbonite_trigger_states (table_name, environment, triggers_enabled, disabled_at, inserted_at, updated_at)
        VALUES (target_table, env_name, false, NOW(), NOW(), NOW())
        ON CONFLICT (table_name, environment) 
        DO UPDATE SET 
            triggers_enabled = false, 
            disabled_at = NOW(), 
            enabled_at = null,
            updated_at = NOW();
    END;
    $$ LANGUAGE plpgsql;
    """

    execute """
    CREATE OR REPLACE FUNCTION carbonite_enable_table_triggers(target_table TEXT)
    RETURNS void AS $$
    DECLARE
        env_name TEXT;
    BEGIN
        -- Get current environment (default to 'development' if not set)
        env_name := COALESCE(current_setting('app.environment', true), 'development');
        
        -- Enable the Carbonite trigger for this table
        BEGIN
            EXECUTE format('ALTER TABLE %I ENABLE TRIGGER capture_changes_into_carbonite_default_trigger', target_table);
        EXCEPTION WHEN undefined_object THEN
            -- Trigger doesn't exist, that's fine
        END;
        
        -- Record the state change
        INSERT INTO carbonite_trigger_states (table_name, environment, triggers_enabled, enabled_at, inserted_at, updated_at)
        VALUES (target_table, env_name, true, NOW(), NOW(), NOW())
        ON CONFLICT (table_name, environment) 
        DO UPDATE SET 
            triggers_enabled = true, 
            disabled_at = null,
            enabled_at = NOW(),
            updated_at = NOW();
    END;
    $$ LANGUAGE plpgsql;
    """
  end

  def down do
    # Revert to previous (incorrect) version - this is just for rollback safety
    execute """
    CREATE OR REPLACE FUNCTION carbonite_disable_table_triggers(target_table TEXT)
    RETURNS void AS $$
    DECLARE
        env_name TEXT;
    BEGIN
        env_name := COALESCE(current_setting('app.environment', true), 'development');
        
        BEGIN
            EXECUTE format('ALTER TABLE %I DISABLE TRIGGER carbonite_insert_trigger', target_table);
        EXCEPTION WHEN undefined_object THEN
        END;
        
        BEGIN
            EXECUTE format('ALTER TABLE %I DISABLE TRIGGER carbonite_update_trigger', target_table);
        EXCEPTION WHEN undefined_object THEN
        END;
        
        BEGIN
            EXECUTE format('ALTER TABLE %I DISABLE TRIGGER carbonite_delete_trigger', target_table);
        EXCEPTION WHEN undefined_object THEN
        END;
        
        INSERT INTO carbonite_trigger_states (table_name, environment, triggers_enabled, disabled_at, inserted_at, updated_at)
        VALUES (target_table, env_name, false, NOW(), NOW(), NOW())
        ON CONFLICT (table_name, environment) 
        DO UPDATE SET 
            triggers_enabled = false, 
            disabled_at = NOW(), 
            enabled_at = null,
            updated_at = NOW();
    END;
    $$ LANGUAGE plpgsql;
    """
  end
end