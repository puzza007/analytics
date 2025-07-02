defmodule Plausible.Repo.Migrations.AddCarboniteManagement do
  use Ecto.Migration

  def up do
    # Create trigger state management table
    create table(:carbonite_trigger_states, primary_key: false) do
      add :table_name, :string, null: false, primary_key: true
      add :environment, :string, null: false, primary_key: true
      add :triggers_enabled, :boolean, default: true, null: false
      add :disabled_at, :naive_datetime
      add :enabled_at, :naive_datetime
      timestamps()
    end

    create unique_index(:carbonite_trigger_states, [:table_name, :environment])

    # Create stored procedures for safe trigger management
    execute """
    CREATE OR REPLACE FUNCTION carbonite_disable_table_triggers(target_table TEXT)
    RETURNS void AS $$
    DECLARE
        env_name TEXT;
    BEGIN
        -- Get current environment (default to 'development' if not set)
        env_name := COALESCE(current_setting('app.environment', true), 'development');
        
        -- Disable the specific Carbonite triggers for this table
        BEGIN
            EXECUTE format('ALTER TABLE %I DISABLE TRIGGER carbonite_insert_trigger', target_table);
        EXCEPTION WHEN undefined_object THEN
            -- Trigger doesn't exist, that's fine
        END;
        
        BEGIN
            EXECUTE format('ALTER TABLE %I DISABLE TRIGGER carbonite_update_trigger', target_table);
        EXCEPTION WHEN undefined_object THEN
            -- Trigger doesn't exist, that's fine
        END;
        
        BEGIN
            EXECUTE format('ALTER TABLE %I DISABLE TRIGGER carbonite_delete_trigger', target_table);
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
        
        -- Enable the specific Carbonite triggers for this table
        BEGIN
            EXECUTE format('ALTER TABLE %I ENABLE TRIGGER carbonite_insert_trigger', target_table);
        EXCEPTION WHEN undefined_object THEN
            -- Trigger doesn't exist, that's fine
        END;
        
        BEGIN
            EXECUTE format('ALTER TABLE %I ENABLE TRIGGER carbonite_update_trigger', target_table);
        EXCEPTION WHEN undefined_object THEN
            -- Trigger doesn't exist, that's fine
        END;
        
        BEGIN
            EXECUTE format('ALTER TABLE %I ENABLE TRIGGER carbonite_delete_trigger', target_table);
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

    # Create convenience function to check trigger state
    execute """
    CREATE OR REPLACE FUNCTION carbonite_triggers_enabled(target_table TEXT)
    RETURNS boolean AS $$
    DECLARE
        env_name TEXT;
        enabled_state boolean;
    BEGIN
        env_name := COALESCE(current_setting('app.environment', true), 'development');
        
        SELECT triggers_enabled INTO enabled_state
        FROM carbonite_trigger_states 
        WHERE table_name = target_table AND environment = env_name;
        
        -- Default to enabled if no record exists
        RETURN COALESCE(enabled_state, true);
    END;
    $$ LANGUAGE plpgsql;
    """

    # Initialize state for existing audited tables
    audited_tables = ["users", "teams", "sso_integrations", "sso_domains"]
    env = Mix.env() |> to_string()
    
    for table <- audited_tables do
      execute """
      INSERT INTO carbonite_trigger_states (table_name, environment, triggers_enabled, enabled_at, inserted_at, updated_at)
      VALUES ('#{table}', '#{env}', true, NOW(), NOW(), NOW())
      ON CONFLICT (table_name, environment) DO NOTHING;
      """
    end
  end

  def down do
    # Drop stored procedures
    execute "DROP FUNCTION IF EXISTS carbonite_triggers_enabled(TEXT);"
    execute "DROP FUNCTION IF EXISTS carbonite_enable_table_triggers(TEXT);"
    execute "DROP FUNCTION IF EXISTS carbonite_disable_table_triggers(TEXT);"
    
    # Drop trigger state table
    drop table(:carbonite_trigger_states)
  end
end