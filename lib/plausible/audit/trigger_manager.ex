defmodule Plausible.Audit.TriggerManager do
  @moduledoc """
  Manages Carbonite trigger states for testing and development.
  
  This module provides safe, atomic operations for enabling and disabling
  Carbonite audit triggers on specific tables. It uses PostgreSQL advisory
  locks to prevent race conditions and maintains state in the database.
  """

  alias Plausible.Repo
  require Logger

  @audited_tables ["users", "teams", "sso_integrations", "sso_domains"]
  @lock_id 12345

  @doc """
  Returns the list of tables that have Carbonite audit triggers.
  """
  def audited_tables, do: @audited_tables

  @doc """
  Disables Carbonite triggers for all audited tables.
  
  This is useful during test setup or bulk operations where you want
  to temporarily suspend audit logging.
  """
  def disable_auditing_for_test() do
    with_advisory_lock(fn ->
      Logger.debug("Disabling Carbonite triggers for test environment")
      
      for table <- @audited_tables do
        case disable_triggers(table) do
          :ok -> 
            Logger.debug("Disabled triggers for table: #{table}")
          {:error, reason} -> 
            Logger.warning("Failed to disable triggers for #{table}: #{inspect(reason)}")
        end
      end
    end)
  end

  @doc """
  Enables Carbonite triggers for all audited tables.
  
  This re-enables audit logging after it was temporarily disabled.
  """
  def enable_auditing_for_test() do
    with_advisory_lock(fn ->
      Logger.debug("Enabling Carbonite triggers for test environment")
      
      for table <- @audited_tables do
        case enable_triggers(table) do
          :ok -> 
            Logger.debug("Enabled triggers for table: #{table}")
          {:error, reason} -> 
            Logger.warning("Failed to enable triggers for #{table}: #{inspect(reason)}")
        end
      end
    end)
  end

  @doc """
  Executes a function with Carbonite triggers temporarily disabled.
  
  This ensures that triggers are properly re-enabled even if the
  function raises an exception.
  
  ## Examples
  
      TriggerManager.with_disabled_auditing(fn ->
        # Bulk operations that don't need audit logging
        Repo.insert_all("users", user_data)
      end)
  """
  def with_disabled_auditing(fun) when is_function(fun, 0) do
    disable_auditing_for_test()
    
    try do
      fun.()
    after
      enable_auditing_for_test()
    end
  end

  @doc """
  Checks if Carbonite triggers are currently enabled for a specific table.
  """
  def triggers_enabled?(table_name) when table_name in @audited_tables do
    case Repo.query("SELECT carbonite_triggers_enabled($1)", [table_name]) do
      {:ok, %{rows: [[enabled]]}} -> enabled
      {:error, _} -> true  # Default to enabled if we can't check
    end
  end

  @doc """
  Disables Carbonite triggers for a specific table.
  """
  def disable_triggers(table_name) when table_name in @audited_tables do
    case Repo.query("SELECT carbonite_disable_table_triggers($1)", [table_name]) do
      {:ok, _} -> 
        :ok
      {:error, reason} -> 
        {:error, reason}
    end
  end

  @doc """
  Enables Carbonite triggers for a specific table.
  """
  def enable_triggers(table_name) when table_name in @audited_tables do
    case Repo.query("SELECT carbonite_enable_table_triggers($1)", [table_name]) do
      {:ok, _} -> 
        :ok
      {:error, reason} -> 
        {:error, reason}
    end
  end

  @doc """
  Gets the current trigger state for all audited tables.
  
  Returns a map of table_name => enabled_status.
  """
  def get_trigger_states() do
    env = Mix.env() |> to_string()
    
    query = """
    SELECT table_name, triggers_enabled 
    FROM carbonite_trigger_states 
    WHERE environment = $1
    """
    
    case Repo.query(query, [env]) do
      {:ok, %{rows: rows}} ->
        rows
        |> Enum.into(%{}, fn [table, enabled] -> {table, enabled} end)
        
      {:error, _} ->
        # If we can't read the state, assume all are enabled
        @audited_tables
        |> Enum.into(%{}, fn table -> {table, true} end)
    end
  end

  @doc """
  Sets the environment configuration for PostgreSQL.
  
  This allows the stored procedures to track environment-specific
  trigger states.
  """
  def set_environment(env_name) when is_binary(env_name) do
    Repo.query("SELECT set_config('app.environment', $1, false)", [env_name])
  end

  # Private helper functions

  defp with_advisory_lock(fun) when is_function(fun, 0) do
    Repo.transaction(fn ->
      # Acquire advisory lock
      case Repo.query("SELECT pg_advisory_lock($1)", [@lock_id]) do
        {:ok, _} ->
          try do
            fun.()
          after
            # Always release the lock
            Repo.query("SELECT pg_advisory_unlock($1)", [@lock_id])
          end
          
        {:error, reason} ->
          Logger.error("Failed to acquire advisory lock: #{inspect(reason)}")
          {:error, :lock_failed}
      end
    end)
  end
end