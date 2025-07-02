defmodule Plausible.CarboniteSetup do
  @moduledoc """
  Helper for managing Carbonite triggers during test setup.
  
  Provides functions to temporarily disable triggers during bulk operations
  like seeds and test setup, then re-enable them for actual testing.
  """

  alias Plausible.Repo

  @doc """
  Temporarily disables Carbonite triggers for the given tables.
  """
  def disable_triggers(table_names) when is_list(table_names) do
    Enum.each(table_names, &disable_trigger/1)
  end

  def disable_trigger(table_name) do
    Repo.query!("ALTER TABLE #{table_name} DISABLE TRIGGER carbonite_insert_trigger")
    Repo.query!("ALTER TABLE #{table_name} DISABLE TRIGGER carbonite_update_trigger")
    Repo.query!("ALTER TABLE #{table_name} DISABLE TRIGGER carbonite_delete_trigger")
  rescue
    _ -> :ok  # Ignore errors if triggers don't exist
  end

  @doc """
  Re-enables Carbonite triggers for the given tables.
  """
  def enable_triggers(table_names) when is_list(table_names) do
    Enum.each(table_names, &enable_trigger/1)
  end

  def enable_trigger(table_name) do
    Repo.query!("ALTER TABLE #{table_name} ENABLE TRIGGER carbonite_insert_trigger")
    Repo.query!("ALTER TABLE #{table_name} ENABLE TRIGGER carbonite_update_trigger")
    Repo.query!("ALTER TABLE #{table_name} ENABLE TRIGGER carbonite_delete_trigger")
  rescue
    _ -> :ok  # Ignore errors if triggers don't exist
  end

  @doc """
  Executes a function with Carbonite triggers temporarily disabled.
  """
  def with_disabled_triggers(table_names, fun) when is_function(fun, 0) do
    disable_triggers(table_names)
    
    try do
      fun.()
    after
      enable_triggers(table_names)
    end
  end

  @doc """
  List of tables that have Carbonite triggers.
  """
  def audited_tables do
    ["users", "teams", "sso_integrations", "sso_domains"]
  end
end