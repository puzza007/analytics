defmodule Plausible.Repo.Migrations.RemoveCarboniteTriggersInTest do
  use Ecto.Migration

  def up do
    if Mix.env() == :test do
      # Remove any existing triggers in test environment
      try_drop_trigger("teams")
      try_drop_trigger("users")
      try_drop_trigger("sso_domains")
      try_drop_trigger("sso_integrations")
    end
  end

  def down do
    # No-op, we don't want to reinstall triggers in test
  end

  defp try_drop_trigger(table_name) do
    try do
      Carbonite.Migrations.drop_trigger(table_name)
    rescue
      _ -> :ok
    end
  end
end