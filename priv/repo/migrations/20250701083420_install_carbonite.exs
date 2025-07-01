defmodule Plausible.Repo.Migrations.InstallCarbonite do
  use Ecto.Migration

  def up do
    # Install Carbonite tables and functions
    Carbonite.Migrations.up(1..12)

    # Only install triggers in non-test environments
    unless Mix.env() == :test do
      # Create triggers for SSO-related tables
      Carbonite.Migrations.create_trigger("sso_integrations")
      Carbonite.Migrations.create_trigger("sso_domains")
      Carbonite.Migrations.create_trigger("users")
      Carbonite.Migrations.create_trigger("teams")

      # Configure user table to exclude password hash from audit logs
      Carbonite.Migrations.put_trigger_config("users", :excluded_columns, ["password_hash"])
    end
  end

  def down do
    # Remove triggers from tables (only if they exist)
    unless Mix.env() == :test do
      Carbonite.Migrations.drop_trigger("teams")
      Carbonite.Migrations.drop_trigger("users")
      Carbonite.Migrations.drop_trigger("sso_domains")
      Carbonite.Migrations.drop_trigger("sso_integrations")
    end

    # Remove Carbonite tables and functions
    Carbonite.Migrations.down(12..1)
  end
end
