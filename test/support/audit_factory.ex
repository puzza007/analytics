defmodule Plausible.AuditFactory do
  @moduledoc """
  Audit-aware factory functions that work with Carbonite triggers.
  
  This module provides factory functions that automatically handle
  Carbonite transaction contexts when audit triggers are enabled,
  while falling back to normal factory operations when they're disabled.
  
  ## Usage
  
  For tests that need audit functionality:
  
      @tag :audit_enabled
      test "operations create audit logs" do
        user = AuditFactory.insert(:user)
        # Audit logs will be created
      end
  
  For regular tests (default behavior):
  
      test "user creation" do
        user = insert(:user)  # or AuditFactory.insert(:user)
        # No audit logs, triggers disabled
      end
  """

  alias Plausible.CarboniteTestHelper
  alias Plausible.Factory

  @doc """
  Inserts a factory record with audit-awareness.
  
  If audit is enabled for the current test, wraps the operation
  in a Carbonite transaction context. Otherwise, uses normal factory insert.
  """
  def insert(factory_name, attrs \\ %{}) do
    if audit_enabled?() do
      CarboniteTestHelper.with_audit_context("test_factory_#{factory_name}", fn ->
        Factory.insert(factory_name, attrs)
      end)
    else
      Factory.insert(factory_name, attrs)
    end
  end

  @doc """
  Inserts multiple factory records with audit-awareness.
  """
  def insert_list(count, factory_name, attrs \\ %{}) do
    if audit_enabled?() do
      CarboniteTestHelper.with_audit_context("test_factory_list_#{factory_name}", fn ->
        Factory.insert_list(count, factory_name, attrs)
      end)
    else
      Factory.insert_list(count, factory_name, attrs)
    end
  end

  @doc """
  Creates related records in a single audit transaction context.
  
  This is useful when you need to create multiple related records
  that should all be captured in the same audit context.
  
  ## Examples
  
      AuditFactory.insert_related("user_with_team", fn ->
        user = Factory.insert(:user)
        team = Factory.insert(:team)
        Factory.insert(:team_membership, user: user, team: team)
        {user, team}
      end)
  """
  def insert_related(context_name, creation_fun) when is_function(creation_fun, 0) do
    if audit_enabled?() do
      CarboniteTestHelper.with_audit_context("test_related_#{context_name}", creation_fun)
    else
      creation_fun.()
    end
  end

  @doc """
  Builds a factory record (no database insertion).
  
  This doesn't need audit context since it doesn't touch the database.
  """
  def build(factory_name, attrs \\ %{}) do
    Factory.build(factory_name, attrs)
  end

  @doc """
  Builds a list of factory records (no database insertion).
  """
  def build_list(count, factory_name, attrs \\ %{}) do
    Factory.build_list(count, factory_name, attrs)
  end

  @doc """
  Executes a function with temporarily disabled audit logging.
  
  This is useful when you need to set up test data that shouldn't
  be audited, even in audit-enabled tests.
  
  ## Examples
  
      @tag :audit_enabled
      test "user operations are audited" do
        # This setup won't be audited
        admin = AuditFactory.without_audit(fn ->
          AuditFactory.insert(:user, role: :admin)
        end)
        
        # This operation will be audited
        user = AuditFactory.insert(:user)
        # ... test audit behavior
      end
  """
  def without_audit(fun) when is_function(fun, 0) do
    if audit_enabled?() do
      Plausible.Audit.TriggerManager.with_disabled_auditing(fun)
    else
      fun.()
    end
  end

  @doc """
  Checks if audit logging is currently enabled for the test.
  """
  def audit_enabled?() do
    Process.get(:audit_enabled, false)
  end

  @doc """
  Creates audit-safe test user with team relationship.
  
  This is a convenience function for the common pattern of creating
  a user with an associated team.
  """
  def create_user_with_team(user_attrs \\ %{}, team_attrs \\ %{}) do
    insert_related("user_with_team", fn ->
      user = Factory.insert(:user, user_attrs)
      {:ok, team} = Plausible.Teams.get_or_create(user)
      
      updated_team = if team_attrs != [] do
        team_changeset = Ecto.Changeset.change(team, team_attrs)
        Plausible.Repo.update!(team_changeset)
      else
        team
      end
      
      {user, updated_team}
    end)
  end

  @doc """
  Creates audit-safe SSO integration for testing.
  """
  def create_sso_integration(team, attrs \\ %{}) do
    insert_related("sso_integration", fn ->
      Factory.insert(:sso_integration, [team: team] ++ attrs)
    end)
  end

  @doc """
  Creates audit-safe SSO domain for testing.
  """
  def create_sso_domain(integration, attrs \\ %{}) do
    insert_related("sso_domain", fn ->
      Factory.insert(:sso_domain, [sso_integration: integration] ++ attrs)
    end)
  end
end