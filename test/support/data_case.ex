defmodule Plausible.DataCase do
  @moduledoc """
  This module defines the setup for tests requiring
  access to the application's data layer.

  You may define functions here to be used as helpers in
  your tests.

  Finally, if the test case interacts with the database,
  it cannot be async. For this reason, every test runs
  inside a transaction which is reset at the beginning
  of the test unless the test case is marked as async.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      use Plausible.Repo
      use Plausible.TestUtils

      import Ecto.Changeset
      import Plausible.DataCase
      import Plausible.Factory
      import Plausible.AssertMatches
    end
  end

  setup tags do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Plausible.Repo)

    unless tags[:async] do
      Ecto.Adapters.SQL.Sandbox.mode(Plausible.Repo, {:shared, self()})
    end

    # Set up Carbonite trigger management based on test tags
    setup_audit_environment(tags)

    :ok
  end

  @doc """
  Sets up the audit environment based on test tags.
  
  - `@tag :audit_enabled` - Enables Carbonite triggers for audit testing
  - `@tag :audit_disabled` - Explicitly disables Carbonite triggers  
  - Default behavior: Disables triggers for backward compatibility
  """
  def setup_audit_environment(tags) do
    # Set the environment for PostgreSQL stored procedures
    Plausible.Audit.TriggerManager.set_environment("test")
    
    cond do
      tags[:audit_enabled] ->
        # Enable auditing for tests that specifically test audit functionality
        Plausible.Audit.TriggerManager.enable_auditing_for_test()
        
        # Store audit state in process dictionary for AuditFactory
        Process.put(:audit_enabled, true)
        
        on_exit(fn ->
          Process.delete(:audit_enabled)
          # Clean up but don't disable - let next test decide
        end)

      tags[:audit_disabled] ->
        # Explicitly disable auditing
        Plausible.Audit.TriggerManager.disable_auditing_for_test()
        Process.put(:audit_enabled, false)
        
        on_exit(fn ->
          Process.delete(:audit_enabled)
        end)

      true ->
        # Default: disable auditing for backward compatibility
        Plausible.Audit.TriggerManager.disable_auditing_for_test()
        Process.put(:audit_enabled, false)
        
        on_exit(fn ->
          Process.delete(:audit_enabled)
        end)
    end
  end
end
