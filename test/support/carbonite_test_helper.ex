defmodule Plausible.CarboniteTestHelper do
  @moduledoc """
  Helper functions for managing Carbonite transactions in tests and factories.
  
  This module provides utilities to wrap database operations in proper Carbonite
  transaction contexts, ensuring that audit triggers work correctly in test environment.
  """

  alias Plausible.Repo

  @doc """
  Wraps a database operation in a Carbonite transaction context.
  
  This ensures that any database operations that trigger Carbonite audit logs
  are properly handled with the required transaction metadata.
  
  ## Examples
  
      iex> CarboniteTestHelper.with_audit_context(fn ->
      ...>   Repo.insert!(%User{name: "Test User"})
      ...> end)
      %User{}
      
      iex> CarboniteTestHelper.with_audit_context("test_operation", fn ->
      ...>   # Multiple operations
      ...>   user = Repo.insert!(%User{name: "Test User"})
      ...>   team = Repo.insert!(%Team{name: "Test Team"})
      ...>   {user, team}
      ...> end)
      {%User{}, %Team{}}
  """
  @spec with_audit_context(String.t() | nil, function()) :: any()
  def with_audit_context(context_name \\ nil, fun) when is_function(fun, 0) do
    context_name = context_name || "test_operation_#{System.unique_integer([:positive])}"
    
    transaction_metadata = %{
      meta: %{
        event_type: "test_operation",
        context: context_name,
        test_pid: inspect(self()),
        timestamp: NaiveDateTime.utc_now(:second)
      }
    }

    result =
      Ecto.Multi.new()
      |> Carbonite.Multi.insert_transaction(transaction_metadata)
      |> Ecto.Multi.run(:operation, fn _repo, _changes ->
        {:ok, fun.()}
      end)
      |> Repo.transaction()

    case result do
      {:ok, %{operation: result}} ->
        result

      {:error, step, error, _changes} ->
        raise "Carbonite transaction failed at step #{step}: #{inspect(error)}"
    end
  end

  @doc """
  Wraps factory operations to work with Carbonite.
  
  This is specifically designed to work with ExMachina factories.
  """
  @spec wrap_factory(function()) :: any()
  def wrap_factory(factory_fun) when is_function(factory_fun, 0) do
    with_audit_context("factory_operation", factory_fun)
  end

  @doc """
  Creates multiple records in a single Carbonite transaction.
  
  This is useful for creating related records that need to be in the same audit context.
  """
  @spec create_multiple(String.t(), list(function())) :: list(any())
  def create_multiple(context_name, factory_funs) when is_list(factory_funs) do
    with_audit_context(context_name, fn ->
      Enum.map(factory_funs, & &1.())
    end)
  end

end