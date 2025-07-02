defmodule Plausible.CarboniteTransactionHandler do
  @moduledoc """
  Custom transaction handler for Carbonite that automatically creates audit contexts
  when database operations happen outside of explicit Carbonite transactions.
  
  This allows existing code to work with Carbonite triggers without requiring
  every database operation to be wrapped in explicit audit transactions.
  """

  use GenServer
  require Logger

  @doc """
  Starts the transaction handler GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates an automatic Carbonite transaction for operations that happen
  outside of explicit audit contexts.
  """
  def ensure_transaction_context(operation_type \\ "auto_transaction") do
    case Process.get(:carbonite_transaction_id) do
      nil ->
        create_auto_transaction(operation_type)
      transaction_id ->
        {:ok, transaction_id}
    end
  end

  defp create_auto_transaction(operation_type) do
    transaction_metadata = %{
      meta: %{
        event_type: operation_type,
        auto_generated: true,
        process_id: inspect(self()),
        timestamp: NaiveDateTime.utc_now(:second),
        source: "automatic_transaction_handler"
      }
    }

    result =
      Ecto.Multi.new()
      |> Carbonite.Multi.insert_transaction(transaction_metadata)
      |> Plausible.Repo.transaction()

    case result do
      {:ok, %{transaction: transaction}} ->
        # Store the transaction ID in the process dictionary for reuse
        Process.put(:carbonite_transaction_id, transaction.id)
        {:ok, transaction.id}

      {:error, _step, error, _changes} ->
        Logger.warning("Failed to create automatic Carbonite transaction: #{inspect(error)}")
        {:error, error}
    end
  end

  # GenServer callbacks
  def init(_opts) do
    {:ok, %{}}
  end
end