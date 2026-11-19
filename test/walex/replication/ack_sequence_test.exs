defmodule WalEx.Replication.AckSequenceTest do
  @moduledoc """
  Drives `WalEx.Replication.Server` through whole message sequences (keepalives,
  transactions, periodic ticks) and checks every status update it emits:
  the reported LSN never decreases and never exceeds the safe position, the
  highest of the slot's own position, the last handled commit's end, and the
  keepalive positions seen while no transaction was open.
  """

  use ExUnit.Case, async: false

  import WalEx.Support.TestHelpers,
    only: [
      ack_lsn: 1,
      begin_msg: 1,
      commit_msg: 2,
      keepalive: 2,
      start_config!: 2,
      streaming_state: 2,
      tick: 1,
      xlog: 1
    ]

  alias WalEx.Config.Registry
  alias WalEx.Replication.Server

  @slot_lsn 50
  @step 100
  @long_ago -10_000

  setup do
    start_supervised!(Registry.child_spec())

    app = String.to_atom("ack_sequence_#{System.unique_integer([:positive])}")

    start_config!(app, durable_slot: true, ack_after_processing: true)

    %{app: app}
  end

  # Each step takes and returns the server state plus the status updates it
  # emitted, as a list of LSNs.
  defp apply_step(state, {:keepalive, wal_end, reply}),
    do: wal_end |> keepalive(reply) |> Server.handle_data(state) |> unpack()

  defp apply_step(state, {:begin, final_lsn}),
    do: final_lsn |> begin_msg() |> xlog() |> Server.handle_data(state) |> unpack()

  defp apply_step(state, {:commit, end_lsn}),
    do: (end_lsn - 8) |> commit_msg(end_lsn) |> xlog() |> Server.handle_data(state) |> unpack()

  defp apply_step(state, :tick) do
    {:noreply, msgs, state} = tick(state)
    {Enum.map(msgs, &ack_lsn/1), state}
  end

  # Ages the last status update so the next commit is acknowledged right away.
  defp apply_step(state, :age),
    do: {[], %{state | last_status_at: System.monotonic_time(:millisecond) + @long_ago}}

  defp unpack({:noreply, state}), do: {[], state}
  defp unpack({:noreply, msgs, state}), do: {Enum.map(msgs, &ack_lsn/1), state}

  # Runs steps and returns every emitted LSN together with the safe position
  # at the time (an upper bound for it).
  defp run(steps, state, slot_lsn) do
    initial = %{state: state, safe: slot_lsn || 0, in_txn: false, emitted: []}

    steps
    |> Enum.reduce(initial, &run_step/2)
    |> Map.fetch!(:emitted)
    |> Enum.reverse()
  end

  defp run_step(step, acc) do
    {lsns, state} = apply_step(acc.state, step)
    acc = model(%{acc | state: state}, step)
    %{acc | emitted: Enum.reverse(Enum.map(lsns, &{&1, acc.safe}), acc.emitted)}
  end

  # The safe position moves with handled commits and idle keepalives only.
  defp model(acc, {:begin, _}), do: %{acc | in_txn: true}
  defp model(acc, {:commit, end_lsn}), do: %{acc | in_txn: false, safe: max(acc.safe, end_lsn)}

  defp model(acc = %{in_txn: false}, {:keepalive, wal_end, _}),
    do: %{acc | safe: max(acc.safe, wal_end)}

  defp model(acc, _step), do: acc

  # Words over the alphabet, kept well-formed (BEGIN/COMMIT alternate), with
  # realistic positions: step i is at LSN i * @step, and a transaction commits
  # at the LSN of its COMMIT step, so keepalives inside it stay below its end.
  defp words(length) do
    alphabet = [:keepalive, :keepalive_reply, :tick, :begin, :commit, :age]

    1..length
    |> Enum.reduce([[]], fn _, acc -> for word <- acc, symbol <- alphabet, do: [symbol | word] end)
    |> Enum.map(&Enum.reverse/1)
    |> Enum.filter(&well_formed?/1)
  end

  # BEGIN and COMMIT must alternate, starting outside a transaction.
  defp well_formed?(word), do: well_formed?(word, false)

  defp well_formed?([], _in_txn), do: true
  defp well_formed?([:begin | rest], false), do: well_formed?(rest, true)
  defp well_formed?([:commit | rest], true), do: well_formed?(rest, false)
  defp well_formed?([symbol | _rest], _in_txn) when symbol in [:begin, :commit], do: false
  defp well_formed?([_other | rest], in_txn), do: well_formed?(rest, in_txn)

  # Turn symbols into concrete messages with positions.
  defp concretize(word) do
    word
    |> Enum.with_index(1)
    |> Enum.map(fn
      {:keepalive, i} -> {:keepalive, i * @step, 0}
      {:keepalive_reply, i} -> {:keepalive, i * @step, 1}
      {:commit, i} -> {:commit, i * @step}
      {:begin, i} -> {:begin, commit_position(word, i)}
      {other, _i} -> other
    end)
  end

  defp commit_position(word, begin_index) do
    commit_index =
      word
      |> Enum.with_index(1)
      |> Enum.find_value(fn
        {:commit, i} when i > begin_index -> i
        _other -> nil
      end)

    # An unterminated transaction commits after the end of the word.
    (commit_index || length(word) + 1) * @step
  end

  defp assert_safe_and_monotonic(emitted, word) do
    positions = for {lsn, _safe} <- emitted, lsn > 0, do: lsn

    assert positions == Enum.sort(positions),
           "reported LSN decreased for #{inspect(word)}: #{inspect(emitted)}"

    for {lsn, safe} <- emitted do
      assert lsn <= safe,
             "reported #{lsn} above the safe position #{safe} for #{inspect(word)}"
    end
  end

  test "keepalive, BEGIN, requested keepalive, tick, COMMIT, tick, keepalive", %{app: app} do
    state =
      streaming_state(app, slot_confirmed_lsn: @slot_lsn, message_middleware: fn _, _ -> :ok end)

    steps = [
      {:keepalive, 100, 0},
      {:begin, 150},
      {:keepalive, 120, 1},
      :tick,
      :age,
      {:commit, 150},
      :tick,
      {:keepalive, 200, 1}
    ]

    # In the open transaction only the slot's position is safe; after the
    # commit it is the commit's end (the keepalive at 120 is behind it); and an
    # idle keepalive moves it to the server's position.
    assert [50, 50, 150, 150, 200] == steps |> run(state, @slot_lsn) |> Enum.map(&elem(&1, 0))
  end

  test "every well-formed interleaving reports a monotonic, safe position", %{app: app} do
    for slot_lsn <- [@slot_lsn, nil], word <- words(6) do
      state =
        streaming_state(app, slot_confirmed_lsn: slot_lsn, message_middleware: fn _, _ -> :ok end)

      word
      |> concretize()
      |> run(state, slot_lsn)
      |> assert_safe_and_monotonic(word)
    end
  end
end
