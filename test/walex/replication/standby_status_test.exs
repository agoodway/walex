defmodule WalEx.Replication.StandbyStatusTest do
  use ExUnit.Case, async: false

  import WalEx.Support.TestHelpers,
    only: [
      ack_lsn: 1,
      begin_msg: 1,
      commit_msg: 2,
      keepalive: 2,
      start_config!: 2,
      streaming_state: 1,
      streaming_state: 2,
      tick: 1,
      xlog: 1,
      xlog: 2
    ]

  alias WalEx.{Config, Config.Registry}
  alias WalEx.Replication.Server

  setup do
    start_supervised!(Registry.child_spec())
    :ok
  end

  describe "config" do
    test "standby_status_interval defaults to disabled" do
      start_config!(:sb_default, [])
      assert Config.get_configs(:sb_default, :standby_status_interval) == nil
    end

    test "standby_status_interval accepts a positive integer" do
      start_config!(:sb_set, standby_status_interval: 5_000)
      assert Config.get_configs(:sb_set, :standby_status_interval) == 5_000
    end

    test "standby_status_interval rejects anything else" do
      for bad <- [0, -1, "5s", 1.5] do
        assert_raise RuntimeError, ~r/standby_status_interval/, fn ->
          start_config!(:sb_bad, standby_status_interval: bad)
        end
      end
    end
  end

  describe "tracking the WAL position" do
    test "a keepalive without a reply request records wal_end" do
      app = :sb_track_k
      start_config!(app, [])

      assert {:noreply, [], %{last_wal_end: 700}} =
               Server.handle_data(keepalive(700, 0), streaming_state(app))
    end

    test "the position never moves backwards" do
      app = :sb_track_back
      start_config!(app, [])

      assert {:noreply, [], %{last_wal_end: 900}} =
               Server.handle_data(keepalive(700, 0), streaming_state(app, last_wal_end: 900))
    end
  end

  describe "periodic standby status" do
    test "does not arm a timer when no interval is configured" do
      app = :sb_off
      start_config!(app, [])

      assert {:noreply, _msgs, %{standby_timer: nil}} =
               tick(streaming_state(app, last_wal_end: 100))
    end

    test "sends wal_end and reschedules while streaming" do
      app = :sb_send
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, last_wal_end: 100))

      assert ack_lsn(msgs) == 100
      assert is_reference(ref)
      Process.cancel_timer(ref)
    end

    test "floors the reported position at the slot's confirmed_flush_lsn" do
      app = :sb_floor
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, last_wal_end: 100, slot_confirmed_lsn: 4_000))

      assert ack_lsn(msgs) == 4_000
      Process.cancel_timer(ref)
    end

    test "reports wal_end when it is above the slot's confirmed_flush_lsn" do
      app = :sb_above_floor
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, last_wal_end: 5_000, slot_confirmed_lsn: 4_000))

      assert ack_lsn(msgs) == 5_000
      Process.cancel_timer(ref)
    end

    test "sends nothing without a keepalive position even if the slot position is known" do
      app = :sb_floor_unknown
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, [], %{standby_timer: ref}} =
               tick(streaming_state(app, last_wal_end: 0, slot_confirmed_lsn: 4_000))

      Process.cancel_timer(ref)
    end

    test "sends nothing before any WAL position is known, but keeps ticking" do
      app = :sb_unknown
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, [], %{standby_timer: ref}} =
               tick(streaming_state(app))

      assert is_reference(ref)
      Process.cancel_timer(ref)
    end

    test "stops ticking when replication is not streaming" do
      app = :sb_stopped
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, %{standby_timer: nil}} =
               tick(streaming_state(app, streaming?: false, last_wal_end: 5))
    end

    test "ignores a timer message from a previous connection" do
      app = :sb_stale
      start_config!(app, standby_status_interval: 60_000)

      current_token = make_ref()
      current_timer = Process.send_after(self(), :never, 60_000)

      st =
        streaming_state(app,
          last_wal_end: 100,
          standby_timer: current_timer,
          standby_token: current_token
        )

      assert {:noreply, ^st} = Server.handle_info({:standby_status, make_ref()}, st)
      Process.cancel_timer(current_timer)
    end

    test "the timer it arms carries the new token" do
      app = :sb_token
      start_config!(app, standby_status_interval: 1)

      {:noreply, _msgs, %{standby_token: token}} = tick(streaming_state(app, last_wal_end: 100))

      assert is_reference(token)
      assert_receive {:standby_status, ^token}
    end
  end

  describe "with ack_after_processing" do
    test "an idle connection advances to the server's position" do
      app = :sb_ack_idle

      start_config!(app,
        durable_slot: true,
        ack_after_processing: true,
        standby_status_interval: 60_000
      )

      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, last_wal_end: 5_000, confirmed_lsn: 51))

      assert ack_lsn(msgs) == 5_000
      Process.cancel_timer(ref)
    end

    test "an open transaction holds the ack at the confirmed position" do
      app = :sb_ack_txn

      start_config!(app,
        durable_slot: true,
        ack_after_processing: true,
        standby_status_interval: 60_000
      )

      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, last_wal_end: 5_000, in_txn: true, confirmed_lsn: 51))

      assert ack_lsn(msgs) == 51
      Process.cancel_timer(ref)
    end

    test "a keepalive reply request also advances when idle" do
      app = :sb_ack_keepalive
      start_config!(app, durable_slot: true, ack_after_processing: true)

      assert {:noreply, msgs, _} =
               Server.handle_data(keepalive(8_000, 1), streaming_state(app, confirmed_lsn: 51))

      assert ack_lsn(msgs) == 8_000
    end

    test "BEGIN marks a transaction open and COMMIT closes it" do
      app = :sb_ack_flag
      start_config!(app, durable_slot: true, ack_after_processing: true)
      message_middleware = fn _msg, _app -> :ok end
      st = streaming_state(app, message_middleware: message_middleware)

      begin = <<?B, 0::64, 0::64, 0::32>>
      commit = <<?C, 0::8, 0::64, 0::64, 0::64>>

      assert {:noreply, %{in_txn: true}} = Server.handle_data(xlog(begin, 10), st)

      assert {:noreply, _status, %{in_txn: false}} =
               Server.handle_data(xlog(commit, 20), %{st | in_txn: true})
    end
  end

  describe "without ack_after_processing, received commits ahead of the last keepalive" do
    # Default mode acknowledges receipt, and a busy stream sends few keepalives,
    # so a received COMMIT must move the periodic position forward too.
    setup do
      app = :"sb_default_ahead_#{System.unique_integer([:positive])}"
      start_config!(app, standby_status_interval: 60_000)
      %{app: app}
    end

    test "a commit received after the last keepalive is reported", %{app: app} do
      message_middleware = fn _msg, _app -> :ok end

      {:noreply, [], st} =
        Server.handle_data(
          keepalive(100, 0),
          streaming_state(app, message_middleware: message_middleware)
        )

      {:noreply, st} = Server.handle_data(xlog(begin_msg(200)), st)
      # No proactive reply in default mode.
      {:noreply, st} = Server.handle_data(xlog(commit_msg(200, 210)), st)

      assert {:noreply, msgs, %{standby_timer: ref}} = tick(st)
      assert ack_lsn(msgs) == 210
      Process.cancel_timer(ref)
    end

    test "the commit position is still floored at the slot's position", %{app: app} do
      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, confirmed_lsn: 210, slot_confirmed_lsn: 4_000))

      assert ack_lsn(msgs) == 4_000
      Process.cancel_timer(ref)
    end
  end

  describe "with ack_after_processing, confirmed commits ahead of the last keepalive" do
    # Only keepalives update last_wal_end, and Postgres sends few of them while
    # it is busy streaming. Handled commits must still move the slot forward.
    setup do
      app = :"sb_ahead_#{System.unique_integer([:positive])}"

      start_config!(app,
        durable_slot: true,
        ack_after_processing: true,
        standby_status_interval: 60_000
      )

      %{app: app}
    end

    test "a commit handled after the last keepalive is reported", %{app: app} do
      message_middleware = fn _msg, _app -> :ok end
      begin = <<?B, 200::64, 0::64, 1::32>>
      commit = <<?C, 0::8, 200::64, 210::64, 0::64>>

      {:noreply, [], st} =
        Server.handle_data(
          keepalive(100, 0),
          streaming_state(app, message_middleware: message_middleware)
        )

      {:noreply, st} = Server.handle_data(xlog(begin, 0), st)
      # The proactive reply already carries the commit; the tick repeats it.
      {:noreply, commit_msgs, st} = Server.handle_data(xlog(commit, 0), st)
      assert ack_lsn(commit_msgs) == 210

      assert {:noreply, msgs, %{standby_timer: ref}} = tick(st)
      assert ack_lsn(msgs) == 210
      Process.cancel_timer(ref)
    end

    test "a confirmed commit is reported before any keepalive", %{app: app} do
      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, confirmed_lsn: 201))

      assert ack_lsn(msgs) == 201
      Process.cancel_timer(ref)
    end

    test "the slot's position is reported before any keepalive", %{app: app} do
      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, slot_confirmed_lsn: 4_000))

      assert ack_lsn(msgs) == 4_000
      Process.cancel_timer(ref)
    end

    test "a later keepalive still wins when nothing is open", %{app: app} do
      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, last_wal_end: 5_000, confirmed_lsn: 201))

      assert ack_lsn(msgs) == 5_000
      Process.cancel_timer(ref)
    end

    test "an open transaction never reports the keepalive position", %{app: app} do
      assert {:noreply, msgs, %{standby_timer: ref}} =
               tick(streaming_state(app, last_wal_end: 5_000, in_txn: true, confirmed_lsn: 201))

      assert ack_lsn(msgs) == 201
      Process.cancel_timer(ref)
    end

    test "nothing is sent when no position is known", %{app: app} do
      assert {:noreply, [], %{standby_timer: ref}} = tick(streaming_state(app))
      Process.cancel_timer(ref)
    end
  end

  describe "reported positions never decrease within a connection" do
    setup do
      app = :"sb_mono_#{System.unique_integer([:positive])}"

      start_config!(app,
        durable_slot: true,
        ack_after_processing: true,
        standby_status_interval: 60_000
      )

      %{app: app}
    end

    test "a requested reply after BEGIN keeps the idle position", %{app: app} do
      message_middleware = fn _msg, _app -> :ok end
      begin = <<?B, 6_000::64, 0::64, 1::32>>

      {:noreply, idle_msgs, st} =
        Server.handle_data(
          keepalive(5_000, 1),
          streaming_state(app, message_middleware: message_middleware, confirmed_lsn: 201)
        )

      assert ack_lsn(idle_msgs) == 5_000

      {:noreply, st} = Server.handle_data(xlog(begin, 0), st)
      assert st.in_txn

      assert {:noreply, msgs, %{last_reported_lsn: 5_000}} =
               Server.handle_data(keepalive(5_000, 1), st)

      assert ack_lsn(msgs) == 5_000
    end

    test "a periodic update after BEGIN keeps the idle position", %{app: app} do
      {:noreply, msgs, st} = tick(streaming_state(app, last_wal_end: 5_000, confirmed_lsn: 201))
      assert ack_lsn(msgs) == 5_000
      Process.cancel_timer(st.standby_timer)

      assert {:noreply, msgs, %{standby_timer: ref}} = tick(%{st | in_txn: true})
      assert ack_lsn(msgs) == 5_000
      Process.cancel_timer(ref)
    end

    test "the fallback LSN 0 is not recorded", %{app: app} do
      assert {:noreply, msgs, %{last_reported_lsn: nil}} =
               Server.handle_data(keepalive(0, 1), streaming_state(app))

      assert ack_lsn(msgs) == 0
    end
  end

  describe "a transaction whose commit starts at the last keepalive position" do
    # Postgres skips, on restart, transactions whose commit record starts before
    # the confirmed position, so nothing above X may be reported before the
    # COMMIT at X has been handled. The ack is X itself in both modes: with
    # ack_after_processing X was reported while idle, and reports never go back
    # within a connection, so it stays X while the transaction is open.
    for {label, opts, held} <- [
          {"default mode", [], 5_000},
          {"ack_after_processing", [ack_after_processing: true], 5_000}
        ] do
      test "#{label} holds the ack at #{held} before the COMMIT", _ctx do
        app = :"sb_boundary_#{System.unique_integer([:positive])}"

        start_config!(
          app,
          [durable_slot: true, standby_status_interval: 60_000] ++ unquote(opts)
        )

        message_middleware = fn _msg, _app -> :ok end
        x = 5_000
        begin = <<?B, x::64, 0::64, 1::32>>

        {:noreply, [reply], st} =
          Server.handle_data(
            keepalive(x, 1),
            streaming_state(app,
              message_middleware: message_middleware,
              slot_confirmed_lsn: 3_000
            )
          )

        assert ack_lsn([reply]) == x

        {:noreply, st} = Server.handle_data(xlog(begin, x), st)

        assert {:noreply, requested, st} = Server.handle_data(keepalive(x, 1), st)
        assert [unquote(held)] == Enum.map(requested, &ack_lsn([&1]))

        assert {:noreply, periodic, %{standby_timer: ref}} = tick(st)
        assert [unquote(held)] == Enum.map(periodic, &ack_lsn([&1]))
        Process.cancel_timer(ref)
      end
    end
  end
end
