defmodule PubSubTest do
  use ExUnit.Case
  alias SessionsTransfer, as: PubSub

  @pubsub_key inspect(__MODULE__)
  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    Application.put_env(:sessions_transfer, :base_path, tmp_dir)
    on_exit(fn -> Application.delete_env(:sessions_transfer, :base_path) end)
    start_supervised!(PubSub)
    :ok
  end

  test "same key" do
    parent = self()

    spawn_link(fn ->
      PubSub.subscribe(@pubsub_key)
      send(parent, :subscribed1)
      assert_receive %{event: "event1"}
      assert_receive %{event: "event2"}
      send(parent, :done1)
    end)

    spawn_link(fn ->
      PubSub.subscribe(@pubsub_key)
      send(parent, :subscribed2)
      assert_receive %{event: "event1"}
      assert_receive %{event: "event2"}
      send(parent, :done2)
    end)

    assert_receive :subscribed1
    assert_receive :subscribed2

    PubSub.broadcast(@pubsub_key, %{event: "event1"})
    PubSub.broadcast(@pubsub_key, %{event: "event2"})

    assert_receive :done1
    assert_receive :done2
  end

  test "different keys" do
    parent = self()

    spawn_link(fn ->
      PubSub.subscribe([@pubsub_key, "1"])
      send(parent, :subscribed1)
      assert_receive %{event: "event1"}
      assert_receive %{event: "event3"}
      refute_received %{event: "event2"}
      send(parent, :done1)
    end)

    spawn_link(fn ->
      PubSub.subscribe([@pubsub_key, "2"])
      send(parent, :subscribed2)
      assert_receive %{event: "event2"}
      assert_receive %{event: "event3"}
      refute_received %{event: "event1"}
      send(parent, :done2)
    end)

    assert_receive :subscribed1
    assert_receive :subscribed2

    PubSub.broadcast([@pubsub_key, "1"], %{event: "event1"})
    PubSub.broadcast([@pubsub_key, "2"], %{event: "event2"})
    PubSub.broadcast([@pubsub_key, "1"], %{event: "event3"})
    PubSub.broadcast([@pubsub_key, "2"], %{event: "event3"})

    assert_receive :done1
    assert_receive :done2
  end
end
