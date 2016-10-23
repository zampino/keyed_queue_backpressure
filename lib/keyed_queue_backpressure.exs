alias Experimental.GenStage

defmodule KeyedQueueBackpressure do

  defmodule Producer do
    # NOTE: This is the article-contents serializer,
    #       a process which receives updates from **all** channels
    #       and possibly merge patches prior to writing to db,
    #       and hence have global notions %{ node_id => node }
    #       of all nodes. It can 'produce' a stream of updates

    use GenStage
    require Logger

    @keys (0..20) |> Enum.map(& "key_#{&1}")
    @values (0..100)
    @emission_frequency (250..350)

    def start_link() do
      GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def init(:ok) do
      Process.send_after self, :user_update, 100
      {:producer, %{counter: 0, nodes: %{}, queue: :queue.new, waiting: 0, batch: []}}
    end

    def handle_demand(nr, %{waiting: waiting} = state) do
      {state, events} =
        %{state | waiting: nr + waiting}
        |> out([])

      Logger.info("handling demand: #{nr} with #{length(events)}")
      {:noreply, events, state}
    end

    # NOTE: this simulates updates to an article coming from the channel
    #       if a node_id (key) is already enqueued, we just update the
    #       map of nodes.
    #       Also if we couldn't satisfy the demand in handle_demand,
    #       we send messages immediately in the handle info slot

    def handle_info(:user_update, %{counter: counter, nodes: nodes, queue: queue, waiting: waiting} = state) do
      key = Enum.random(@keys)
      value = Enum.random(@values)
      queue = if :queue.member(key, queue) do
        queue
      else
        :queue.in(key, queue)
      end
      event = %{key: key, counter: counter, value: value}
      {state, events} =
        %{state | counter: counter + 1, nodes: Map.put(nodes, key, event), queue: queue}
        |> out([])

      Logger.warn("#{counter}) waiting: #{waiting} -- sending immediately #{length(events)} -- queued: #{:queue.len(state.queue)}")

      Process.send_after(self, :user_update, Enum.random(@emission_frequency))
      {:noreply, events, state}
    end

    def out(%{waiting: 0} = state, acc), do: {state, :lists.reverse(acc)}
    def out(%{waiting: nr, queue: queue, nodes: nodes} = state, acc) when nr > 0 do
      case :queue.out(queue) do
        {{:value, key}, rest} ->
          out(%{state | waiting: nr - 1, queue: rest}, [Map.fetch!(nodes, key) | acc])
        {:empty, _same} ->
          {state, :lists.reverse(acc)}
      end
    end
  end

  defmodule Consumer do
    require Logger
    use GenStage

    @min_demand 10
    @max_demand 20
    @worker_frequency (300..400)

    def start_link do
      GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def init(:ok) do

      :timer.sleep 1_000
      {:consumer, %{}, subscribe_to: [{KeyedQueueBackpressure.Producer, min_demand: @min_demand, max_demand: @max_demand}] }
    end

    def handle_events(events, _from, state) do
      Logger.debug("#{inspect Enum.map(events, &to_string(&1.counter))}) receiving #{length(events)}")
      Enum.each events, &spawn_or_update(&1, state)
      {:noreply, [], state}
    end

    # NOTE: blocking calls to the NodeManager to start a node
    #       back-pressuring demand
    def spawn_or_update(event, state) do
      :timer.sleep Enum.random(@worker_frequency)
      # case Manager.start_node(state.manager, event) do
      #   {:ok, _pid} -> :ok
      #   {:error, {:already_started, pid}} ->
      #     Node.update(pid, event)
      # end
    end
  end

  def start do
    import Supervisor.Spec
    children = [
      worker(Producer, []),
      worker(Consumer, []),
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

end

KeyedQueueBackpressure.start
:timer.sleep 40_000
