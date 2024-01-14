defmodule BrcCity do
  use Agent

  def dispatch_line(state, line) do
    [city, temperature_text] = :binary.split(line, ";")
    [t1, t2] = :binary.split(temperature_text, ".")
    temperature = :erlang.binary_to_integer(t1 <> t2)

    {new_state, pid} = case Map.get(state, city) do
      nil ->
        {:ok, pid} = Agent.start_link(fn -> {1000, 0, 0, -1000} end)
        {Map.put(state, city, pid), pid}
      existing_pid ->
        {state, existing_pid}
    end

    Agent.cast(pid, Brc, :update_worker, [temperature])
    new_state
  end

  def dispatch_get(state) do
    keys_strings = Map.keys(state)
    |> Enum.map(fn key ->
      pid = Map.get(state, key)
      Agent.get(pid, fn {min_temp, count, sum, max_temp} ->
        {key, "#{key}=#{min_temp / 10}/#{:erlang.float_to_binary(sum / (count * 10), [decimals: 1])}/#{max_temp / 10}"}
      end)
    end)
    sorted_strings = Enum.sort_by(keys_strings, &(elem(&1, 0))) |> Enum.map(&(elem(&1, 1)))

    "{#{Enum.join(sorted_strings, ", ")}}"
  end

  @spec update_worker({integer(), integer(), integer(), integer()}, integer()) :: {integer(), integer(), integer(), integer()}
  def update_worker({min_temp, count, sum, max_temp}, temperature) do
    {
      min(min_temp, temperature),
      count + 1,
      sum + temperature,
      max(max_temp, temperature)
    }
  end

  defp gather_lines(<<"\n", rest::binary>>, acc, dispatch_pid) do
    # do things with acc
    Agent.cast(dispatch_pid, Brc, :dispatch_line, [acc])
    gather_lines(rest, <<>>, dispatch_pid)
  end

  defp gather_lines(<<c::binary-size(1), rest::binary>>, acc, dispatch_pid) do
    gather_lines(rest, acc <> c, dispatch_pid)
  end

  defp gather_lines(<<>>, acc, _dispatch_pid) do
    acc
  end

  def run_file_buf(filename) do

    {:ok, dispatch_pid} = Agent.start_link(fn -> %{} end)

    file_stream = File.stream!(filename, 16384, [:read_ahead])
    IO.inspect(file_stream)

    worker_stream = Stream.transform(file_stream, <<>>,
      fn elem, acc ->
        # IO.puts(elem)
        new_acc = gather_lines(acc <> elem, <<>>, dispatch_pid)
        {[], new_acc}
      end
    )

    Stream.run(worker_stream)

    my_list = Agent.get(dispatch_pid, &dispatch_get/1)
    IO.puts(my_list)

  end

  def main(args) do

    {uSec, :ok} = :timer.tc(
      fn ->
        run_file_buf(Enum.at(args, 0))
        :ok
      end
    )
    IO.puts("It took #{uSec / 1000} milliseconds")
  end
end
