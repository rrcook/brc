defmodule Brc do
  use Agent

  @pool_size :erlang.system_info(:logical_processors)
  @blob_size 100_000

  # each process has its own partial map of the cities
  # for each line, parse out the city name and temperature * 10, keeping the numbers as integers
  # if a city is already in the map, to the min, max and counting on it
  def process_lines(state_map, buffer) do
    lines = :binary.split(buffer, <<"\n">>, [:global])

    Enum.reduce(lines, state_map, fn line, acc_map ->
      case line do
        "" ->
          acc_map

        _ ->
          [city, temperature_text] = :binary.split(line, ";")
          [t1, t2] = :binary.split(temperature_text, ".")
          temperature = :erlang.binary_to_integer(t1 <> t2)

          Map.update(
            acc_map,
            city,
            {temperature, 1, temperature, temperature},
            fn {min_temp, count, sum, max_temp} ->
              {min(min_temp, temperature), count + 1, sum + temperature,
               max(max_temp, temperature)}
            end
          )
      end
    end)
  end

  def process_file(file, worker_pool) do
    case :prim_file.read(file, @blob_size) do
      :eof ->
        :ok

      {:ok, buffer} ->
        buffer =
          case :prim_file.read_line(file) do
            :eof ->
              buffer

            {:ok, line} ->
              <<buffer::binary, line::binary>>
          end

        index = :ets.update_counter(:brc, _key = :index, _increment_by = 1)

        Agent.cast(:ets.lookup_element(:workers, rem(index, @pool_size), 2), Brc, :process_lines, [
          buffer
        ])

        process_file(file, worker_pool)
    end
  end

  def run_file_buf(filename) do
    :ets.new(:brc, [:public, :named_table])
    :ets.insert(:brc, {:index, -1})

    :ets.new(:workers, [:set, :public, :named_table])

   worker_pool =
     Enum.map(1..@pool_size, fn _ ->
       Agent.start_link(fn -> %{} end) |> elem(1)
     end)

  Enum.each(Enum.zip(0..@pool_size - 1, worker_pool), fn w ->
    :ets.insert(:workers, w)
  end)

    {:ok, file} = :prim_file.open(filename, [:binary, :read])

    process_file(file, worker_pool)

    # synchronous here to make sure all of the workers are finished
    pool_maps = Enum.map(worker_pool, fn pid -> Agent.get(pid, fn state -> state end, :infinity) end)

    # feeding all other maps into one, first one is the chosen one
    [head | tail] = pool_maps

    # if each map has a city, merge it into the chosen one
    combined_map =
      Enum.reduce(tail, head, fn elem, acc ->
        Map.merge(acc, elem, fn _key, {min1, count1, sum1, max1}, {min2, count2, sum2, max2} ->
          {min(min1, min2), count1 + count2, sum1 + sum2, max(max1, max2)}
        end)
      end)

    # city for sorting, plus string we will output
    keys_strings =
      Map.keys(combined_map)
      |> Enum.map(fn key ->
        {min_temp, count, sum, max_temp} = Map.get(combined_map, key)

        {key,
         "#{key}=#{min_temp / 10}/#{:erlang.float_to_binary(sum / (count * 10), decimals: 1)}/#{max_temp / 10}"}
      end)

    # sort the strings by city/key then discard the key, keep the output
    sorted_strings = Enum.sort_by(keys_strings, &elem(&1, 0)) |> Enum.map(&elem(&1, 1))

    # output in brc format
    IO.puts("{#{Enum.join(sorted_strings, ", ")}}")
  end

  def main(args) do
    IO.puts("Using prim_file")
    {uSec, :ok} =
      :timer.tc(fn ->
        run_file_buf(Enum.at(args, 0))
        :ok
      end)

    IO.puts("It took #{uSec / 1000} milliseconds")
  end
end
