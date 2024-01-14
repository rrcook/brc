defmodule BrcTest do
  use ExUnit.Case
  doctest Brc

  test "greets the world" do
    assert Brc.hello() == :world
  end
end
