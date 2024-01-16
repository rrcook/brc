# Brc

Update 1
The main switch is from using File.stream to erlang's prim_file.
For some reason, and I want to investigate this, is that the File.stream based approach only used 8 of 16 processors. That's on my 8 physical CPU, 16 logical CPU machine.

Erlang's prim_file is very handy here. You can read a block of bytes, then at that stopping point you can read_line to get to the end of the line that could have been chopped off from the block read.
Thanks to icedragon200 for pointing me to prim_file.

Original

My attempt at the billion row challenge in Elixir. 
Elixir 1.16.0 - important because the argument order for File.stream changed between 1.15 and 1.16.
Vanilla elixir, no extra libraries to run. I am including eflambe to run & make flamegraphs to help tune performance.

brc_city was my first attempt. It used a process for every city. Don't do this, most of your app's time will be spent in process sleeping.

brc uses a pool of workers. Each worker receives a list of cities. Fewer workers with more work eliminates idle time.

If using eflambe, run something like

iex -S mix

:eflambe.apply({Brc, :run_file_buf, ["measurements.txt"]}, [output_format: :brendan_gregg, open: :speedscope])


## Installation

After cloning, run 'mix deps.get' to get eflambe. Then 'mix escript.build', then './brc measurments.txt'

