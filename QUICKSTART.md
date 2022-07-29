# Quick start

## Data Ingest and Database Preparation

To ingest data for a TPC-H benchmark with scale factor 10 using this toolkit,
run the following command:

    ./prepare_benchmark \
        --dsn postgresql://postgres@localhost:5432/swarm64_benchmark \
        --benchmark tpch \
        --scale-factor 10 \
        --schema s64da_performance

Adapt the values for the parameters `--benchmark` and `--scale-factor` for other
benchmarks and scale factors accordingly. If the system you want to benchmark
runs on a different host, you need to adapt the value for `--dsn` as well.

## Benchmark Running

To run a bechmark that has been prepared with the `prepare_benchmark` script,
execute the following command:

    ./run_benchmark \
        --dsn postgresql://postgres@localhost:5432/swarm64_benchmark \
        tpch

Change `tpch` to a different value if you ingested data for a different
benchmark in the step above.

Please refer to `README.md` for a detailed description of all the available
parameters and options.

# Umbra Quick Start

## Using the provided demo script

The provided `demo-umbra-htap` script automatically starts an Umbra server, ingests 
the initial HTAP database population, and finally runs the HTAP benchmark. The script
should be invoked as follows:

    UMBRA_PATH=<path-to-umbra-binaries> \
    DB_PATH=<path-to-put-database-files> \
    UMBRA_PORT=<port> \
    ./demo-umbra-htap \
        <scale-factor> \
        <oltp-workers> \
        <olap-workers>

For example, the following invocation

    UMBRA_PATH=/home/xyz/umbra/bin DB_PATH=/raid/db ./demo-umbra-htap 1 16 2

will use the `/home/xyz/umbra/bin/sql` and `/home/xyz/umbra/bin/server` binaries to
configure and start an Umbra instance which places its database files in the directory
`/raid/db/htap/`. The script will then load the initial database population at scale 
factor `1` (corresponding by default to 20 warehouses, see `benchmarks/htap/lib/helpers.py`),
and subsequently run the HTAP benchmark with `16` OLTP threads and `2` OLAP threads.
For maximum performance, `DB_PATH` should point to a reasonably fast SSD drive (especially
in terms of write bandwidth and response time). Alternatively one can also have `DB_PATH`
point to directory into which a `tmpfs` is mounted. `UMBRA_PORT` can be omitted and defaults
to `54321`.

More fine-grained control over the benchmark is possible by manually adjusting the 
contents of the `demo-umbra-htap` script, which internally simply calls the `prepare_benchmark`
and `run_benchmark` Python scripts with some sensible default parameters. Please note that
both scripts require the `--umbra` flag to be set in order to ensure compatibility with Umbra.