from s64da_benchmark_toolkit.prepare import PrepareBenchmarkFactory, TableGroup


class PrepareBenchmark(PrepareBenchmarkFactory):
    PrepareBenchmarkFactory.TABLES = (TableGroup(
        'date',
        'customer',
        'part',
        'supplier',
        'lineorder'
    ),)

    def get_ingest_tasks(self, table):
        use_chunks = (self.args.scale_factor > 1 and
                      self.args.chunks > 1 and
                      table in ('customer', 'lineorder'))
        table_code = table[0]

        dbgen_cmd = f'./dbgen -s {self.args.scale_factor} -T {table_code} -O o'
        psql_copy = self.psql_exec_cmd(f"COPY {table} FROM STDIN WITH DELIMITER '|'")
        sed_cmd = "sed 's/|$//'"

        if use_chunks:
            tasks = [f'{dbgen_cmd} -S {chunk} -C {self.args.chunks} | {sed_cmd} | {psql_copy}' for
                     chunk in range(1, self.args.chunks + 1)]

        else:
            tasks = [f"{dbgen_cmd} | {sed_cmd} | {psql_copy}"]

        return tasks
