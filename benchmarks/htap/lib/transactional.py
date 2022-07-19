import time
from datetime import datetime
from collections import deque

from .helpers import Random, OLTPText, TimestampGenerator
from .helpers import MAX_ITEMS, DIST_PER_WARE, CUST_PER_DIST, NUM_ORDERS, STOCKS, NAMES


class TransactionalWorker:
    def __init__(self, seed, num_warehouses, latest_timestamp, conn, dry_run):
        self.conn = conn
        self.random = Random(seed)
        self.oltp_text = OLTPText(self.random)
        self.num_warehouses = num_warehouses
        self.dry_run = dry_run

        # the loader only generates timestamps for the orders table, and
        # generates a timestamp stream per warehouse.
        # here we generate a tsx for any warehouse and therefore have to scale
        # for both: 10/23 and warehouse-count. the 10/23 comes from next_transaction
        # and is the ratio between calls to new_order() and timestamp_generator.next()
        timestamp_scalar = (10 / 23.0) / self.num_warehouses

        self.timestamp_generator = TimestampGenerator(
            latest_timestamp, self.random, timestamp_scalar
        )
        self.ok_count = 0
        self.err_count = 0
        self.new_order_count = 0
        self.query_stats = deque()

    def add_stats(self, query, state, start, count):
        now = time.time()
        self.query_stats.append({'timestamp': now, 'query': query, 'status': state, 'runtime': now - start, 'count' : count})

    def stats(self):
        query_stats = self.query_stats
        self.query_stats = deque()
        return query_stats

    def other_ware(self, home_ware):
        if self.num_warehouses == 1:
            return home_ware

        while True:
            tmp = self.random.randint_inclusive(1, self.num_warehouses)
            if tmp != home_ware:
                return tmp

    def execute_sql(self, sql, args_seq, query_type, error=False):
        if self.dry_run:
            return
        start = time.time()
        # do not catch timeouts because we want that to stop the benchmark.
        # if we get timeouts the benchmark gets inbalanced and we eventually get
        # to a complete halt.
        self.conn.cursor.executemany(sql, args_seq)
        self.add_stats(query_type, 'ok', start, len(args_seq)) if not error else self.add_stats(query_type, 'error', start, len(args_seq))

    def new_order(self, timestamps, count):

        args_seq = []
        for idx in range(0, count-1):
            w_id = self.random.randint_inclusive(1, self.num_warehouses)
            d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
            c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
            order_line_count = self.random.randint_inclusive(5, 15)
            rbk = self.random.randint_inclusive(1, 100)
            itemid = []
            supware = []
            qty = []
            all_local = 1

            for order_line in range(1, order_line_count + 1):
                itemid.append(self.random.nurand(8191, 1, MAX_ITEMS))
                if (order_line == order_line_count - 1) and (rbk == 1):
                    itemid[-1] = -1

                if self.random.randint_inclusive(1, 100) != 1:
                    supware.append(w_id)
                else:
                    supware.append(self.other_ware(w_id))
                    all_local = 0

                qty.append(self.random.randint_inclusive(1, 10))

            args = (w_id, c_id, d_id, order_line_count, all_local, itemid, supware, qty, timestamps[idx])
            args_seq.append(args)

        sql = 'CALL new_order(%t::integer, %t::integer, %t::integer, %t::integer, %t::integer, %t::integer array, %t::integer array, %t::integer array, %t::timestamptz)'

        # rolled back or commit tsxs they both count
        self.new_order_count += count
        self.execute_sql(sql, args_seq, 'new_order', rbk == 1)

    def payment(self, timestamps, count):
        args_seq = []

        for idx in range(0, count-1):
            w_id = self.random.randint_inclusive(1, self.num_warehouses)
            d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
            c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
            h_amount = self.random.randint_inclusive(1, 5000)
            c_last = self.oltp_text.lastname(self.random.nurand(255, 0, 999))

            byname = self.random.randint_inclusive(1, 100) <= 60
            if self.random.randint_inclusive(1, 100) <= 85:
                c_w_id = w_id
                c_d_id = d_id
            else:
                c_w_id = self.other_ware(w_id)
                c_d_id = self.random.randint_inclusive(1, DIST_PER_WARE)

            args = (w_id, d_id, c_d_id, c_id, c_w_id, h_amount, byname, c_last, timestamps[idx])
            args_seq.append(args)

        sql = 'CALL payment(%t, %t, %t, %t, %t, %t::numeric(12,2), %t, %t::varchar(16), %t::timestamptz)'
        self.execute_sql(sql, args_seq, 'payment')

    def order_status(self, count):
        args_seq = []

        for idx in range(0, count-1):
            w_id = self.random.randint_inclusive(1, self.num_warehouses)
            d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
            c_id = self.random.nurand(1023, 1, CUST_PER_DIST)
            c_last = self.oltp_text.lastname(self.random.nurand(255, 0, 999))
            byname = self.random.randint_inclusive(1, 100) <= 60

            args = (w_id, d_id, c_id, c_last, byname)
            args_seq.append(args)

        sql = 'CALL order_status(%t::integer, %t::integer, %t::integer, %t::varchar(24), %t::boolean)'
        self.execute_sql(sql, args_seq, 'order_status')

    def delivery(self, timestamps, count):
        args_seq = []

        for idx in range(0, count-1):
            w_id = self.random.randint_inclusive(1, self.num_warehouses)
            o_carrier_id = self.random.randint_inclusive(1, 10)

            args = (w_id, o_carrier_id, DIST_PER_WARE, timestamps[idx])
            args_seq.append(args)

        sql = 'CALL delivery(%t, %t, %t, %t::timestamptz)'
        self.execute_sql(sql, args_seq, 'delivery')

    def stock_level(self, count):
        args_seq = []

        for idx in range(0, count-1):
            w_id = self.random.randint_inclusive(1, self.num_warehouses)
            d_id = self.random.randint_inclusive(1, DIST_PER_WARE)
            level = self.random.randint_inclusive(10, 20)

            args = (w_id, d_id, level)
            args_seq.append(args)

        sql = 'CALL stock_level(%t, %t, %t)'
        self.execute_sql(sql, args_seq, 'stock_level')

    def next_transaction(self):
        count = 100
        timestamps_to_use = []
        for idx in range(1, count):
            timestamps_to_use.append(self.timestamp_generator.next())
        # WARNING: keep in sync with initialization of scalar of timestamp generator!
        trx_type = self.random.randint_inclusive(1, 23)
        if trx_type <= 10:
            self.new_order(timestamps_to_use, count)
        elif trx_type <= 20:
            self.payment(timestamps_to_use, count)
        elif trx_type <= 21:
            self.order_status(count)
        elif trx_type <= 22:
            self.delivery(timestamps_to_use, count)
        elif trx_type <= 23:
            self.stock_level(count)
