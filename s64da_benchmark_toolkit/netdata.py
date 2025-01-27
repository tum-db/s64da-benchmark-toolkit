import os
import logging

import requests
import pandas

from dateutil.tz import tzlocal
from natsort import natsorted

LOG = logging.getLogger()


class Netdata:
    def __init__(self, config):
        self.url = f"{config['url']}/api/v1/data"
        self.metrics = config['metrics']
        self.charts = config['charts']

    def _get_data(self, timerange, resolution):
        data = pandas.DataFrame()
        for chart, dimensions in self.charts.items():
            response = requests.get(self.url, params={
                'chart': chart,
                'after': timerange[0],
                'before': timerange[1],
                'dimensions': ','.join(dimensions),
                'gtime': resolution
            })

            status_code = response.status_code
            if status_code != 200:
                LOG.warning(f'Netdata response not 200, but {status_code} '
                            f'for chart {chart}.')
                LOG.warning(response.text)
                LOG.warning(timerange)
                continue

            result = response.json()

            columns = ['time']
            columns.extend([f'{chart}.{dimension}' for dimension in dimensions])
            columns = [column.replace('.', '_') for column in columns]

            df = pandas.DataFrame(result['data'], columns=columns)
            df = df.set_index('time')
            data = pandas.concat([data, df], axis=1)

        data.index = pandas.to_datetime(data.index, unit='s')

        return data

    @classmethod
    def make_timestamp(cls, value):
        ts = int(value.tz_localize(tzlocal()).timestamp())
        return ts

    def _get_netdata_per_query(self, df, output):
        data = {}
        for _, row in df.iterrows():
            if row['status'] == 'IGNORED':
                continue

            timerange = (
                Netdata.make_timestamp(row['timestamp_start']),
                Netdata.make_timestamp(row['timestamp_stop'])
            )

            netdata_df = self._get_data(timerange, 1)

            query_id = row['query_id']
            data[query_id] = netdata_df.agg(self.metrics)

        return data

    def get_system_stats(self, df, resolution):
        ts_from = Netdata.make_timestamp(df['timestamp_start'].min())
        ts_to = Netdata.make_timestamp(df['timestamp_stop'].max())
        return self._get_data((ts_from, ts_to), resolution).sort_index()

    def _write_stats_per_query(self, df, output):
        data = self._get_netdata_per_query(df, output)
        query_ids = natsorted(data.keys())

        with open(output, 'w') as output_file:
            for query_id in query_ids:
                output_file.write(f'{query_id}\n')
                data[query_id].to_csv(output_file, header=False)
                output_file.write('\n')

    def _write_stats_no_breakdown(self, df, output):
        netdata_df = self.get_system_stats(df, 1)
        with open(output, 'w') as output_file:
            netdata_df.to_csv(output_file)
            output_file.write('\n')

    def write_stats(self, df, output):
        if len(df['stream_id'].unique()) == 1:
            flname, flext = os.path.splitext(output)
            self._write_stats_per_query(df, f'{flname}_single_stream{flext}')
        else:
            LOG.info('Running more than one stream. Netdata stats are written '
                     'out without analysis.')

        self._write_stats_no_breakdown(df, output)

    def is_netdata_set_and_running(self):
        try:
            response = requests.get(f'{self.url}?chart=system.cpu')
            status_code = response.status_code
            if status_code != 200:
                LOG.warning(f'Netdata url response ({self.url}) does not return 200, but {status_code}')
                LOG.warning(response.text)
                return False

        except requests.exceptions.ConnectionError as e:
            LOG.warning(f'Host has no Netdata running on the given URL: {self.url}')
            LOG.info(f'Please make sure Netdata is running, or check if the URL provided is correct')
            LOG.info(f'Or remove netdata from the configs if its not going to be used')
            return False
        return True
