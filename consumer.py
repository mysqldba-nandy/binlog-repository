from influxdb import InfluxDBClient
import base64
import time
import logging

from producer import queue
import config

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S", level=config.LOG_LEVEL)


class Consumer:
    def __init__(self):
        self.conn = InfluxDBClient(**config.INFLUX_CONN_SETTING)
        self.queue = queue
        self.rows = []
        self.timestamp = 0

    @staticmethod
    def _base64(value):
        return base64.b64encode(value.encode()).decode()

    def run(self):
        while True:
            try:
                binlog_rows = self.queue.popleft()
                self.timestamp = binlog_rows[0]['time']
                for i, binlog in enumerate(binlog_rows):
                    binlog['time'] = binlog['time'] * config.NANO + i
                    binlog['key'] = self._base64(binlog['key'])
                    binlog['data'] = self._base64(binlog['data'])
                    binlog['old'] = 'old' in binlog and self._base64(binlog['old']) or ''
                    binlog_rows[i] = {
                        "measurement": config.INFLUX_TABLE_POINT,
                        "time": binlog['time'],
                        "tags": {key: binlog[key] for key in ['file', 'database', 'table', 'type', 'xid']},
                        "fields": {key: binlog[key] for key in ['key', 'pos', 'end_log_pos', 'exec_time', 'data', 'old']}
                    }
                self.rows += binlog_rows
                logging.log(logging.DEBUG, 'timestamp:{} datetime:{} rows:{}'.format(
                    self.timestamp,
                    time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(self.timestamp)),
                    len(self.rows)
                ))
            except IndexError:
                time.sleep(1)
                logging.log(logging.INFO, 'queue is empty')
            finally:
                if len(self.rows) >= config.INFLUX_CACHE_SIZE or (len(self.rows) > 0 and len(self.queue) == 0):
                    # write binlog points
                    self.conn.write_points(self.rows, batch_size=config.INFLUX_BATCH_SIZE)
                    # write checkpoint
                    file = self.rows[-1]['tags']['file']
                    pos = self.rows[-1]['fields']['pos']
                    end_log_pos = self.rows[-1]['fields']['end_log_pos']
                    xid = self.rows[-1]['tags']['xid']
                    checkpoint = [{
                        "measurement": config.INFLUX_TABLE_CHECK,
                        "time": int(time.time() * config.NANO),
                        "tags": {'table': config.INFLUX_TABLE_POINT},
                        "fields": {'file': file, 'pos': pos, 'end_log_pos': end_log_pos, 'xid': xid, 'timestamp': self.timestamp}
                    }]
                    self.conn.write_points(checkpoint)
                    logging.log(logging.WARNING, 'timestamp:{} datetime:{} file:{} pos:{} end_log_pos:{} rows:{} remaining_events:{}'.format(
                        self.timestamp,
                        time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(self.timestamp)),
                        file,
                        self.rows[0]['fields']['pos'],
                        end_log_pos,
                        len(self.rows),
                        len(self.queue)
                    ))
                    # delete old checkpoint
                    sql = f'''
                        DELETE 
                        FROM "{config.INFLUX_TABLE_CHECK}" 
                        WHERE time<{checkpoint[0]['time']}
                        AND table='{config.INFLUX_TABLE_POINT}'
                    '''
                    self.conn.query(sql)
                    self.rows = []
