import sys
import logging
import time
import json
from collections import deque

import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import (
    RotateEvent,
    GtidEvent,
    QueryEvent,
    XidEvent
)
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)
from influxdb import DataFrameClient

import config

queue = deque()
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S", level=config.LOG_LEVEL)


class Producer:
    def __init__(self):
        self.conn_setting = config.MYSQL_CONN_SETTING  # {'host': '127.0.0.1', 'port': 3306, 'user': 'user', 'password': 'password'}
        self.log_file = config.MYSQL_LOG_FILE  # start binlog file, default current
        self.log_pos = 4  # start binlog position, default 4
        self.connection = pymysql.connect(**self.conn_setting)
        self.cursor = self.connection.cursor()
        self.server_id = None
        self.binlog_format = 'ROW'
        self._check_config()
        self._check_influx()

    def _check_config(self):
        with self.connection as cursor:
            # server id
            cursor.execute("SELECT @@server_id")
            self.server_id = cursor.fetchone()[0]
            if self.server_id is None:
                logging.log(logging.ERROR, 'server_id is {}'.format(self.server_id))
                sys.exit(-1)
            else:
                logging.log(logging.INFO, 'server_id is {}'.format(self.server_id))

            # binlog file
            cursor.execute("SHOW MASTER LOGS")
            for log_name, _ in cursor.fetchall():
                if self.log_file == log_name:
                    logging.log(logging.INFO, 'log_file is {}'.format(self.log_file))
                    break
            else:
                logging.log(logging.WARNING, 'specific log_file {} is missing, use current {}'.format(self.log_file, log_name))
                self.log_file = log_name

            # row format
            cursor.execute("SELECT @@binlog_format")
            binlog_format = cursor.fetchone()[0]
            if self.binlog_format != binlog_format:
                logging.log(logging.WARNING, 'binlog_format is {}, should be {}'.format(binlog_format, self.binlog_format))
                self.binlog_format = binlog_format
            else:
                logging.log(logging.INFO, 'binlog_format is {}'.format(self.binlog_format))

    def _check_influx(self):
        conn = DataFrameClient(**config.INFLUX_CONN_SETTING)
        sql = f'''SELECT * FROM "{config.INFLUX_TABLE_CHECK}" WHERE table='{config.INFLUX_TABLE_POINT}' ORDER BY time DESC LIMIT 1'''
        row = conn.query(sql)
        if row:
            row = row[config.INFLUX_TABLE_CHECK]
            self.log_file = row['file'].values[0]
            self.log_pos = row['end_log_pos'].values[0]
            logging.log(logging.WARNING, 'found binlog check table, now log_file is {} and restart pos is {}'.format(self.log_file, self.log_pos))
        conn.close()
        return row

    def run(self):
        stream = BinLogStreamReader(connection_settings=self.conn_setting, server_id=self.server_id, log_file=self.log_file,
                                    log_pos=self.log_pos, resume_stream=True, blocking=True)
        logging.log(logging.INFO, 'parse is starting!')
        binlog_rows = []
        log_file, log_pos, exec_time = self.log_file, 0, 0
        for binlog_event in stream:
            if isinstance(binlog_event, RotateEvent):
                log_file = binlog_event.next_binlog
                logging.log(logging.WARNING, 'log_file:{}'.format(
                    log_file
                ))
            elif isinstance(binlog_event, GtidEvent):
                logging.log(logging.INFO, 'timestamp:{} datetime:{} position:{}, transaction begin'.format(
                    binlog_event.timestamp,
                    time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(binlog_event.timestamp)),
                    binlog_event.packet.log_pos
                ))
            elif isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                exec_time = binlog_event.execution_time
            elif isinstance(binlog_event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                dml_type = self._dml_type(binlog_event)
                for row in binlog_event.rows:
                    binlog = {
                        'time': binlog_event.timestamp,
                        'file': log_file,
                        'database': binlog_event.schema,
                        'table': binlog_event.table,
                        'type': dml_type,
                        'key': self._handle_pk(binlog_event.primary_key),
                        'pos': log_pos,
                        'end_log_pos': binlog_event.packet.log_pos,
                        'exec_time': exec_time
                    }
                    if row.get('values'):
                        binlog['data'] = json.dumps(row['values'], ensure_ascii=False)
                    else:
                        binlog['data'] = json.dumps(row['after_values'], ensure_ascii=False)
                        binlog['old'] = json.dumps(row['before_values'], ensure_ascii=False)
                    binlog_rows.append(binlog)
            elif isinstance(binlog_event, QueryEvent) and binlog_event.query not in ('BEGIN', 'COMMIT'):
                binlog = {
                    'time': binlog_event.timestamp,
                    'file': log_file,
                    'database': binlog_event.schema and binlog_event.schema.decode() or '',
                    'table': '',
                    'type': 'DDL',
                    'key': self._handle_pk(''),
                    'pos': log_pos,
                    'end_log_pos': binlog_event.packet.log_pos,
                    'exec_time': exec_time,
                    'data': binlog_event.query
                }
                binlog_rows.append(binlog)
            elif isinstance(binlog_event, XidEvent):
                for binlog in binlog_rows:
                    binlog['xid'] = binlog_event.xid
                    logging.log(logging.DEBUG, binlog)
                if binlog_rows:
                    queue.append(binlog_rows)
                    logging.log(logging.INFO, 'timestamp:{} datetime:{} position:{} xid:{} rows:{} exec:{}s, transaction commit '.format(
                        binlog_event.timestamp + exec_time,
                        time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(binlog_event.timestamp + exec_time)),
                        binlog_event.packet.log_pos,
                        binlog_event.xid,
                        len(binlog_rows),
                        exec_time
                    ))
                    binlog_rows = []
            log_pos = binlog_event.packet.log_pos

    @staticmethod
    def _dml_type(event):
        if isinstance(event, WriteRowsEvent):
            return 'INSERT'
        elif isinstance(event, UpdateRowsEvent):
            return 'UPDATE'
        elif isinstance(event, DeleteRowsEvent):
            return 'DELETE'

    @staticmethod
    def _handle_pk(pk):
        if pk:
            if isinstance(pk, str):
                pk = [pk]
            else:
                pk = list(pk)
        else:
            pk = []
        return json.dumps(pk, ensure_ascii=False)


if __name__ == '__main__':
    producer = Producer()
    producer.run()
