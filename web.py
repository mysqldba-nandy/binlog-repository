from flask import Flask, request, jsonify, redirect
from flask_compress import Compress
import influxdb
import base64
import json
import warnings
import pytz
import re

import config

warnings.filterwarnings('ignore')
app = Flask(__name__)
Compress(app)
BOOL = {'true': True, 'false': False, 'UNDO': True, 'REDO': False, 'DDL': None}


def influx(sql):
    conn = influxdb.DataFrameClient(**config.INFLUX_CONN_SETTING)
    rows = conn.query(sql)
    if rows:
        rows = rows[config.INFLUX_TABLE_POINT]
    conn.close()
    return rows


@app.route("/")
def index():
    return redirect('/static/index.html')


@app.route("/files", methods=['POST'])
def files():
    sql = f'SHOW TAG VALUES FROM "{config.INFLUX_TABLE_POINT}" WITH KEY="file"'
    rows = influx(sql)
    rows = [row['value'] for row in list(rows)]
    return jsonify(rows)


@app.route("/databases", methods=['POST'])
def databases():
    file = request.form.get('file')
    sql = f"""SHOW TAG VALUES FROM "{config.INFLUX_TABLE_POINT}" WITH KEY="database" WHERE "file"='{file}'"""
    rows = influx(sql)
    rows = [row['value'] for row in list(rows)]
    return jsonify(rows)


@app.route("/tables", methods=['POST'])
def tables():
    file = request.form.get('file')
    database = request.form.get('database')
    sql = f"""SHOW TAG VALUES FROM "{config.INFLUX_TABLE_POINT}" WITH KEY="table" WHERE "file"='{file}' AND "database"='{database}'"""
    rows = influx(sql)
    rows = [row['value'] for row in list(rows)]
    return jsonify(rows)


@app.route("/binlogs", methods=['POST'])
def binlogs():
    timezone = pytz.FixedOffset(int(request.form.get('timezone')) * 60)
    rows, func, use_pk, no_pk = fetch_all()
    sqls, regex = [], re.compile(r'\.\d+')
    if len(rows):
        rows.index = rows.index.map(timezone.normalize)
        rows['data'] = rows['data'].apply(_base64_decode)
        rows['key'] = rows['key'].apply(_base64_decode).apply(json.loads)
        for time_index, row in rows.iterrows():
            row = row.to_dict()
            sql = func(row, use_pk=use_pk, no_pk=no_pk)
            annotation = {
                'position': [row['pos'], row['end_log_pos']],
                'timestamp': time_index.value // config.NANO,
                'datetime': regex.sub('', str(time_index)),
                'xid': row['xid']
            }
            if config.OUTPUT_ROWS_EXTRA:
                sql = sql + f" # {json.dumps(annotation)}"
            sqls.append(sql)
    return jsonify(sqls)


def fetch_all():
    file = request.form.get('file')
    database = request.form.get('database')
    start_time = request.form.get('start_time')
    stop_time = request.form.get('stop_time')
    start_position = request.form.get('start_position')
    stop_position = request.form.get('stop_position')
    extend = ''
    if start_time:
        extend += f' AND time>={int(start_time) * config.NANO}'
    if stop_time:
        extend += f' AND time<{(int(stop_time) + 1) * config.NANO}'
    if start_position:
        extend += f' AND "pos">={int(start_position)}'
    if stop_position:
        extend += f' AND "end_log_pos"<={int(stop_position)}'
    output_type = BOOL[request.form.get('output_type')]
    if output_type is None:
        use_pk, no_pk = None, None
        sql = f"""
            SELECT 
              * 
            FROM "{config.INFLUX_TABLE_POINT}" 
            WHERE "file"='{file}' 
            AND "database"='{database}'
            AND "type"='DDL'
            {extend}
            ORDER BY time
            LIMIT {config.OUTPUT_ROWS_LIMIT}
            """
    else:
        table = request.form.get('table')
        sql_type = json.loads(request.form.get('sql_type'))
        sql_type = ' OR '.join(f""""type"='{_type}'""" for _type in sql_type)
        use_pk, no_pk = BOOL[request.form.get('use_pk')], BOOL[request.form.get('no_pk')]
        sql = f"""
            SELECT 
              * 
            FROM "{config.INFLUX_TABLE_POINT}" 
            WHERE "file"='{file}' 
            AND "database"='{database}' 
            AND "table"='{table}' 
            AND ({sql_type})
            {extend}
            ORDER BY time {output_type and 'DESC' or 'ASC'}
            LIMIT {config.OUTPUT_ROWS_LIMIT}
            """
    rows = influx(sql)
    if output_type is None:
        func = ddl_sql
    elif output_type:
        func = undo_sql
        if len(rows):
            rows = rows.sort_index(ascending=False)
    else:
        func = redo_sql
    return rows, func, use_pk, no_pk


def undo_sql(row, use_pk=False, no_pk=False):
    row['data'] = json.loads(row['data'])
    if row['type'] == 'INSERT':
        if use_pk and row['key']:
            sql = 'DELETE FROM `{0}`.`{1}` WHERE {2};'.format(
                row['database'], row['table'],
                ' AND '.join(_convert_key_value(k=k, v=row["data"][k], is_null=True) for k in row['key'])
            )
        else:
            sql = 'DELETE FROM `{0}`.`{1}` WHERE {2};'.format(
                row['database'], row['table'],
                ' AND '.join(_convert_key_value(k=k, v=row["data"][k], is_null=True) for k in row['data'])
            )
    elif row['type'] == 'DELETE':
        if no_pk and row['key']:
            [row['data'].pop(key) for key in row['key']]
            sql = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                row['database'], row['table'],
                ', '.join(f'`{k}`' for k in row['data'].keys()),
                ', '.join(_convert_value(v=v) for v in row['data'].values())
            )
        else:
            sql = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                row['database'], row['table'],
                ', '.join(f'`{k}`' for k in row['data'].keys()),
                ', '.join(_convert_value(v=v) for v in row['data'].values())
            )
    elif row['type'] == 'UPDATE':
        row['old'] = json.loads(_base64_decode(row['old']))
        if use_pk and row['key']:
            sql = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3};'.format(
                row['database'], row['table'],
                ', '.join(f'{_convert_key_value(k=k, v=row["old"][k], is_null=False)}' for k in row['old']),
                ' AND '.join(f'{_convert_key_value(k=k, v=row["data"][k], is_null=True)}' for k in row['key'])
            )
        else:
            sql = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3};'.format(
                row['database'], row['table'],
                ', '.join(f'{_convert_key_value(k=k, v=v, is_null=False)}' for k, v in row['old'].items()),
                ' AND '.join(f'{_convert_key_value(k=k, v=v, is_null=True)}' for k, v in row['data'].items())
            )
    else:
        sql = row['data'] + ';'
    return sql


def redo_sql(row, use_pk=False, no_pk=False):
    row['data'] = json.loads(row['data'])
    if row['type'] == 'DELETE':
        if use_pk and row['key']:
            sql = 'DELETE FROM `{0}`.`{1}` WHERE {2};'.format(
                row['database'], row['table'],
                ' AND '.join(f'{_convert_key_value(k=k, v=row["data"][k], is_null=True)}' for k in row['key'])
            )
        else:
            sql = 'DELETE FROM `{0}`.`{1}` WHERE {2};'.format(
                row['database'], row['table'],
                ' AND '.join(f'{_convert_key_value(k=k, v=row["data"][k], is_null=True)}' for k in row['data'])
            )
    elif row['type'] == 'INSERT':
        if no_pk and row['key']:
            [row['data'].pop(key) for key in row['key']]
            sql = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                row['database'], row['table'],
                ', '.join(f'`{k}`' for k in row['data'].keys()),
                ', '.join(_convert_value(v=v) for v in row['data'].values())
            )
        else:
            sql = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                row['database'], row['table'],
                ', '.join(f'`{k}`' for k in row['data'].keys()),
                ', '.join(_convert_value(v=v) for v in row['data'].values())
            )
    elif row['type'] == 'UPDATE':
        row['old'] = json.loads(_base64_decode(row['old']))
        if use_pk and row['key']:
            sql = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3};'.format(
                row['database'], row['table'],
                ', '.join(_convert_key_value(k=k, v=row["data"][k], is_null=False) for k in row['data']),
                ' AND '.join(_convert_key_value(k=k, v=row["old"][k], is_null=True) for k in row['key'])
            )
        else:
            sql = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3};'.format(
                row['database'], row['table'],
                ', '.join(_convert_key_value(k=k, v=v, is_null=False) for k, v in row['data'].items()),
                ' AND '.join(_convert_key_value(k=k, v=v, is_null=True) for k, v in row['old'].items())
            )
    else:
        sql = row['data'] + ';'
    return sql


def ddl_sql(row, use_pk=False, no_pk=False):
    sql = row['data'] + ';'
    return sql


def _base64_decode(value):
    return base64.b64decode(value).decode()


def _convert_key_value(k, v, is_null=True):
    converted_v = _convert_value(v)
    if v is None and is_null:
        return f'`{k}` IS {converted_v}'
    else:
        return f'`{k}`={converted_v}'


def _convert_value(v):
    if v is None:
        v = 'NULL'
    elif isinstance(v, (list, dict)):
        v = f"'{json.dumps(v, ensure_ascii=False)}'"
    elif isinstance(v, str):
        v = f"'{v}'"
    else:
        v = str(v)
    return v


if __name__ == '__main__':
    app.run(port=3000)
