import configparser

parser = configparser.ConfigParser(inline_comment_prefixes='#')
parser.read('project.ini')


def config(group, key=None):
    if key:
        return parser[group][key]
    else:
        return dict(parser[group])


mysql_config = config('mysql')
influx_config = config('influx')
web_config = config('web')
other_config = config('other')

# mysql
MYSQL_CONN_SETTING = {
    'host': mysql_config['host'],
    'port': int(mysql_config['port']),
    'user': mysql_config['username'],
    'password': mysql_config['password'],
    'charset': mysql_config['charset']
}
MYSQL_LOG_FILE = mysql_config['binlog-file']

# influx
INFLUX_CONN_SETTING = {
    'host': influx_config['host'],
    'port': int(influx_config['port']),
    'username': influx_config['username'],
    'password': influx_config['password'],
    'database': influx_config['database'],
}
INFLUX_TABLE_CHECK = influx_config['check-table']
INFLUX_TABLE_POINT = f"binlog_{mysql_config['host']}_{mysql_config['port']}"
INFLUX_CACHE_SIZE = int(influx_config['cache-size'])
INFLUX_BATCH_SIZE = int(influx_config['batch-size'])
NANO = 10 ** 9  # const

# web
PORT = web_config['port']
OUTPUT_ROWS_LIMIT = int(web_config['output-size'])

# other
LOG_LEVEL = other_config['log-level']

if __name__ == '__main__':
    print(config('mysql'))
