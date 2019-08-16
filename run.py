from threading import Thread
from multiprocessing import Process
from influxdb import InfluxDBClient

from producer import Producer
from consumer import Consumer
from web import app
import config


def run():
    conn = InfluxDBClient(**config.INFLUX_CONN_SETTING)
    conn.create_database(config.INFLUX_CONN_SETTING['database'])
    conn.create_retention_policy(
        name=f"{config.influx_config['database']}_policy",
        duration=config.influx_config['retention'],
        replication='1',
        database=config.influx_config['database']
    )
    conn.close()
    Thread(target=Producer().run).start()
    Thread(target=Consumer().run).start()


if __name__ == '__main__':
    Process(target=run).start()
    app.run(host='0.0.0.0', port=config.PORT)
