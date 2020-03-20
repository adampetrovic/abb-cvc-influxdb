from influxdb import InfluxDBClient
from datetime import datetime, timedelta
from dateutil import parser
import click
import requests
import yaml
import time

CVC_JSON_URL = 'http://akashabyronbay.com/abbcvcs/?cvcs%5B%5D={cvc}&numberofdays={num_days}&priortodate={start_date}&json='
MEASUREMENT_INTERVAL = 5 # minutes

def generate_dates(start, num_days, interval):
    curr = start
    end = start - timedelta(days=num_days)
    interval = timedelta(days=interval)
    while curr > end:
        yield curr
        curr -= interval


def get_cvc_json(cvcs, num_days, start_date=None):
    if not start_date:
        start_date = datetime.now()

    if num_days > 30:
        dates = [d for d in generate_dates(start_date, num_days, 30)]
        num_days = 30
    else:
        dates = [start_date]

    for date in dates:
        resp_json = requests.get(
            CVC_JSON_URL.format(
                cvc=','.join(cvcs),
                num_days=num_days,
                start_date=date.strftime("%Y.%m.%d"),
            )
        )
        if resp_json.ok:
            yield resp_json.json()

        # sleep for 1 second so we don't smash the server
        time.sleep(1)

def json_to_influx_payload(payload):
    metrics = []
    speeds = zip(payload['download'], payload['upload'])
    base_time = parser.parse("{} 00:00:00 {}".format(payload['date'], payload['timezone']))
    delta = timedelta(minutes=5)
    for idx, (download, upload) in enumerate(speeds):
        timestamp = base_time + (delta * idx)
        metrics.append({
            "measurement": "aussiebb.speed",
            "tags": {
                "cvc": payload['slug'],
            },
            "time": base_time + (delta * idx),
            "fields": {
                "upload": upload,
                "download": download,
                "capacity": payload['cvc'],
            }
        })
    return metrics

@click.command()
@click.option('--config', type=click.Path(exists=True), required=True, help='Configuration file (see example.yml).')
def main(config):
    with open(config) as config_file:
        config = yaml.full_load(config_file)

    check_interval = config.get('check_interval', 86400)
    if not check_interval:
        click.echo("check_interval is not set in config file.", err=True)
        return

    client = InfluxDBClient(
        host=config['influxdb']['host'],
        port=config['influxdb']['port'],
        database='aussiebb',
    )
    client.create_database('aussiebb')

    while True:
        cvcs = config.get('cvcs')
        num_days = int(config.get('num_days'))
        for batch in get_cvc_json(cvcs, num_days):
            for day in batch:
                metrics = json_to_influx_payload(day)
                # submit metrics
                client.write_points(metrics)
                click.echo("wrote {} data points".format(len(metrics)))
        time.sleep(check_interval)

if __name__ == '__main__':
    main()