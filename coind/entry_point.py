import click
from procedure import index
import coind.framework as fw
import os


RETRY = click.option("-r", "--retry", default=3, type=click.INT)
START = click.option("-s", "--start", default=None, type=click.STRING)
END = click.option("-e", "--end", default=None, type=click.STRING)
CONTRACTS = click.option("-c", "--contracts", default=None, type=click.STRING)


@click.command("pull")
@click.argument("dates", nargs=-1)
@RETRY
def index_pull(dates, retry=3):
    dates = list(dates)
    index.pull(dates, retry)


@click.command("create")
@START
@END
@CONTRACTS
def index_create(start, end, contracts):
    if contracts is None:
        contracts = fw.conf.targets()
    else:
        contracts = contracts.split(",")

    if end is None:
        end = recent_date()
 
    if start is None:
        start = fw.index.last_date()
    
    index.create(index.date_range(start, end), contracts)


@click.command("update")
@click.argument("end", nargs=1, default=None, type=click.INT, required=False)
def index_update(end):
    if end is None:
        end = recent_date()
    index.update(end)

    
    
def recent_date():
    from datetime import datetime, timedelta
    return (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")


index_group = click.Group(
    "index", 
    {"create": index_create,
     "update": index_update,
     "pull": index_pull}
)


entry = click.Group(
    "coind",
    {"index": index_group}
)


fw.init(os.environ.get("CONFIG", "/conf/config.yml"))


def main():
    entry()


if __name__ == '__main__':
    main()