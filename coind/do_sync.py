from coind.IO.sync import sync, SyncMapper
from coind.utils.recycle import Recycler
import yaml
import click


def sync_data(mapper, key="datetime", chunk=1000, times=1):
    recycler = Recycler(sync)
    sm = SyncMapper(**mapper)
    params = [(source, target, key, chunk) for source, target in sm.pairs()]
    recycler.run(params, times)


@click.command()
@click.argument('filename', nargs=1)
def command(filename):
    with open(filename) as f:
        config = yaml.load(f)
    sync_data(**config)


if __name__ == '__main__':
    command()