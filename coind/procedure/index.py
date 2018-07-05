from coind.IO.contracts import get_contracts
from coind.framework.base import ContractDaily, DailyIndex, BaseConf
import coind.framework as framework
from collections import Iterable
import logging
import pandas as pd


def pull(dates, retry=3):
    if isinstance(dates, str):
        dates = dates.split(",")
    
    framework.contracts.create(dates)
    for i in range(retry):
        for date in framework.contracts.empty(dates):
            _pull(date)
        if len(framework.contracts.empty(dates)) == 0:
            return


def _pull(date):
    contracts = get_contracts(date)
    framework.contracts.set(date, contracts)
    logging.warning("contracts | %s | %s", date, len(contracts))


def create(dates, contracts):
    if isinstance(dates, str):
        dates = dates.split(",")
    pull(dates)

    if isinstance(contracts, str):
        contracts = contracts.split(",")
    contracts = set(contracts)

    for date in dates:
        all_contracts = framework.contracts.get_contracts(date)
        valid_contracts = all_contracts.intersection(contracts)
        framework.index.create(date, valid_contracts)
        logging.warning("index | %s | %s", date, len(valid_contracts))
    

def date_range(start, end):
    return list(
        pd.date_range(start, end).map(lambda t: t.strftime("%Y-%m-%d"))
    )


def update(end):
    start = framework.index.last_date()
    dates = date_range(start, end)[1:]
    create(dates, framework.conf.targets())
    return dates


def main():
    import yaml

    config = yaml.load(open(r"D:\CoinData\conf\mongodb-conf.yml"))
    framework.init(config)
    update("2018-06-10")


if __name__ == '__main__':
    main()