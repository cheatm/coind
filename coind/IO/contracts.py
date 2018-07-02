import requests
from datautils.tools.logger import logger
import json
import pandas as pd


CONTRACTS = "http://alihz-net-0.qbtrade.org/contracts?date=%s"


def contracts_url(date):
    return CONTRACTS % date


@logger("contracts", 1, success="debug", default=dict)
def get_contracts(date):
    response = requests.get(contracts_url(date))
    assert response.status_code == 200, response.status_code
    return json.loads(response.content)


def valid_contracts(contracts, date):
    source = get_contracts(date)
    return dict.fromkeys(
        find_contracts(contracts, source),
        1
    )


def find_contracts(target, source):
    for c in target:
        if c in source:
            yield c


def list_contracts(dates, contracts):
    valids = [valid_contracts(contracts, date) for date in dates]
    return pd.DataFrame(valids, dates)