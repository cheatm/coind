from coind.framework import mongodb
from coind.framework.base import BaseConf, ContractDaily, DailyIndex


_frame_types = {
    "mongodb": mongodb
}

conf = BaseConf()
contracts = ContractDaily()
index = DailyIndex()


def init(conf):
    if isinstance(conf, str):
        import yaml
        with open(conf) as f:
            conf = yaml.load(f)

    _type = conf.get("type")
    module = _frame_types[_type]
    globals().update(
        module.generate(conf)
    )
 