FROM registry.docker-cn.com/library/python:3.6

RUN apt-get update
RUN apt-get install -y cron libsnappy-dev
RUN echo 'Asia/Shanghai' >/etc/timezone & cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

ENV LC_ALL="C.UTF-8" LANG="C.UTF-8" PYTHONPATH=/CoinData:$PYTHONPATH
WORKDIR /CoinData

COPY requirements.txt ./
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY . ./

RUN ln -s /CoinData/routing/env.sh /etc/profile.d/env.sh

RUN crontab /CoinData/routing/timelist

VOLUME ["/data", "/conf", "/log"]

CMD ["/usr/sbin/cron" "-f"]