FROM python:3.7-slim-stretch

WORKDIR /app

ADD . /app

RUN echo " \
        deb http://mirrors.aliyun.com/debian/ stretch main non-free contrib \n\
        deb-src http://mirrors.aliyun.com/debian/ stretch main non-free contrib \n\
        deb http://mirrors.aliyun.com/debian-security stretch/updates main \n\
        deb-src http://mirrors.aliyun.com/debian-security stretch/updates main \n\
        deb http://mirrors.aliyun.com/debian/ stretch-updates main non-free contrib \n\
        deb-src http://mirrors.aliyun.com/debian/ stretch-updates main non-free contrib \n\
        deb http://mirrors.aliyun.com/debian/ stretch-backports main non-free contrib \n\
        deb-src http://mirrors.aliyun.com/debian/ stretch-backports main non-free contrib \n\
        " \
        > /etc/apt/sources.list \
    && buildDeps="git" \
    && apt-get update && apt-get install -y --no-install-recommends --allow-unauthenticated $buildDeps \
    && cp -f /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo 'Asia/Shanghai' > /etc/timezone \
    && pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt \
    && git clone https://github.com/nloneday/python-mysql-replication.git && cd python-mysql-replication \
    && python setup.py build && python setup.py install \
    && cd .. && rm -rf python-mysql-replication \
    && apt-get purge -y --auto-remove $buildDeps

CMD ["python", "run.py"]
