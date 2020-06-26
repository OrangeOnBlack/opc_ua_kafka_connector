FROM ubuntu:xenial

RUN apt-get update && apt-get install -y \
    python3-pip \
    iputils-ping \
    libsnappy-dev

ENV LC_ALL C.UTF-8

RUN pip3 install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

ENV OPCUA_SERVER localhost
ENV OPCUA_PORT 4840
ENV KAFKA_SERVER 192.168.0.7
ENV KAFKA_PORT 9092
ENV KAFKA_TOPIC my-topic
ENV SLEEP_DURATION 1
ENV OUT_FILE /respone_time_measurements/response
#ENV COMPRESSION snappy

COPY opc_ua_kafka_connector.py opc_ua_kafka_connector.py
COPY start.sh start.sh
RUN chmod +x start.sh
CMD /start.sh
