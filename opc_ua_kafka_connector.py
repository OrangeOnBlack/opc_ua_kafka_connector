'''A simple OPC UA client which reads values from the OPC UA server and publishes them to a Apache Kafka Cluster'''

import time
from opcua import Client
from kafka import KafkaConsumer, KafkaProducer
import json
import sys
import os
from datetime import datetime
import threading
from queue import Queue
import _thread

INSTANCE_NAME = os.environ.get('INSTANCE_NAME', 'MY-INSTACE')
OPCUA_SERVER = os.environ.get('OPCUA_SERVER', 'localhost')
OPCUA_PORT = os.environ.get('OPCUA_PORT', '4840')
KAFKA_SERVER = os.environ.get('KAFKA_SERVER', '192.168.0.7')
KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'my-topic')
SLEEP_DURATION = os.environ.get('SLEEP_DURATION', '1') # seconds sleep after each update written to kafka
OUT_FILE = os.environ.get('OUT_FILE', 'response_time.json')
NUMBER_MESSAGES = os.environ.get('NUMBER_MESSAGES', 10)

number_messages = 0
q = Queue()
file = open(OUT_FILE, "w")

kafka_consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers='%s:%s' % (KAFKA_SERVER, KAFKA_PORT),
                             # auto_offset_reset='earliest',
                             consumer_timeout_ms=30000)

def _monitor_response_time(kafka_consumer, q):
    global number_messages
    while True:
        try:
            message = next(kafka_consumer)
            timestamp_received = datetime.now()
            msg = json.loads(message.value.decode('utf-8'))
            timestamp_sent = datetime.strptime(str(msg['TimestampSent']), '%Y-%m-%d %H:%M:%S.%f')
            response_time = timestamp_received - timestamp_sent
            print("Response time: %f seconds" % response_time.total_seconds())
            q.put(response_time.total_seconds())
            number_messages += 1
            if int(NUMBER_MESSAGES) > 0:
                if number_messages >= int(NUMBER_MESSAGES):
                    _thread.interrupt_main()
                    sys.exit()
        except StopIteration:
            pass
        # except Exception:
        #     pass


if __name__== "__main__":

    thread = threading.Thread(target=_monitor_response_time, args=(kafka_consumer, q,))
    thread.daemon = True
    thread.start()

    opcua_client = Client("opc.tcp://%s:%s/freeopcua/server/" % (OPCUA_SERVER, OPCUA_PORT))
    opcua_client.connect()
    print('Connected to OPC UA server %s:%s' % (OPCUA_SERVER, OPCUA_PORT))
    root = opcua_client.get_root_node()
    imms = root.get_child(["0:Objects", "2:IMMS"])

    kafka_producer = KafkaProducer(bootstrap_servers='%s:%s' % (KAFKA_SERVER, KAFKA_PORT),
                                retries=5,
                                batch_size=0,
                                compression_type=None)
                                # compression_type='gzip')
                                # compression_type='snappy')
                                # compression_type='lz4')
    kafka_producer.flush()

    print('Connected to Kafka %s:%s' % (KAFKA_SERVER, KAFKA_PORT))

    while True:
        try:
            msg = {
                "date": float(imms.get_variables()[0].get_value()),
                "time": str(imms.get_variables()[1].get_value()),
                "ATActSimPara1": float(imms.get_variables()[2].get_value()),
                "ATActSimPara2": float(imms.get_variables()[3].get_value()),
                "ActCntCyc": float(imms.get_variables()[4].get_value()),
                "ActCntPrt": float(imms.get_variables()[5].get_value()),
                "ActStsMach": str(imms.get_variables()[6].get_value()),
                "ActTimCyc": float(imms.get_variables()[7].get_value()),
                "SetCntMld": float(imms.get_variables()[8].get_value()),
                "SetCntPrt": float(imms.get_variables()[9].get_value()),
                "SetTimCyc": float(imms.get_variables()[10].get_value()),
                "InstanceName": INSTANCE_NAME,
                "TimestampSent": datetime.now()
            }

            future = kafka_producer.send(KAFKA_TOPIC, json.dumps(msg, default=str).encode('utf-8'))
            kafka_producer.flush()
            print('Update written to topic %s' % KAFKA_TOPIC)

            time.sleep(float(SLEEP_DURATION))
        except KeyboardInterrupt:
            try:
                opcua_client.disconnect()
                kafka_producer.close()
                kafka_consumer.close()
            except Exception:
                pass
            finally:
                print("Writing response times to", OUT_FILE)
                file.write(json.dumps(list(q.queue)))
                file.close()
                sys.exit()
