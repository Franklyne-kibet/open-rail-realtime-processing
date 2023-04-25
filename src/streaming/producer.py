import stomp
import zlib
import json
import time
import socket
import logging
import xml.etree.ElementTree as ET

from kafka import KafkaProducer
from settings import BOOTSTRAP_SERVERS,KAFKA_TOPIC

logging.basicConfig(format='%(asctime)s %(levelname)s\t%(message)s', level=logging.INFO)

try:
    import ppv16
except ModuleNotFoundError:
    logging.error("Class files not found - please configure the client following steps in README.md!")


USERNAME = 'DARWINca7163a1-cc87-4133-a6a0-8703744e8f23'
PASSWORD = 'e43cea40-4aac-4d21-a0cc-c28cf26cd940'
HOSTNAME = 'darwin-dist-44ae45.nationalrail.co.uk'
HOSTPORT = 61613
# Always prefixed by /topic/ (it's not a queue, it's a topic)
TOPIC = '/topic/darwin.pushport-v16'

CLIENT_ID = socket.getfqdn()
HEARTBEAT_INTERVAL_MS = 15000
RECONNECT_DELAY_SECS = 15

if USERNAME == '':
    logging.error("Username not set - please configure your username and password in opendata-nationalrail-client.py!")

# Kafka Configuration
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    key_serializer=lambda key: json.dumps(key).encode('utf-8'),
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def connect_and_subscribe(connection):
    if stomp.__version__[0] < 5:
        connection.start()

    connect_header = {'client-id': USERNAME + '-' + CLIENT_ID}
    subscribe_header = {'activemq.subscriptionName': CLIENT_ID}

    connection.connect(username=USERNAME, passcode=PASSWORD, wait=True, headers=connect_header)

    connection.subscribe(destination=TOPIC, id='1', ack='auto', headers=subscribe_header)

class StompClient(stomp.ConnectionListener):

    def on_heartbeat(self):
        logging.info('Received a heartbeat')

    def on_heartbeat_timeout(self):
        logging.error('Heartbeat timeout')

    def on_error(self, headers, message):
        logging.error(message)

    def on_disconnected(self):
        logging.warning('Disconnected - waiting %s seconds before exiting' % RECONNECT_DELAY_SECS)
        time.sleep(RECONNECT_DELAY_SECS)
        exit(-1)

    def on_connecting(self, host_and_port):
        logging.info('Connecting to ' + host_and_port[0])

    def on_message(self, frame):
        try:
            sequence_number = frame.headers['SequenceNumber']
            message_type = frame.headers['MessageType']
            logging.info('Message sequence=%s, type=%s received', sequence_number, message_type)

            # Extract the compressed message body from the frame
            compressed_msg = frame.body

            # Decompress the message
            xml_string = zlib.decompress(compressed_msg, zlib.MAX_WBITS | 32)
            
            # Parse the XML string
            root = ET.fromstring(xml_string)

            # Namespace dictionary for easier access to namespaces
            ns = {
                'ns5': 'http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3',
                'pp': 'http://www.thalesgroup.com/rtti/PushPort/v16'
            }

            # Create a dictionary to hold the train data
            train_data = {}

            # Extract the attributes of the <TS> element
            ts_element = root.find('.//{http://www.thalesgroup.com/rtti/PushPort/v16}TS')
            if ts_element is not None:
                schedule = ts_element.get('rid')
                unique =  ts_element.get('uid')
                service = ts_element.get('ssd')

            # Add the rid, uid, ssd, and trainID attributes to the train data dictionary
            train_data.update({
                "sequence_number": sequence_number,
                'schedule_id': schedule,
                'unique_id': unique,
                'service_start_date': service
            })
            
            # Loop through all the train locations in the XML file and add their information to the dictionary
            for location in root.iterfind('.//ns5:Location', ns):
                train_location = location.get('tpl') # Train location code
                sch_arrival = location.get('wta') # Scheduled arrival time
                sch_departure = location.get('wtd') # Scheduled departure time
                actual_arr = location.get('pta') # Actual arrival time
                actual_dep = location.get('ptd') # Actual departure time
                plat_element = location.find('ns5:plat', ns) # Platform element
                if plat_element is not None:
                    plat = plat_element.text # Platform number
                else:
                    plat = None
                arr_details = location.find('ns5:arr', ns)  # Arrival details
                dep_details = location.find('ns5:dep', ns)  # Departure details
                if arr_details is not None:
                    est_time = arr_details.get('et')
                    src = arr_details.get('src')
                elif dep_details  is not None:
                    est_time = dep_details .get('et')
                    src = dep_details .get('src')
                else:
                    est_time = None
                    src = None

                # Create a dictionary with the train location information
                train_location = {
                    "location_code": train_location,
                    "scheduled_arrival": sch_arrival,
                    "scheduled_departure": sch_departure,
                    "actual_arrival": actual_arr,
                    "actual_departure": actual_dep,
                    "platforms": plat,
                    "estimated_time": est_time,
                    "source": src,
                }
                # Add the train location to the dictionary
                train_data.update(train_location)
                
                # Send the train location data to the Kafka topic
                producer.send(topic=KAFKA_TOPIC,key=train_data["unique_id"], value=train_data)

        except Exception as e:
            logging.error('Error processing message: %s', e)


conn = stomp.Connection12([(HOSTNAME, HOSTPORT)], auto_decode=False, heartbeats=(HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS))

conn.set_listener('', StompClient())
connect_and_subscribe(conn)

while True:
    time.sleep(1)
    
producer.close()

conn.disconnect()