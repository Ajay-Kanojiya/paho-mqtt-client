
from paho.mqtt import client as mqtt_client
import time
import socket
import json
import logging
import random
from datetime import timezone
from datetime import datetime
import sys


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s: %(message)s')
file_handler = logging.FileHandler('mqtt_client.log')
stream_handler = logging.StreamHandler(sys.stdout)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)



class MqttClientConn:
    """
    This class can be used to establish a connection to Fogwing IoTHub
    and publish the machines data/payload.
    """

    def __init__(self, host, port, usr_name, pwd, client_id, pub_topic, frq):
        """
        :param host:        Fogwing IoTHub broker/host address(default: iothub.enterprise.fogwing.net).
        :param port:        Fogwing IoTHub broker/host port(default: 1883)
        :param usr_name:    Fogwing IoTHub username.
        :param pwd:         Fogwing IoTHub password.
        :param client_id:   Fogwing IoTHub client_id.
        :param pub_topic:   Fogwing IoTHub publish topic
        """
        self.host = host
        self.port = port
        self.usr_name = usr_name
        self.pwd = pwd
        self.clientid = client_id
        self.pub_topic = pub_topic
        self.frq = frq
        global mid_value

    def connect_mqtt(self):
        """
        This method connect to Fogwing IoTHub.
        :return: client object
        :rtype: object
        """
        try:
            client = mqtt_client.Client(self.clientid)
            client.username_pw_set(self.usr_name, self.pwd)
            client.on_connect = MqttClientConn.on_connect
            client.on_log = MqttClientConn.on_log
            client.on_publish = MqttClientConn.on_publish
            client.connect(self.host, self.port, keepalive=60)
            return client
        #except socket.gaierror:
        #    logger.debug("No Internert.. Please check the connectivity1")
        except Exception:
            pass

    def sendtofwg(self, client, payload):
        """
        This method is responsible for publishing/sending payload to Fogwing IoTHub.
        :param client: client object
        :param payload: payload/machine data
        """
        try:
            rc, mid = client.publish(self.pub_topic, payload, qos=1, retain=False)
            if rc == 0:
                time.sleep(0.5)
                logger.info(f'Fogwing IoTHub: Published data to Fogwing IoTHub')
                logger.debug(json.dumps(json.loads(payload), indent=2))
                logger.debug(f'Fogwing IoTHub: rc: {rc} and mid: {mid}')
            return rc, mid
        except Exception:
            logger.exception("Exception in sendtofwg method")
       

              
    @staticmethod
    def on_log(cli, userdata, level, buf):
        """
        This call back function provides log information define to allow debugging.
        :param cli:         the client instance for this callback
        :param userdata:    the private user data as set in Client() or userdata_set()
        :param level:       gives the severity of the message and will be one of
                            MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING,
                            MQTT_LOG_ERR, and MQTT_LOG_DEBUG.
        :param buf:         the message itself
        """
        logger.debug(f"Fogwing IoT Hub: {buf}")
        # mid_value = buf[buf.index("m") + 1:buf.index(')')]

    @staticmethod
    def on_publish(client, userdata, mid):
        """
        This callback called when a message that was to be sent using the publish() call has
        completed transmission to the broker. For messages with QoS levels 1 and 2,
        this means that the appropriate handshakes have completed.
        For QoS 0, this simply means that the message has left the client.
        The mid variable matches the mid variable returned from the corresponding publish() call,
        to allow outgoing messages to be tracked.
        This callback is important because even if the publish() call returns success,
        it does not always mean that the message has been sent.
        :param client:      the client instance for this callback
        :param userdata:    the private user data as set in Client() or userdata_set()
        :param mid:         matches the mid variable returned from the corresponding
                            publish() call, to allow outgoing messages to be tracked.
        """
        logger.debug(f"Fogwing IoTHub: On publish mid: {mid}")
        MqttClientConn.mid_value = mid

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        """
        This call back called when the Fogwing IoTHub/broker responds to our connection request.
        :param client:     the client instance for this callback
        :param userdata:   the private user data as set in Client() or user_data_set()
        :param flags:      response flags sent by the broker
        :param rc:         the connection result
        flags is a dict that contains response flags from the broker:
            flags['session present'] - this flag is useful for clients that are
                using clean session set to 0 only. If a client with clean
                session=0, that reconnects to a broker that it has previously
                connected to, this flag indicates whether the broker still has the
                session information for the client. If 1, the session still exists.
        The value of rc indicates:
            0: Connection successful
            1: Connection refused - incorrect protocol version
            2: Connection refused - invalid client identifier
            3: Connection refused - server unavailable
            4: Connection refused - bad username or password
            5: Connection refused - not authorised
            6-255: Currently unused.
        """
        if rc == 0:
            logger.info("Connected to Fogwing IoTHub")
        else:
            logger.info("Failed to connect to Fogwing IoTHub, return code:", rc)

    def net_conectivity(self):

        """
        This method is responsible for checking the internet connectivity.
        :return: internet connectivity status
        :rtype: bool
        """
        try:
            socket.create_connection((self.host, self.port))
            return True
        except OSError:
            return False
        except Exception:
            return False

    def utcnow(self):
        try:
            local_time = datetime.now()
            epoch_time_utc = datetime.now(timezone.utc).replace(tzinfo=timezone.utc).replace(microsecond=0).timestamp()
            return str(epoch_time_utc).rstrip(".0"), local_time.strftime("%d %b %Y"), local_time.strftime("%I:%M:%S %p")
        except Exception as exptn:
            return exptn


if __name__ == '__main__':

    connect_mqtt = False
    mqtt = MqttClientConn(host='iothub.enterprise.fogwing.net', port=1883,
                              pub_topic='fwent/edge/171de0153fae20f8/inbound',
                              usr_name='ajaykanojiya', pwd='Ajaya@123', client_id='1151-1103-1080-1002', frq=10)
        
    while True: 
        logger.debug("-----------------------------------------------DEBUG---------------------------------------------------------------")
        try: 
            if mqtt.net_conectivity():
                if not connect_mqtt:
                    client = mqtt.connect_mqtt()
                    connect_mqtt = client 
                if connect_mqtt:
                    client.loop_start()
                    # add here or call what you want to send to Fogwing.
                    weight = round(random.uniform(80.55, 100.95), 2)
                    epoch_stamp, date, Time = mqtt.utcnow()
                    data = {"date": date, "time": Time, "weight": weight}
                    mqtt.sendtofwg(client, json.dumps(data)) 
            else:   
                logger.debug("No Internet.. Please check the internet connection")        
        except AttributeError as e:  
            logger.debug("No Internet.. Please check the internet connection")
        except Exception as ex:
            logger.exception("Exception at starting..")

        time.sleep(mqtt.frq)

