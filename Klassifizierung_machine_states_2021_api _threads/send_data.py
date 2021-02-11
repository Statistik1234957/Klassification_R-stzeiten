from __future__ import print_function
import sys
import traceback
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import time
from config_utils import ConfigUtils
import functioncollector
import logging
import get_data_for_sending
import get_bearer_token

"""This file contains the execution of the code and all its subclasses"""

#disable debug logger info
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

#get he constant values
cfg = ConfigUtils()
broker_url = cfg.read_cfgvalue("MQTT", "broker_url")
broker_port = int(cfg.read_cfgvalue("MQTT", "broker_port"))
topicproductionstates1 = cfg.read_cfgvalue("MQTT", "topicproductionstates1")
topicproductionstates2 = cfg.read_cfgvalue("MQTT", "topicproductionstates2")
ca_cert_path = cfg.read_cfgvalue("MQTT", "ca_cert")
certfile_path = cfg.read_cfgvalue("MQTT", "client_cert")
certkey_path = cfg.read_cfgvalue("MQTT", "client_key")
email = cfg.read_cfgvalue("API","email")
password = cfg.read_cfgvalue("API","password")
login_url = cfg.read_cfgvalue("API","qarclogin")

#if in main
if __name__ == "__main__":

    print("-" * 10)
    logger = functioncollector.createlogger()

    try:

        # connect to the mqtt client
        start1 = time.time()
        mqttc = mqtt.Client()
        ssl_context = functioncollector.ssl_alpn(ca_cert=ca_cert_path,
                                                 certfile=certfile_path,
                                                 certkey=certkey_path)

        mqttc.tls_set_context(context=ssl_context)
        logger.info("start connect with MQTT")

        mqttc.connect(broker_url, broker_port, 15)
        logger.info("mqtt connect success")
        print(f'(Time for MQTT connenction in seconds: {round(time.time() - start1,4)})')
        print("-" * 10)

        mqttc.loop_start()

        while True:

            start2 = time.time()

            #get the bearer token
            bearer_token = get_bearer_token.get_bearer_token(email=email, password=password, loginurl=login_url)

            # set the timespan
            endtime = datetime.utcnow().replace(microsecond=0)
            starttime = endtime - timedelta(minutes=30)

            status_small, status_big = get_data_for_sending.get_data_for_sending(endtime=endtime,
                                                                                 starttime=starttime,
                                                                                 bearer_token= bearer_token)

            # publish the data
            #mqttc.publish(topicproductionstates1, status_small, qos=2, retain=True)
            #mqttc.publish(topicproductionstates2, status_big, qos=2, retain=True)
            print(f'(Time to get all data in seconds: {round(time.time() - start2,4)})')
            print("-" * 10)

            time.sleep(5)

    except Exception as e:

        logger.error("exception main()")
        logger.error("e obj:{}".format(vars(e)))
        traceback.print_exc(file=sys.stdout)










