from __future__ import print_function
import sys
import traceback
import paho.mqtt.client as mqtt
import logging
from get_data_for_sending import get_data_for_sending
from config_utils import ConfigUtils
import functioncollector

# if in main
if __name__ == "__main__":

    # disable debug logger info
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # get he constant values
    cfg = ConfigUtils()
    broker_url = cfg.read_cfgvalue("MQTT", "broker_url")
    broker_port = int(cfg.read_cfgvalue("MQTT", "broker_port"))
    ca_cert_path = cfg.read_cfgvalue("MQTT", "ca_cert")
    certfile_path = cfg.read_cfgvalue("MQTT", "client_cert")
    certkey_path = cfg.read_cfgvalue("MQTT", "client_key")
    logger = functioncollector.createlogger()

    print("-" * 10)

    try:

        # create client object
        mqttc = mqtt.Client()

        # deal with the certificates
        ssl_context = functioncollector.ssl_alpn(ca_cert=ca_cert_path,
                                                 certfile=certfile_path,
                                                 certkey=certkey_path)

        # give the client the certificates
        mqttc.tls_set_context(context=ssl_context)
        logger.info("start connect with MQTT")

        # connect with borker
        mqttc.connect(broker_url, broker_port, 15)
        logger.info("mqtt connect success")
        print("-" * 10)

        # starting the loop :starts a new thread, that calls the loop method at regular intervals for you
        mqttc.loop_start()

        # get the topic for the new machines
        productionstate1 = "1000000/machines/testrule3/productionstates"

        print("start getting the data")

        # get the data
        data_out = get_data_for_sending()

        print("finished getting the data")
        print("-" * 10)

        # for each row convert to json format
        data_out['json'] = data_out.apply(lambda x: x.to_json(), axis=1)

        print("start sending the data")

        # send each row
        data_out['json'].apply(lambda row: mqttc.publish(topic=productionstate1, payload=row, qos=1, retain=True))

        print("finished sending the data")

        print("-" * 10)

        # stops the loop
        mqttc.loop_stop()

    except Exception as e:

        logger.error("exception main()")
        logger.error("e obj:{}".format(vars(e)))
        traceback.print_exc(file=sys.stdout)










