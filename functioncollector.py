import ssl
import logging
import sys

def ssl_alpn(ca_cert, certfile, certkey):

    "This function deals with the given certificates for the MQTT Connection"

    try:
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(cafile=ca_cert)
        ssl_context.load_cert_chain(certfile=certfile, keyfile=certkey)

        return ssl_context

    except Exception as e:
        print("exception ssl_alpn()")
        raise e

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def createlogger():

    "This function creates a pretty logger"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(log_format)
    logger.addHandler(handler)

    return logger

