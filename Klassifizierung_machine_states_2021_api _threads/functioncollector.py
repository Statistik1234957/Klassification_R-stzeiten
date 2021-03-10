import ssl
import logging
import sys
from datetime import datetime
import pandas as pd

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def api_time_correction(df,data_name):

    "This file converts the times given by the api"

    #list with the columns of the df
    collist = df.columns.tolist()

    #list with time columns
    collist_time = ["startTime", "endTime"]

    for col in collist_time:

        if col in collist:

            if data_name == "machinespindle":

                #no microseconds and thus no divison by 1000 needed
                df[col] = df[col].map(lambda x: datetime.utcfromtimestamp(x))
            else:
                #microseconds and thus dividision by 1000 needed
                df[col] = df[col].map(lambda x: datetime.utcfromtimestamp(x / 1000))

                #adjust the time format
                df[col] = df[col].map(lambda x: x.replace(microsecond=0))
                df[col] = pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%d %H:%M:%S")

    return df

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

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

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def messtaster_warmlauf(df, colname_from_df, equalstring):

    "This function binary decodes the wanted columns"

    # Get the Data of the wanted column
    df_col = df[colname_from_df]

    list_of_new_col = [1 if df_col[i] == equalstring else 0 for i in range(len(df_col.index))]

    return list_of_new_col

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #