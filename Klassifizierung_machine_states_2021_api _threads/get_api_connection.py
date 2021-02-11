from config_utils import ConfigUtils
import requests
import pandas as pd
import json
import functioncollector
import time
from datetime import timedelta

def get_api_connection(data_name,starttime,endtime,lastentry,bearer_token):

    " This function contains the api connection "

    # api needs iso time formatted string
    starttime_iso = starttime.isoformat() + ".000Z"
    endtime_iso = endtime.isoformat() + ".000Z"

    # if there isn't any data cath in a bigger timespan
    starttime_last_row = starttime - timedelta(seconds = 164501)
    starttime_last_row_iso = starttime_last_row.isoformat() + ".000Z"

    #get constants from config file
    cfg = ConfigUtils()
    qarc_url = cfg.read_cfgvalue("API","qarc")
    machineid = cfg.read_cfgvalue("Machine","machineid")

    #authentification
    payload = {}
    headers = {
        'x-grob-jwt': bearer_token,
        'Authorization': 'Bearer ' + bearer_token
    }

    #create the wanted urls
    if data_name == "analyzestate" and lastentry is None:

        url = qarc_url + "/analyze/2.0.0/" + "productionStates" + "/byMachineAndPeriod?id=" + machineid + "&from=" + starttime_iso + "&until=" + endtime_iso

    elif data_name == "analyzestate" and lastentry is not None:

        url = qarc_url + "/analyze/2.0.0/" + "productionStates" + "/byMachineAndPeriod?id=" + machineid + "&from=" + starttime_last_row_iso + "&until=" + endtime_iso

    elif data_name == "machinespindle" and lastentry is None:

        url = qarc_url + "/s/analyze/1.0.0/machines/" + "spindle" + "/aggregate?id=" + machineid + "&from=" + starttime_iso + "&until=" + endtime_iso

    elif data_name == "machinespindle" and lastentry is not None:

        url = qarc_url + "/s/analyze/1.0.0/machines/" + "spindle" + "/aggregate?id=" + machineid + "&from=" + starttime_last_row_iso + "&until=" + endtime_iso

    else:

        if lastentry is None:

            url = qarc_url + "/s/analyze/" + data_name + "/byMachineAndPeriod?id=" + machineid + "&from=" + starttime_iso + "&until=" + endtime_iso

        else:

            url = qarc_url + "/s/analyze/" + data_name + "/byMachineAndPeriod?id=" + machineid + "&from=" + starttime_last_row_iso + "&until=" + endtime_iso

    start_sec = 2
    punishment = 4
    limit = 16

    while 1:
        try:

            if start_sec == limit:
                start_sec = (limit - 1)

            # get the the responses
            response = requests.request("GET", url, headers=headers, data=payload)
            data = response.text
            json_string = json.loads(data)

        except Exception:
            print(f"There has been an error getting Information from the api: Waiting {punishment * start_sec} seconds and trying again.")
            time.sleep(punishment*start_sec)
            start_sec += 1
            continue
        break

    try:

        if data_name == "machinespindle":

            #get the dataframe
            df = pd.json_normalize(json_string['listAnalyzeSpindle'])
            df.rename({"ts.epochSecond": "startTime"}, axis=1, inplace=True)

        else:
            df = pd.json_normalize(json_string["searchResults"])

        #correct the time
        functioncollector.api_time_correction(df, data_name)

    except Exception:
        df = pd.DataFrame(columns=["alarmtext","duration", "speedavg",
                                   "ncprogram","startTime", "endTime", 'tool.name'])

    return df





