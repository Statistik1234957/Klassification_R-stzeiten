from __future__ import print_function
import dctrees
import get_final_data
import json
import combine_all_lists

def get_data_for_sending(endtime,starttime,bearer_token):

    "This function takes the Dataframe and adjusts the data to fit the needed format"

    # get the data
    df = combine_all_lists.combine_all_lists(starttime=starttime, endtime=endtime,bearer_token = bearer_token)

    if df.empty:

        time_for_empty_sending = endtime.isoformat() + ".000Z"

        datasmallexport = {
            "ruleId": "umati",
            "state": 11,
            "startTime": time_for_empty_sending,
        }

        databigexport = {
            "ruleId": "umati",
            "state": 11,
            "startTime": time_for_empty_sending,
        }

    else:

        # adjust the time col
        df["timeframe"] = df["timeframe"].map(lambda x: x.isoformat() + ".000Z")
        timeframe_both = df["timeframe"].tolist()

        # only select the relevant data
        dfsmall = get_final_data.getdatasmall(df)
        dfbig = get_final_data.getdatabig(df)

        # add Status col from decision trees
        dfwithstatussmall = dctrees.dctreesmall(dfsmall)
        dfwithstatusbig = dctrees.dctreebig(dfbig)

        # get the status
        status_small = dfwithstatussmall["Status"].tolist()

        # convert to json for sending
        datasmallexport = {
            "ruleId": "umati",
            "state": status_small[-1],
            "startTime": timeframe_both[-1],
        }

        # get status
        status_big = dfwithstatusbig["Status"].tolist()

        # convert to json for sending
        databigexport = {
            "ruleId": "umati",
            "state": status_big[-1],
            "startTime": timeframe_both[-1],
        }

    print(datasmallexport)
    print(databigexport)

    # create data for sending
    data_outsmall = json.dumps(datasmallexport, indent=4, sort_keys=True, default=str)


    #convert to json for sending
    data_outbig = json.dumps(databigexport, indent=4, sort_keys=True, default=str)

    return data_outsmall,data_outbig