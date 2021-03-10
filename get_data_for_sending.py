from __future__ import print_function
import pandas as pd
from datetime import datetime, timedelta

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)

def get_data_for_sending():

    "This function takes the Dataframe and adjusts the data to fit the needed format"

    # get the data
    df = pd.read_csv("Status.csv", header = 0,sep= ";")

    #create new Dataframe
    df_new = pd.DataFrame()

    #adjust state column as integer
    df["state"] = df["state"].map(lambda x: int(x))

    #adjust the time column and specify the format
    df["time"] = df["time"].map(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))

    #add start and endtime column
    df["startTime"] = ""
    df["endTime"] = ""

    #for each row in the df
    for row in range(len(df) - 1):

        #get the state
        state = df["state"].iloc[row]

        #set the start and endtime
        startime_row = df["time"].iloc[row]
        endtime_row = df["time"].iloc[row + 1]

        #get the data in seconds interval
        while startime_row <= endtime_row:

            df_new = df_new.append({"startTime" : startime_row, "state" : state},ignore_index= True)

            startime_row = startime_row + timedelta(seconds = 5)

    #add the Z
    df_new["startTime"] = df_new["startTime"].map(lambda x: x.isoformat() + ".000Z")

    #add the rule id
    df_new["ruleId"] = "umati"

    #only select the relevant columns
    df_new = df_new[["startTime","state","ruleId"]]

    return df_new

