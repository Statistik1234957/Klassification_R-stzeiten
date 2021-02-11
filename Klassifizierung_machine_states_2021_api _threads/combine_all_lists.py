import pandas as pd
from datetime import timedelta
import concurrent.futures
from get_list import get_list
import functioncollector

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def combine_all_lists(starttime, endtime,bearer_token):

    """This function recieves all lists and returns the complete DF"""

    #create needed constants
    start_time_for_counting = starttime
    timedel = timedelta(seconds=1)

    # list that saves the timestamps
    timelist = []

    #create time list
    while start_time_for_counting < endtime:

        timelist.append(start_time_for_counting)
        start_time_for_counting = start_time_for_counting + timedel

    value_list = [(starttime, endtime, "alarms", "alarmtext",bearer_token),(starttime, endtime, "analyzestate", "duration",bearer_token)
                  ,(starttime, endtime, "machinespindle", "speedavg",bearer_token),(starttime, endtime, "ncprograms", "ncprogram",bearer_token)
                  ,(starttime, endtime, "activeTools", "tool.name",bearer_token)]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(get_list, value_list)

    total_list = []
    for result in results:
        total_list.append(result)

    tuerlist = total_list[0]
    statelist = total_list[1]
    spindlespeedlist = total_list[2]
    analyzemachinenclist = total_list[3]
    machinetoollist = total_list[4]

    #if no data is recieved
    if not (statelist and spindlespeedlist and analyzemachinenclist and machinetoollist and tuerlist):

        #create empty DF
        df = pd.DataFrame(
            columns=["timeframe", "stateduration", "speed", "ncprogram", "toolname", "arbeitsraumtuer_entriegelt"])

    else:

        # create the Dataframe
        df = pd.DataFrame(
            {
                "timeframe": timelist,
                "stateduration": statelist[0],
                "speed": spindlespeedlist,
                "ncprogram": analyzemachinenclist,
                "toolname": machinetoollist,
                "arbeitsraumtuer_entriegelt": tuerlist,
            }
        )

    # if no data is recieved
    if df.empty:

        # create empty DF
        df = pd.DataFrame(columns= ["timeframe","stateduration","speed","ncprogram","toolname","arbeitsraumtuer_entriegelt",
                                   "averagespeed5","averagemesstaster5","averagespeed10","averagemesstaster10",
                                   "averagearbeitsraumtuer_entriegelt30","averagespeed30","averagewarmlauf30","messtaster",
                                   "tuer_entriegelt","warmlauf",
                                   ])

    else:

        # create messtaster 0/1
        messtasterlist = functioncollector.messtaster_warmlauf(df, "toolname", "MESSTASTER")
        df["messtaster"] = messtasterlist

        # create warmlauf 0/1
        warmlauflist = functioncollector.messtaster_warmlauf(df, "ncprogram", "WARMLAUF.H")
        df["warmlauf"] = warmlauflist

        # create tuer 0/1
        tuerboollistrelevant = []

        dftuer = df["arbeitsraumtuer_entriegelt"]

        for l in range(len(dftuer.index)):

            if any(
                    s in dftuer[l]
                    for s in ("P149", "P89", "P147", "P151", "P148", "P1279", "P67")
            ):
                tuerboollistrelevant.append(1)
            else:
                tuerboollistrelevant.append(0)

        df["tuer_entriegelt"] = tuerboollistrelevant

        # list for average computation
        averagelistnames = [
            "speed",
            "messtaster",
            "speed",
            "messtaster",
            "tuer_entriegelt",
            "speed",
            "warmlauf",
        ]

        # list with names of new columns
        averagelistnames2 = [
            "averagespeed5",
            "averagemesstaster5",
            "averagespeed10",
            "averagemesstaster10",
            "averagearbeitsraumtuer_entriegelt30",
            "averagespeed30",
            "averagewarmlauf30",
        ]

        # compute the different avg values for different timespans
        avgtime = 300

        for a in range(len(averagelistnames)):

            df[averagelistnames2[a]] = (
                df[averagelistnames[a]].rolling(window=avgtime).mean()
            )

            if a == 1:
                avgtime = 600

            if a == 3:
                avgtime = 1800

        df = df.tail(1)

    return df

