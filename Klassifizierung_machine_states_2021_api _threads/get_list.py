
from datetime import timedelta
import numpy as np
import get_api_connection


def get_list(inputlist):

    """This function gets the data and returns a list with
    the timestamp and the relevant information of a timespan of 30 min"""

    #create empty list and timeinterval
    timed = timedelta(seconds=1)
    list_return = []
    start_time_for_counting = inputlist[0]

    #handle analyzestate seperately
    if inputlist[2] == "analyzestate":

        #get data from a bigger timespan, otherwise stateduration is maxed by 30 min: Timeinterval
        df_with_last_entry = get_api_connection.get_api_connection(inputlist[2], inputlist[0], inputlist[1], 0, inputlist[4])

        # if the recieved DF is empty:
        if df_with_last_entry.empty:

            None

        # if you recieved data:
        else:
            #get the first starttime
            starttime_for_duration = df_with_last_entry.tail(1)["startTime"].iloc[0]

            #compute the duration up to now
            duration = inputlist[1] - starttime_for_duration
            duration = duration / np.timedelta64(1, "s")

            # append value to list
            list_return.append(duration)

    else:

        # get the data from the api
        df = get_api_connection.get_api_connection(inputlist[2], inputlist[0], inputlist[1], None, inputlist[4])
        collist = df.columns.tolist()

        #if the recieved DF is empty:
        if df.empty:

            # make the api call with a larger timespan in order to catch a value
            df_with_last_entry = get_api_connection.get_api_connection(inputlist[2], inputlist[0], inputlist[1], 0, inputlist[4])

            # if no data return empty List
            if df_with_last_entry.empty:

                None

            else:

                # get the last entry
                df_with_last_entry = df_with_last_entry.tail(1)
                df_with_last_entry.reset_index(inplace=True, drop=True)

                last_value = df_with_last_entry[inputlist[3]].iloc[0]

                while start_time_for_counting < inputlist[1]:
                    list_return.append(last_value)
                    start_time_for_counting = start_time_for_counting + timed

        # if you recieved data:
        elif inputlist[3] in collist:

            #add a new row at the end of the data and always loop for df -1
            new_row = {"startTime": inputlist[1]}

            # append the new row to the df
            df = df.append(new_row, ignore_index=True)

            #set the first Startdate in the df as the startdate given by the Code
            df["startTime"] = df["startTime"].replace([df["startTime"].head(1).iloc[0]], inputlist[0])

            # filter out crazy data
            df = df[df["startTime"] <= inputlist[1]]
            df = df[df["startTime"] >= inputlist[0]]

            # for each row (except the artificial created one):
            for i in range(len(df)-1):

                # time = starttime row
                time_from_db = df["startTime"].iloc[i]

                # endtime = endtime row
                endtime_from_db = df["startTime"].iloc[i + 1]

                while time_from_db < endtime_from_db:

                    #append value to list
                    list_return.append(df[inputlist[3]].iloc[i])

                    #increment 1 sec
                    time_from_db = time_from_db + timed

            else:
                None

    return list_return

