
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def getdatasmall(df1):

    listtodrop1 = [
        "speed",
        "ncprogram",
        "toolname",
        "messtaster",
        "warmlauf",
        "arbeitsraumtuer_entriegelt",
        "averagespeed5",
        "averagemesstaster5",
        "averagemesstaster10",
        "averagearbeitsraumtuer_entriegelt30",
        "averagespeed30",
        "averagewarmlauf30",
        "tuer_entriegelt",
    ]

    df1 = df1.drop(listtodrop1, axis=1)

    return df1


def getdatabig(df2):

    listtodrop2 = [
        "speed",
        "ncprogram",
        "toolname",
        "messtaster",
        "arbeitsraumtuer_entriegelt",
        "warmlauf",
        "tuer_entriegelt",
    ]

    df2 = df2.drop(listtodrop2, axis=1)

    return df2
