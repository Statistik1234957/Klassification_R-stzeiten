import numpy as np

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def dctreesmall(df):

    conditions = [
        (df["stateduration"] <= 16450.0) & (df["averagespeed10"] > 500.5),
        (df["stateduration"] <= 16450.0) & (df["averagespeed10"] <= 500.5),
        (df["stateduration"] > 16450.0),
    ]

    valuesasnumber = [1, 6, 7]
    default2 = 11

    df["Status"] = np.select(conditions, valuesasnumber, default=default2)

    return df

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def dctreebig(df):


    conditions = [
            (df["stateduration"] <= 16450.0)
            & (df["averagespeed5"] <= 86.177)
            & (df["averagearbeitsraumtuer_entriegelt30"] <= 0)
            & (df["averagemesstaster10"] <= 0.127),
            (df["stateduration"] <= 16450.0)
            & (df["averagespeed5"] <= 86.177)
            & (df["averagearbeitsraumtuer_entriegelt30"] <= 0)
            & (df["averagemesstaster10"] > 0.127),
            (df["stateduration"] <= 16450.0)
            & (df["averagespeed5"] <= 86.177)
            & (df["averagearbeitsraumtuer_entriegelt30"] > 0)
            & (df["averagespeed30"] <= 784.95),
            (df["stateduration"] <= 16450.0)
            & (df["averagespeed5"] <= 86.177)
            & (df["averagearbeitsraumtuer_entriegelt30"] > 0)
            & (df["averagespeed30"] > 784.95),
            (df["stateduration"] <= 16450.0)
            & (df["averagespeed5"] > 86.177)
            & (df["averagewarmlauf30"] <= 0.371)
            & (df["averagemesstaster5"] <= 0.545),
            (df["stateduration"] <= 16450.0)
            & (df["averagespeed5"] > 86.177)
            & (df["averagewarmlauf30"] <= 0.371)
            & (df["averagemesstaster5"] > 0.545),
            (df["stateduration"] <= 16450.0)
            & (df["averagespeed5"] > 86.177)
            & (df["averagewarmlauf30"] > 0.371),
            (df["stateduration"] > 16450.0),
        ]

    valuesasnumber = [7, 6, 6, 6, 1, 6, 6, 7]
    default2 = 11

    df["Status"] = np.select(conditions, valuesasnumber, default=default2)

    return df

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #