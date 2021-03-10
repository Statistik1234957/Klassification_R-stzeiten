
import json
import pandas as pd
import requests
import time

def get_bearer_token(email,password,loginurl):

    "This function creates the bearer Token"

    payload = "{\"username\":\"" + email + "\",\r\n\"password\":\"" + password +"\",\r\n\"lang\":\"de\"\r\n}"
    headers = {
        'Content-Type': 'application/json'
    }

    start_sec = 2
    punishment = 4
    limit = 16

    while 1:

        try:

            if start_sec == limit:
                start_sec = (limit - 1)

            response = requests.request("POST", loginurl, headers=headers, data=payload)
            data = response.text
            json_string = json.loads(data)
            df = pd.json_normalize(json_string)
            token = df["token"].iloc[0]

        except Exception:
            print(f"There has been a problem recieving the authorization token: Waiting {punishment * start_sec} seconds and trying again.")
            time.sleep(punishment * start_sec)
            start_sec += 1
            continue
        break

    print(token)

    return token



