import tweepy
import json
import sys
from datetime import datetime
non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

key = ""
secret = ""
token = ""
token_secret = ""
auth = tweepy.OAuthHandler(key, secret)
auth.set_access_token(token, token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

keywords = ["debatedeldebate", "DebateINE", "EleccionesMexico", "amloentercergrado",
"DebatePresidencial2018", "AMLO", "AMLOPresidente", "AnayaPresidente", "ElFrente", "ViolentosComoAMLO""amnistia" "amloNO", "AMLOmania", "JuntosHaremosHistoria", "@RicardoAnayaC", "@JoseAMeadeK", "@lopezobrador_", "@IVilla_g", "@Mzavalagc", "@JaimeRdzNL", "@INEMexico"]


class DebateStreamListener(tweepy.StreamListener):
    
    def on_data(self, data):
        status = json.loads(data)
        if status.get("text")  is not None:
            status["text"] = status['text'].translate(non_bmp_map)
        user = status.get("user")
        if user != None:
            if user.get("description") != None:
                status['user']['description'] = status['user']['description'].translate(non_bmp_map)
            if user.get("location") != None:
                status['user']['location'] = status['user']['location'].translate(non_bmp_map)
        today = datetime.now()
        file_path="debate/{month}_{day}_{hour}.txt".format(month=today.month,day=today.day, hour=today.hour)
        with open(file_path, 'a') as result_file:
            status_str = json.dumps(status)
            result_file.write(status_str)
            result_file.write("\n")

    def on_error(self, status_code):
        print('An Error has occured: ' + repr(status_code))
        return False

debateListener = DebateStreamListener()
stream = tweepy.Stream(auth = api.auth, listener = debateListener)
stream.filter(track=keywords, async=True)

