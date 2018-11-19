import re
import json
import os

def parse_videos(entities, out):
    counter = 0
    if entities is not None:
        media = entities.get("media")
        if media is not None:
            for m in media:
                if m['type'] == 'video':
                    m['found_user_id_str'] = tweet['user']['id_str']
                    m['found_id_str'] = tweet['id_str']
                    m['found_timestamp'] = tweet['created_at']
                    m_obj = json.dumps(m)
                    print(m_obj, file=out)
                    counter += 1
    return counter

source_dir = "D:/twitter_data_bucket1"
source_files = ["{0}/{1}".format(source_dir, f)
                for f in os.listdir(source_dir) if f.startswith("6_") or f.startswith("7_1_")]
source_files.sort()

counter = 0
with open("all_videos.txt", 'w') as out:
    for fyle in source_files:
        print(fyle)
        with open(fyle, 'r') as f:
            for line in f.readlines():
                try:
                    tweet = json.loads(line)
                    entities1 = tweet.get("extended_entities")
                    counter += parse_videos(entities1, out)
                    if tweet.get("truncated"):
                        extended = tweet.get("extended_tweet")
                        entities2 = extended.get("extended_entities")
                        counter += parse_videos(entities2, out)
                except Exception as e:
                    print("Error: {0}".format(e))
        print(counter)
        
