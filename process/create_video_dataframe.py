import json
import pandas as pd
import sys

fields = ['vid', 'publishedAt', 'channelId', 'title', 'description',
          'channelTitle', 'liveBroadcastContent']

rows = []

with open("../sql_video_results/youtube_video_data.json", 'r') as fyle:
    counter = 0
    for line in fyle.readlines():
        counter += 1
        obj = json.loads(line)
        row = {f : None for f in fields}
        row['vid'] = obj['id']['videoId']
        snippet = obj['snippet']
        if snippet is not None:
            row['publishedAt'] = snippet['publishedAt']
            row['channelId'] = snippet['channelId']
            row['title'] = snippet['title']
            row['description'] = snippet['description']
            row['channelTitle'] = snippet['channelTitle']
            row['liveBroadcastContent'] = snippet['liveBroadcastContent']
        rows.append(row)
print(counter)
video_df = pd.DataFrame(rows)
video_df.to_csv("../sql_video_results/youtube_video_df.csv", index=False)
