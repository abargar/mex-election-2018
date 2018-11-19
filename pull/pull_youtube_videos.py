from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import csv
import json
import sys

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

DEVELOPER_KEY = ""
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


def youtube_search(service, vid, max_results=1, token=None, order="relevance"):
    data_parts = "id,snippet"
    #"id,snippet,contentDetails,statistics,topicDetails,recordingDetails"
    try:
        search_response = service.search().list(
            q=vid,
            type="video",
            pageToken=token,
            order=order,
            part=data_parts,
            maxResults=max_results).execute()
        result_found = False
        for search_result in search_response.get("items", []):
            result_found = True
            search_id = search_result.get("id")
            snippet = search_result.get("snippet")
            if snippet is not None:
                search_result['snippet']["title"] = snippet["title"].translate(non_bmp_map)
                search_result['snippet']["description"] = snippet["description"].translate(non_bmp_map)
                search_result['snippet']["channelTitle"] = snippet["channelTitle"].translate(non_bmp_map)
            if search_id is not None:
                search_id = search_id.get("videoId")
                if vid != search_id:
                    with open("../sql_video_results/mismatch_youtube.csv", 'a') as mout:
                        print("{0},{1}".format(vid,search_id), file=mout)
            with open('../sql_video_results/youtube_video_data.json', 'a') as out:
                result_str = json.dumps(search_result)
                print(result_str, file=out)
        if result_found is False:
            with open("../sql_video_results/no_results.csv", 'a') as nout:
                print("{0}, {1}".format(vid, len(vid)), file=nout)
            
    except Exception as e:
        with open("../sql_video_results/err_youtube.csv", 'a') as eout:
            print("{0}, {1}".format(vid, e), file=eout)      

def find_videos():
    service = build(YOUTUBE_API_SERVICE_NAME,
                YOUTUBE_API_VERSION,
                developerKey=DEVELOPER_KEY)
    with open("../unique_vids.csv", 'r') as fyle:
        reader = csv.reader(fyle)
        counter = 0
        for row in reader:
            vid = row[0].strip()
            #space before id keeps Excel from ruining everything
            youtube_search(service, vid)
            counter += 1
            if counter % 100 == 0:
                print("Counter = {0}".format(counter))

find_videos()
