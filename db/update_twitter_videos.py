import pandas as pd
import sqlite3
import ast

def parse_time(field):
    if field is not None:
        result = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(field,'%a %b %d %H:%M:%S +0000 %Y'))
    else:
        result = ""
    return result

conn = sqlite3.connect("mexico_urls.db")

c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS twitter_videos
(id_str text primary key, found_id_str text, found_timestamp timestamp, found_user_id_str text,
media_url text, source_status_id_str text, source_user_id_str text, type text, duration_millis integer,
title text, description text, aspect_ratio text)''')


#video_df = pd.read_csv("../media/twitter_videos.csv", dtype=object)
#video_df = video_df.drop(["media_url_https", "sizes", "url", "display_url", "indices", "Unnamed: 0"], axis=1)
#video_df['title'] = video_df.apply(lambda row: ast.literal_eval(row['additional_media_info']).get("title"), axis=1)
#video_df['aspect_ratio'] = video_df.apply(lambda row: ast.literal_eval(row['video_info']).get("aspect_ratio"), axis=1)
#video_df = video_df.drop(['additional_media_info', 'video_info'], axis=1)
#video_df['found_timestamp'] = video_df['found_timestamp'].apply(lambda x: parse_time(x))
#video_df['aspect_ratio'] = video_df['aspect_ratio'].astype(str)

video_df = pd.read_csv("parsed_twitter_video_df.csv")

video_df.to_sql("twitter_videos", conn, if_exists="replace")
