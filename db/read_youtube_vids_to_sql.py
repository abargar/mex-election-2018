import sqlite3
import csv
import time

"""
New Table:  YouTube
- vid (index)
- title
- publishedAt
- description
- channelId
- channelTitle
"""

def parse_time(field):
    if field is not None:
        result = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(field,'%Y-%m-%dT%H:%M:%S.%fZ'))
    else:
        result = ""
    return result

VID_INSERT = '''INSERT or IGNORE INTO youtube_vids (vid, title, publishedAt, description, channelId, channelTitle)
VALUES (:vid, :title, :publishedAt, :description, :channelId, :channelTitle)'''

conn = sqlite3.connect("mexico_urls.db")

c = conn.cursor()


c.execute('''CREATE TABLE IF NOT EXISTS youtube_vids (vid text PRIMARY KEY, title text, publishedAt timestamp, description text,
channelId text, channelTitle text)''')

counter = 0
with open("../sql_video_results/youtube_video_df.csv", encoding = "latin1") as fyle:
    reader = csv.DictReader(fyle)
    c.execute("BEGIN TRANSACTION")
    for row in reader:
        # liveBroadcastContent was none for every row, so skipping
        try:
            row['publishedAt'] = parse_time(row['publishedAt'])
            c.execute(VID_INSERT, row)
            counter += 1
            if counter % 1000 == 0:
                print(counter)
            if counter % 10000 == 0:
                c.execute("COMMIT")
                c.execute("BEGIN TRANSACTION")
        except Exception as e:
            print(row['vid'])
            print("Error occured: {0}".format(e))
    c.execute("COMMIT")
    print("DONE!")
