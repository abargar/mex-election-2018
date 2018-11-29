# -*- coding: utf-8 -*-
"""
Purpose: To convert tweets (JSON) into a SQL database with queryable
tables for analysis.

In this research project, I'm specifically interested in the media that people
are sharing.  So I'm going to write this with some additional tables for urls
and YouTube video data in particular.

Table 1:  Tweet
- id_str (index)
- user_id
- created_at
- retweeted_status (tweet id or None)
- quoted_status (tweet id or None)
- text
- lang
- place (full_name or None)
- source

Table 2:  TweetCounters
-id_str (index)
-timestamp_ms
-quote_count
-reply_count
-retweet_count
-favorite_count

Table 3: Tweet Relationships
-id_str
-source_id_str
-user.id_str
-source_user.id_str
-relationship (retweet, quote, reply)
-timestamp_ms

Table 4: User
-id_str
-screen_name
-created_at
-time_zone
-name
-description
-url
-geo_enabled
-timestamp_ms

Table 5: UserCounters
-id_str
-timestamp_ms
-followers_count
-friends_count
-statuses_count

Table 5: Tweet Urls
-id_str
-url
# will add full_url, domain fields later)

Table 6: Tweet Media
-id_str
-media (url)
-type (video_thumb or media)

Table 6: Embedded Videos

Table 7: YouTube Videos
# This one will be populated in a separate script
"""
import os
import json
import csv
import sys
import sqlite3
import time

sqlite3.enable_callback_tracebacks(True)
non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

# defining tables

tweet_cols = ["id_str", "user_id", "created_at", "retweeted_status",
                "quoted_status", "text_content", "lang", "place", "source"]
tweetcounter_cols = ["id_str", "timestamp_ms", "quote_count", "retweet_count",
                     "reply_count", "favorite_count"]
tweetrelation_cols = ["id_str", "source_id_str", "user_id_str", "source_user_id_str",
                      "relationship", "timestamp_ms"]
user_cols = ["user_id", "screen_name", "created_at", "time_zone", "name",
             "url", "timestamp_ms", "description"]
usercounter_cols = ["user_id", "timestamp_ms", "statuses_count", "followers_count", "friends_count"]
entity_cols = ["id_str", "entity", "type", "user_id", "created_at"]

TWEET_INSERT = '''INSERT or IGNORE INTO tweets (id_str, user_id, created_at,
retweeted_status, quoted_status, text_content, lang, place, source)
VALUES (:id_str, :user_id, :created_at, :retweeted_status,
:quoted_status, :text_content, :lang, :place, :source)'''

TWEETRELATION_INSERT = '''INSERT or IGNORE INTO tweet_relations (id_str, source_id_str,
user_id, source_user_id, relationship, timestamp_ms) VALUES (:id_str,
:source_id_str, :user_id, :source_user_id, :relationship, :timestamp_ms)'''

TWEETCOUNTER_INSERT = '''INSERT or IGNORE INTO tweet_counters (id_str, timestamp_ms, quote_count,
retweet_count, reply_count, favorite_count) VALUES (:id_str, :timestamp_ms, :quote_count,
:retweet_count, :reply_count, :favorite_count)'''

USER_INSERT = '''INSERT or IGNORE INTO users (user_id, screen_name, created_at, time_zone, name,
url, timestamp_ms, description) VALUES (:user_id, :screen_name, :created_at,
:time_zone, :name, :url, :timestamp_ms, :description)'''

USERCOUNTER_INSERT = '''INSERT or IGNORE INTO user_counters (user_id, timestamp_ms, statuses_count, followers_count, friends_count)
VALUES (:user_id, :timestamp_ms, :statuses_count, :followers_count, :friends_count)'''

URL_INSERT = '''INSERT or IGNORE INTO tweet_urls (id_str, user_id, entity, type, created_at)
VALUES (:id_str, :user_id, :entity, :type, :created_at)'''

MEDIA_INSERT = '''INSERT or IGNORE INTO tweet_media (id_str, user_id, entity, type, created_at)
VALUES (:id_str, :user_id, :entity, :type, :created_at)'''

HASHTAGMENTION_INSERT = '''INSERT or IGNORE INTO tweet_hashtagmentions (id_str, user_id, entity, type, created_at)
VALUES (:id_str, :user_id, :entity, :type, :created_at)'''

def parse_time(field):
    if field is not None:
        result = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(field,'%a %b %d %H:%M:%S +0000 %Y'))
    else:
        result = ""
    return result

def parse_entities(entities):
    entity_list = []
    if entities is not None:
        urls = entities.get("urls")
        if urls is not None and len(urls) > 0:
            url_links = [(u['expanded_url'], 'url') for u in urls]
            entity_list.extend(url_links)
        media = entities.get("media")
        if media is not None and len(media) > 0:
            media_links = [(m['media_url'], 'media') for m in media]
            entity_list.extend(media_links)
        hashtags = entities.get("hashtags")
        if hashtags is not None and len(hashtags) > 0:
            hashs = [(h['text'], "hashtag") for h in hashtags]
            entity_list.extend(hashs)
        mentions = entities.get("user_mentions")
        if mentions is not None and len(mentions) > 0:
            ments = [(m['screen_name'], "mention") for m in mentions]
            entity_list.extend(ments)
        symbols = entities.get("symbols")
        if symbols is not None and len(symbols) > 0:
            symbs = [(s['text'], "symbol") for s in symbols]
            entity_list.extend(symbs)
    return entity_list

def check_has_url(tweet):
    #filtering - I just want tweets that have a url or media
    entities = parse_entities(tweet.get("entities"))
    if tweet.get("truncated") is True:
        extended = tweet.get("extended_tweet")
        entities2 = parse_entities(extended.get("entities"))
        ext_entities = parse_entities(extended.get("extended_entities"))
        entities.extend(entities2)
        entities.extend(ext_entities)
    HAS_URL = False
    for e in entities:
        if e[1] == "url" or e[1] == "media":
            HAS_URL = True
    return HAS_URL

def write_tweet_record(cursor, tweet, timestamp = None):
    record = {col : None for col in tweet_cols}
    if timestamp is None:
        timestamp = parse_time(tweet.get("created_at"))
    retweet = tweet.get("retweeted_status")
    quote = tweet.get("quoted_status")
    has_url = check_has_url(tweet)
    if has_url:
        write_tweet_entities(cursor, tweet)
        write_tweet_counters(cursor, tweet, timestamp)
        is_extended = record.get("truncated") is True
        for col in tweet_cols:
            if col == "retweeted_status" and retweet is not None:
                record[col] = retweet.get('id_str')
                write_tweet_relation(cursor, tweet, retweet, "retweet", timestamp)
            elif col == "quoted_status" and quote is not None:
                record[col] = quote.get('id_str')
                write_tweet_relation(cursor, tweet, quote, "quote", timestamp)
            elif col == "user_id":
                user = tweet.get("user")
                record[col] = user.get("id_str")
                write_user_record(cursor, user, timestamp)
            elif col == "place" and tweet.get(col) is not None:
                place = tweet.get(col)
                record[col] = place.get("full_name")
            elif col == "text_content":
                if is_extended:
                    extended = tweet.get("extended_tweet")
                    record[col] = extended.get("full_text")
                else:
                    record[col] = tweet.get("text")
            else:
                record[col] = tweet.get(col)
        record['created_at'] = parse_time(record['created_at'])
        cursor.execute(TWEET_INSERT, record)
    if retweet is not None:
        has_url += write_tweet_record(cursor, retweet, timestamp)
    if quote is not None:
        has_url += write_tweet_record(cursor, quote, timestamp)
    return has_url

def write_tweet_relation(cursor, tweet, source, relationship, timestamp):
    record = {}
    record["id_str"] = tweet.get("id_str")
    record["source_id_str"] = source.get("id_str")
    record["user_id"] = tweet.get("user").get("id_str")
    record["source_user_id"] = source.get("user").get("id_str")
    record["relationship"] = relationship
    record["timestamp_ms"] = timestamp
    cursor.execute(TWEETRELATION_INSERT, record)

def write_tweet_counters(cursor, tweet, timestamp):
    record = {col: None for col in tweetcounter_cols}
    for col in tweetcounter_cols:
        if col == "timestamp_ms":
            record[col] = timestamp
        else:
            record[col] = tweet.get(col)
    cursor.execute(TWEETCOUNTER_INSERT, record)

def write_tweet_entities(cursor, tweet):
    id_str = tweet.get("id_str")
    user_id = tweet.get("user").get("id_str")
    created_at = parse_time(tweet.get("created_at"))
    entities = parse_entities(tweet.get("entities"))
    if tweet.get("truncated") is True:
        extended = tweet.get("extended_tweet")
        entities2 = parse_entities(extended.get("entities"))
        ext_entities = parse_entities(extended.get("extended_entities"))
        entities.extend(entities2)
        entities.extend(ext_entities)
    for e in entities:
        record = {}
        record['id_str'] = id_str
        record['user_id'] = user_id
        record['created_at'] = created_at
        e_type = e[1]
        record['type'] = e_type
        record['entity'] = e[0]
        if e_type == "url":
            cursor.execute(URL_INSERT, record)
        elif e_type == "media":
            cursor.execute(MEDIA_INSERT, record)
        else:
            cursor.execute(HASHTAGMENTION_INSERT, record)
        
def write_user_record(cursor, user, timestamp):
    record = {col: None for col in user_cols}
    for col in user_cols:
        if col == "user_id":
            record[col] = user.get("id_str")
        elif col == "timestamp_ms":
            record[col] = timestamp
        else:
            record[col] = user.get(col)
    record["created_at"] = parse_time(record['created_at'])
    write_user_counters(cursor, user, timestamp)
    cursor.execute(USER_INSERT, record)
    
def write_user_counters(cursor, user, timestamp):
    record = {col: None for col in usercounter_cols}
    for col in usercounter_cols:
        if col == "user_id":
            record[col] = user.get("id_str")
        elif col == "timestamp_ms":
            record[col] = timestamp
        else:
            record[col] = user.get(col)
    cursor.execute(USERCOUNTER_INSERT, record)
            
# create database if needed, or access
CREATE_DB = True
conn = sqlite3.connect("../mexico_urls.db")

c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS tweets
(id_str text primary key, user_id text, created_at timestamp, retweeted_status text,
quoted_status text, text_content text, lang text, place text, source text)''')
c.execute('''CREATE TABLE IF NOT EXISTS tweet_relations (id_str text, source_id_str text,
user_id text, source_user_id text, relationship text, timestamp_ms timestamp)''')
c.execute('''CREATE TABLE IF NOT EXISTS tweet_counters
(id_str text, timestamp_ms timestamp, quote_count int, retweet_count int,
                 reply_count int, favorite_count int, PRIMARY KEY(id_str, timestamp_ms))''')
c.execute('''CREATE TABLE IF NOT EXISTS users
(user_id text, screen_name text, created_at timestamp, time_zone text, name text,
         url text, timestamp_ms timestamp, description text, PRIMARY KEY(user_id, timestamp_ms))''')
c.execute('''CREATE TABLE IF NOT EXISTS user_counters
(user_id text, timestamp_ms timestamp, statuses_count int, followers_count int, friends_count int, PRIMARY KEY(user_id, timestamp_ms))''')
c.execute('''CREATE TABLE IF NOT EXISTS tweet_urls (id_str text, user_id text, entity text, type, text, created_at timestamp,
PRIMARY KEY(id_str, entity))''')
c.execute('''CREATE TABLE IF NOT EXISTS tweet_media (id_str text, user_id text, entity text, type text, created_at timestamp,
PRIMARY KEY(id_str, entity))''')
c.execute('''CREATE TABLE IF NOT EXISTS tweet_hashtagmentions (id_str text, user_id text, entity text, type text, created_at timestamp, PRIMARY KEY(id_str, entity))''')   
    
# getting source files
source_dir = "D:/twitter_data_bucket1"

source_files = ["{0}/{1}".format(source_dir, f)
                for f in os.listdir(source_dir) if f.startswith("6_") or f.startswith("7_1_")]
source_files.sort()
counter = 0
for fyle in source_files:
    print(fyle)
    with open(fyle, 'r') as f:
        c.execute("BEGIN TRANSACTION")
        file_counter = 0
        for line in f.readlines():
            try:
                tweet = json.loads(line)
                if tweet.get("created_at") is not None:
                    counter += write_tweet_record(c, tweet)
                file_counter += 1
                if counter % 20000 == 0:
                    print(counter)
                    c.execute("COMMIT")
                    c.execute("BEGIN TRANSACTION")
            except Exception as e:
                print("error: {0}".format(e))
        c.execute("COMMIT")
    #with open("file_records_tosql.csv", 'a') as out:
    #    print("{0},{1}".format(fyle.split("/")[-1][:-4], file_counter), file = out)
            

c.close()
conn.close()




