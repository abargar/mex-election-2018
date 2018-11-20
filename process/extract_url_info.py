# -*- coding: utf-8 -*-
import json
import csv

FIELDS =  ["id",
          "timestamp",
          "user_id",
          "screen_name",
          "url",
          "source",
          "retweeted_tweet",
          "retweeted_user",
          "quoted_tweet",
          "quoted_user"]

def parse_tweet_info(tweet):
    ""
    ""
    output = {field : None for field in FIELDS}
    output['id'] = tweet.get('id_str')
    output['timestamp'] = tweet.get("created_at")
    user = tweet.get('user')
    if user is not None:
        output['user_id'] = user.get('id_str')
        output['screen_name'] = user.get('screen_name')
    output['source'] = tweet.get('source')
    retweet = tweet.get('retweeted_status')
    if retweet is not None:
        output['retweeted_tweet'] = retweet['id_str']
        output['retweeted_user'] = retweet['user'].get('id_str')
    quote = tweet.get('quoted_status')
    if quote is not None:
        output['quoted_tweet'] = quote.get('id_str')
        output['quoted_user'] = quote['user'].get('id_str')
    return output

def parse_tweet_urls(tweet):
    ""
    ""
    if tweet.get('truncated') is True:
        entities = tweet['extended_tweet'].get('entities')
        entities_ext = tweet['extended_tweet'].get('extended_entities')
        urls = extract_links(entities)
        urls_ext = extract_links(entities_ext)
        urls.extend(urls_ext)
    else:
        entities = tweet.get('entities')
        urls = extract_links(entities)
    return urls    

def extract_links(entities):
    ""
    ""
    if entities is None:
        return []
    else:
        urls = entities.get("urls")
        url_links = []
        media_links = []
        if urls is not None and len(urls) > 0:
            url_links = [u['expanded_url'] for u in urls] 
        media = entities.get("media")
        if media is not None and len(media) > 0:
            media_links = [m['media_url'] for m in media]
        all_links = url_links + media_links
        return all_links

filenames = ["D:/twitter_data_local/6_13_{0}.txt".format(hour)
             for hour in range(5)]
with open("D:/youtube_links_6_13.csv", 'a', encoding="utf-8", newline='') as youtubes:
    youtube_writer = csv.DictWriter(youtubes, fieldnames=FIELDS)
    with open("D:/urls_6_13.csv", "a", encoding="utf-8", newline='') as out:
        writer = csv.DictWriter(out, fieldnames=FIELDS)
        writer.writeheader()
        for filename in filenames:
            print(filename)
            try:
                with open(filename, 'r') as test:
                    for line in test.readlines():
                        tweet = json.loads(line)
                        urls = parse_tweet_urls(tweet)
                        for u in urls:
                            output = parse_tweet_info(tweet)
                            output['url'] = u
                            writer.writerow(output)
                            if "youtu.be" in u or "youtube" in u:
                                youtube_writer.writerow(output)
                        if tweet.get("quoted_status") is not None:
                            quote = tweet.get("quoted_status")
                            q_urls = parse_tweet_urls(quote)
                            for u in q_urls:
                                output = parse_tweet_info(quote)
                                output['url'] = u
                                writer.writerow(output)
                                if "youtu.be" in u or "youtube" in u:
                                    youtube_writer.writerow(output)
                        if tweet.get("retweeted_status") is not None:
                            retweet = tweet.get("retweeted_status")
                            r_urls = parse_tweet_urls(retweet)
                            for u in r_urls:
                                output = parse_tweet_info(retweet)
                                output['url'] = u
                                writer.writerow(output)
                                if "youtu.be" in u or "youtube" in u:
                                    youtube_writer.writerow(output)
            except Exception as e:
                print(e)
                
                    
            
                
                
                    
