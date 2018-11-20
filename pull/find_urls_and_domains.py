import sqlite3
import requests
from urllib.parse import urlparse
import csv
#import multiprocessing


# if we expand the urls, what are the main domains we find?
# approach: I'm going to roll through the tweet_urls first to
# unshorten them (if they are) and write the true url.
# next, I'll create a new column in the table, find their domain,
# and left-join these domains back with the urls.

session = requests.Session()

#conn = sqlite3.connect("mexico_urls.db")
#c = conn.cursor()

url_results = []
url_errors = []


def get_url_info(url):
    try:
        resp = session.head(url, allow_redirects = True)
        full_url = resp.url
        parsed_url = urlparse(full_url)
        domain = parsed_url.netloc
        return (url, full_url, domain)
    except Exception as e:
        url_errors.append(url)
        return (url, None, None)

#url_col = c.execute('''select distinct(entity) from tweet_urls
#where entity not like 'https://twitter.com/%';''')

counter = 0
with open("unique_urls.txt", 'r') as fyle:
    for line in fyle.readlines():
        url = line.strip()
        if counter < 47001: # skipping 40000 - 41000
            counter += 1
            continue
        result = get_url_info(url)
        url_results.append(result)
        counter += 1
        if counter % 500 == 0:
            print ("Counter: {0}".format(counter))
        if counter % 1000 == 0:
            with open("url_results.csv", 'a', newline='\n') as out:
                writer = csv.writer(out)
                if counter == 1000:
                    writer.writerow(['url', 'full_url', 'domain'])
                for r in url_results:
                    writer.writerow(r)
                url_results = []
            with open("url_errors.csv", 'a', newline='\n') as out:
                writer = csv.writer(out)
                if counter == 1000:
                    writer.writerow(['url'])
                for e in url_errors:
                    writer.writerow([e])
                url_errors = []
            
        
        
