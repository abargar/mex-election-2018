import re
import csv

filename = ""

new_filename = ""

with open(new_filename, 'w', newline='') as out:
    writer = csv.writer(out)
    writer.writerow(["link", "type", "vid", "aid", "lid"])         
    with open(filename, 'r') as fyle:
        for line in fyle.readlines():
            line = line.strip()
            vid = None
            aid = None
            #print(line)
            if "/c/" in line or "channel" in line:
                linktype = "channel"
                cid = ""
            elif "search_query" in line:
                linktype = "search"
                termsearch = re.search("(?<=search_query=)[\w+]+", line)
                searchterms = termsearch.group()
            elif "user" in line:
                linktype = "user"
            elif "attribution_link" in line:
                linktype = "attributedvideo"
                aidsearch = re.search("(?<=a=)[A-Za-z\d_-]{11}", line)
                if aidsearch:
                    aid = aidsearch.group(0)
                else:
                    aid = None
                vidsearch = re.search("(?<=v%3D)[A-Za-z\d_-]+", line)
                if vidsearch:
                    vid = vidsearch.group()
                else:
                    vid = None
                listsearch = re.search("(?<=list=)[A-Za-z\d_-]+", line)
                if listsearch:
                    lid = listsearch.group()
                else:
                    lid = None
            elif "time_continue" in line:
                linktype = "video"
                vidsearch = re.search("(?<=v=)[A-Za-z\d_-]+", line)
                if vidsearch:
                    vid = vidsearch.group()
                else:
                    vid = None
                
            else:
                linktype = "video"
                vidsearch = re.search("[A-Za-z\d_-]{11}", line)
                aid = None
                if vidsearch:
                    vid = vidsearch.group()
                else:
                    vid = None
                listsearch = re.search("(?<=list=)[A-Za-z\d_-]+", line)
                if listsearch:
                    lid = listsearch.group()
                else:
                    lid = None
                #list 
            #print(linktype)
            if linktype == "video" or linktype == "attributedvideo":
                writer.writerow([line, linktype, vid, aid, lid])
