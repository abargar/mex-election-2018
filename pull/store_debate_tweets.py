import boto3
import json
import os
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()
name_format = "debate/{m}_{d}_{h}.txt"
tag_format = "{m}_{d}_{h}.txt"
s3 = boto3.client('s3')
bucket_name= 'deldebate'

@sched.scheduled_job('interval', hours=1)
def timed_job():
	dt = datetime.now() - timedelta(hours=1)
	hour = dt.hour
	day = dt.day
	month = dt.month
	filename = name_format.format(h=hour, d=day, m=month)
	tag = tag_format.format(h=hour, d=day, m=month)
	#print(filename)
	try:
		s3.upload_file(filename, bucket_name, tag)
	except Exception as e:
            print("Error occured when uploading {f}: {err}".format(f=filename, err=e))
            pass
	dt2 = dt - timedelta(hours=2)
	filename2 = name_format.format(h=dt2.hour, d=dt2.day, m=dt2.month)
	try:
		os.remove(filename2)
	except OSError:
		pass

sched.start()

