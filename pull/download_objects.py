import boto3
client = boto3.client('s3')

files_b2 = client.list_objects(Bucket = "bucket_name")
keys = [content['Key'] for content in files_b2['Contents']]
keyset = ["7_1_{0}.txt".format(x) for x in range(20,24)]
keyset_2 = ["7_2_{0}.txt".format(x) for x in range(0, 24)]
keyset_3 = ["7_3_{0}.txt".format(x) for x in range(0, 6)]
keyset.extend(keyset_2)
keyset.extend(keyset_3)
#filtered_keys = list(filter(lambda k: "6__12__6_13/" not in k, keys))
#filtered_keys = list(filter(lambda k: "6_15" not in k, filtered_keys))

for key in keyset[6:]:
	print(key)
	try:
		client.download_file("bucket_name", key, "{0}".format(key))
	except Exception as e:
		print(e)
		break
