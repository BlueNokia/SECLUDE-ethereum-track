# Host should check periodically for newdata available and download
# Import Minio library.
import time
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

# Initialize minioClient with an endpoint and access/secret keys.

minioClient = Minio('172.16.10.153:9001',
                    access_key='rohan',
                    secret_key='rohan123',
                    secure=False)

# listing the buckets 
while True:                   
	buckets = minioClient.list_buckets()
	for bucket in buckets:
		print(bucket.name,bucket.creation_date)
		
	# Since only 1 bucket is used therefore 1st element in the list is said to be bucket

	bucket = buckets[0].name
           

	#Getting objects in the bucket

	objects = minioClient.list_objects(bucket ,recursive=False)
	address = '/home/uddeshya/Miniocodes/'
	# printing properties of the object
	#creating a file for database
	file1 = open('bucket1.txt','w+')
	for obj in objects:
		print(obj.bucket_name, obj.object_name, obj.size, obj.content_type)
		filename = obj.object_name
		
		file1.write(obj.bucket_name)
		print('\n')
		file1.write(obj.object_name)
		#downloading the object in the bucket to specific folder(here)-->>('/home/uddeshya/Miniocodes/)
		try:
	    		print(minioClient.fget_object(bucket,filename,address+filename))
		except ResponseError as err:
	    		print(err)
	# Get current policy of all object paths in bucket "mybucket"
	file1.close()
	
	# Make a bucket with the make_bucket API call.
	try:
       		minioClient.make_bucket("maylogs")
	except BucketAlreadyOwnedByYou as err:
		pass
	except BucketAlreadyExists as err:
       		pass
	except ResponseError as err:
       		raise
	else:
        # Put an object 'pumaserver_debug.log' with contents from 'pumaserver_debug.log'.
		try:
			minioClient.fput_object(maylogs,bucket1.txt,'/home/uddeshya/Miniocodes/bucket1.txt')
		except ResponseError as err:
			print(err)
	time.sleep(5)	