from minio import Minio
import time
import subprocess
import os

port = 9000

ip = "localhost:"

client = Minio( ip+str(port) , 
                access_key='aditya',
                secret_key='aditya123',
                secure = False)

port+=1

host1 = Minio(
                ip+str(port) , 
                access_key='rohan',
                secret_key='rohan123',
                secure = False
         )

port+=1

host2 = Minio(
                ip+str(port) , 
                access_key='hrithik',
                secret_key='hrithik123',
                secure = False
            )

# buck = client.list_buckets()
# for x in buck:
#     obj = client.list_objects(x.name)
#     #obj = [x.object_name for x in obj]
#     for y in obj:
#         client.remove_object(x.name , y.object_name)
#     client.remove_bucket(x.name)

buck = host1.list_buckets()
for x in buck:
    obj = host1.list_objects(x.name)
    #obj = [x.object_name for x in obj]
    for y in obj:
        host1.remove_object(x.name , y.object_name)
    host1.remove_bucket(x.name)

buck = host2.list_buckets()
for x in buck:
    obj = host2.list_objects(x.name)
    for y in obj:
        host2.remove_object(x.name , y.object_name)
    host2.remove_bucket(x.name)