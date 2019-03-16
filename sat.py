from minio import Minio
import time

port = 9000

ip = "localhost:"

client = Minio( ip+port , 
                access_key='aditya',
                secret_key='aditya123',
                secure = False)
port+=1

host[0] = Minio(
                ip+port , 
                access_key='rohan',
                secret_key='rohan123',
                secure = False
            )

host[1] = Minio(
                ip+port , 
                access_key='rohan',
                secret_key='rohan123',
                secure = False
            )


buck = client.list_buckets()
reqhosts = 0
flag = True

while flag:
    
    #Downloading Objects from buckets
    for x in buck:
        #print(x.name)
        obj = client.list_objects(x.name)
        for y in obj:
            #print(y.object_name)
            path = '/home/captainlazarus/hackfest/client/data/'
            client.fget_object(x.name , y.object_name , path+y.object_name)
            print('Uploaded: ' , y.object_name)

            extract()

            with open(path+ , "r") as f:
                pass

            reqhosts += 1
    


    time.sleep(3)