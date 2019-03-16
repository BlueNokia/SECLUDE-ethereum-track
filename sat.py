from minio import Minio
import time
import subprocess
import os

host = []
Relib = [100,100]
TotalRelib = Relib[0]+Relib[1]

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

host.append(host1)
host.append(host2)

buck = client.list_buckets()
#reqhosts = 0
flag = True

loops = 0

while flag:

    #Downloading Objects from buckets
    for x in buck:

        #print(x.name)
        obj = client.list_objects(x.name)
        for y in obj:
            path = '../client/data/'
            client.fget_object(x.name , y.object_name , path+y.object_name)
            print('Uploaded: ' , y.object_name)

        #Making buckets for hosts
        try:
            for j in range(2):
                host[j].make_bucket(x.name)
        except:
            pass

    bashP = ('/home/captainlazarus/hackfest/Client/data/'+y.object_name)
    #Splitting zip file
    subprocess.call([   'bash',
                        '/home/captainlazarus/hackfest/Codes/bash.sh',
                        y.object_name , str(10000) , bashP])

    #Finding hosts and sending files
    
    #Getting the folder name from zip file
    reqname = list((y.object_name).split("."))[0]
    print(reqname)
    
    
    dirpath = '../client/split_data/'+reqname
    a = os.listdir(dirpath)
    sum = 0
    for x in range(len(host)):
        z = round(Relib[x]*len(a)/TotalRelib)
        for k in range(sum,sum+z):
            #print(k)
            path = ('../client/split_data/para/'+a[k])
            host[x].fput_object(y.bucket_name , a[k] , path)
        sum += z 



    if loops == 5:
        flag = False
    loops+=1

    #Delete the original file
    

    time.sleep(1)
    # #Replace by general code
