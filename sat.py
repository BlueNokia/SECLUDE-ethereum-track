from minio import Minio
import time
import subprocess
import os
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
BucketAlreadyExists)
import socket
from contextlib import closing
import re

def check_socket(host, port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex((host, port)) == 0:
            return 1
        else:
            return 0

def select_port(listen):
    return listen

host = []
Relib = [100,50]
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

#reqhosts = 0
flag = True
loops = 0

def check(a , b):
    #print('Entered Check Function\n')
    c = len( set(a) - set(b) )
    print(c , '\n')
    if c>0:
        return 1
    elif c<0:
        return -1
    else:
        return 0
def upload(bucketName , a):
    #print('Entered Upload Function\n')
    #Finding ports that are listening
    listen = []
    port = 9000

    for i in range(1,10):
        if(check_socket('127.0.0.1' , port+i)):
            listen.append(port+i)
    selected = select_port(listen) #Complete Function
    #print(listen)

    a = [y for y in a if (re.search(".zip" , y.object_name)) ]

    for obj in a:
        reqname = list((obj.object_name).split("."))[0]
        path = '../client/data/'
        client.fget_object(bucketName , obj.object_name , path+obj.object_name)
        print('Downloaded ' , obj.object_name , ' to ' , path+obj.object_name)
        
        #Writing to text
        fname = "../client/text/" + reqname +".txt"
        meta = client.stat_object(bucketName , obj.object_name)
        file = open(fname , 'w')
        file.write(str(meta.object_name) + '\n' + str(meta.bucket_name) + '\n' +  str(meta.size))
        for selected_host in selected:
            file.write('\n' + str(selected_host))
        file.close()
        client.fput_object(bucketName , (reqname+".txt") , fname)
        print( (reqname+".txt") , ' uploaded')
        print(obj.object_name , ' removed')
        client.remove_object(bucketName , obj.object_name)
    try:
        for testHost in host:
            testHost.make_bucket(bucketName)
            print('Created bucket ' , bucketName , ' in ' , testHost)  
    except:
        pass

        # Getting the folder name from zip file
        #print(reqname)
        
        dirpath = '../client/split_data/'+reqname
        a = os.listdir(dirpath)

        sum = 0
        for x in range(len(host)):
            z = round(Relib[x]*len(a)/TotalRelib)
            for k in range(sum,sum+z):
                #print(k)
                path = ('../client/split_data/' + reqname + '/' + a[k])
                host[x].fput_object(y.bucket_name , a[k] , path)
            sum += z 




def download(a):
    for k in a:
        print(k.object_name)

#Global
objectList = []

while flag:

    clientBuckets = client.list_buckets()
    updatedList = list(client.list_objects(clientBuckets[0].name))

    # for x in updatedList:
    #     print(x.object_name)


    c1 = check(updatedList , objectList)

    # for x in updatedList:
    #     print(x.object_name)

    if c1 == 1:
        # for x in updatedList:
        #     print(x.object_name)
        upload(clientBuckets[0].name , updatedList)
    elif c1 == -1:
        a = list( set(objectList) - set(updatedList) )
        download(a)


    objectList = updatedList

    #After the loop ends

    loops+=1
    if loops == 1:
        flag = False
        buck = host[1].list_buckets()
        for x in buck:
            obj = host[1].list_objects(x.name)
        #obj = [x.object_name for x in obj]
            for y in obj:
                host[1].remove_object(x.name , y.object_name)
            host[1].remove_bucket(x.name)

        buck = host[0].list_buckets()
        for x in buck:
            obj = host[0].list_objects(x.name)
            for y in obj:
                host[0].remove_object(x.name , y.object_name)
            host[0].remove_bucket(x.name)


    time.sleep(1)
    #Replace by general code


    #     #Downloading Objects from buckets
    # for x in buck:
    #     obj = client.list_objects(x.name)
        
    #     #Ignoring txt files
    #     obj = [y for y in obj if not (re.search(".txt" , y.object_name)) ]

    #     for y in obj:
    #         path = '../client/data/'
    #         client.fget_object(x.name , y.object_name , path+y.object_name)
    #         #print('Downloaded ' , y.object_name , ' to ' , path+y.object_name)
            
    #         #Writing to text
    #         fname = "../client/text/" + y.object_name+".txt"
    #         meta = client.stat_object(x.name , y.object_name)
    #         file = open(fname , 'w')
    #         file.write(str(meta.object_name) + '\n' + str(meta.bucket_name) + '\n' +  str(meta.size))
    #         for selected_host in selected:
    #             file.write('\n' + str(selected_host))
    #         file.close()
    #         client.fput_object(x.name , (y.object_name+".txt") , fname)
    #         print( (y.object_name+".txt") , ' uploaded')
    #         print(y.object_name , ' removed')
    #         client.remove_object(x.name , y.object_name)
            


    #     #Making buckets for hosts
    #     try:
    #         for j in host:
    #             j.make_bucket(x.name)
    #             print('Created bucket ' , x.name , ' in ' , j)  
    #     except:
    #         pass

    # bashP = ('/home/captainlazarus/hackfest/client/data/' + y.object_name)
    # #Splitting zip file
    # subprocess.call([   'bash',
    #                     '/home/captainlazarus/hackfest/codes/bash.sh',
    #                     y.object_name , str(1000) , bashP , ])
    
    # #Getting the folder name from zip file
    # reqname = list((y.object_name).split("."))[0]
    # #print(reqname)
    
    # dirpath = '../client/split_data/'+reqname
    # a = os.listdir(dirpath)

    # sum = 0
    # for x in range(len(host)):
    #     z = round(Relib[x]*len(a)/TotalRelib)
    #     for k in range(sum,sum+z):
    #         #print(k)
    #         path = ('../client/split_data/' + reqname + '/' + a[k])
    #         host[x].fput_object(y.bucket_name , a[k] , path)
    #     sum += z 
