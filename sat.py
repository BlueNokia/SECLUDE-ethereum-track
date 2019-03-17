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
    if len(a) == len(b):
        return 0
    c = len( set(a) - set(b))
    #print('c is: ' , c , '\n')
    if c > 0:
        return 1
    else:
        return 0
def upload(bucketName , a1):
    #print('Entered Upload Function\n')
    #Finding ports that are listening
    listen = []
    port = 9000

    for i in range(1,10):
        if(check_socket('127.0.0.1' , port+i)):
            listen.append(port+i)
    selected = select_port(listen) #Complete Function
    #print(listen)

    a1 = [y for y in a1 if not (re.search(".txt" , y.object_name)) ]

    for obj in a1:
        reqname = list((obj.object_name).split("."))[0]
        path = '../client/data/'
        client.fget_object(bucketName , obj.object_name , path+obj.object_name)
        print('Downloaded ' , obj.object_name , ' to ' , path+obj.object_name)
        
        #Writing to text
        fname = "../client/text/" + reqname +".txt"
        meta = client.stat_object(bucketName , obj.object_name)
        file = open(fname , 'w')
        file.write(str(meta.object_name) + ' ' + str(meta.bucket_name) + ' ' +  str(meta.size))
        for selected_host in selected:
            file.write(' ' + str(selected_host))
        file.close()
        client.fput_object(bucketName , (reqname+".txt") , fname)
        print( (reqname+".txt") , ' uploaded')
        #print(obj.object_name , ' removed')

        #Code for splitting
        arg = "mkdir /home/captainlazarus/hackfest/client/split_data/" + reqname
        subprocess.call(arg , shell = True)

        # arg = 'mv ../client/data/' + obj.object_name + ' /home/captainlazarus/hackfest/client/split_data/'+reqname
        # subprocess.run(arg , shell=True)

        arg = 'split -b 15k /home/captainlazarus/hackfest/client/data/' + obj.object_name + ' ' + reqname[::-1]
        subprocess.call(arg , shell=True)

        arg = 'mv ' + reqname + '* ' + '/home/captainlazarus/hackfest/client/split_data/' + reqname
        subprocess.call(arg , shell=True)

        # subprocess.run(
        #     "cd /home/captainlazarus/hackfest/codes" , shell=True
        # )

        #arg = 'cat ' + reqname + '* >' + obj.object_name
        # arg = ''
        # subprocess.call(arg , shell=True)

        ######Dont cross

        dirpath = '../client/split_data/'+reqname
        a = os.listdir(dirpath)
        #print(a)

        try:
            for testHost in host:
                testHost.make_bucket(bucketName)
                print('Created bucket ' , bucketName , ' in ' , testHost)  
        except:
            pass

        sum = 0
        for x in range(len(host)):
            z = round(Relib[x]*len(a)/TotalRelib)
            for k in range(sum,sum+z):
                #print(k)
                path = ('../client/split_data/' + reqname + '/' + a[k])
                host[x].fput_object(bucketName , a[k] , path)
            sum += z 

        print(obj.object_name , ' removed')
        client.remove_object(bucketName , obj.object_name)    

    

def download(a):
    for k in a:
        #print(k.object_name)
        fname = list((k.object_name).split('.'))[0]
        print(fname)
        fpath =  '/home/captainlazarus/hackfest/client/text/'+fname+'.txt'
        file = open(fpath , 'r')
        meta = file.readlines(100000)
        #print('Meta is: ' , meta)
        meta = meta[0].split()
        #print('Meta is: ' , meta)
        ports = meta[3:]
        for Host in host:
            #print(meta)
            objects = Host.list_objects(meta[1])    #List from bucket
            #print(1)
            objects = [x for x in objects if (re.match(fname , x.object_name))] #Find matching file segments
            #print(2)
            #print()
            #print()
            # print(objects)
            # print()
            # print()
            for z in objects:   #Download File segments
                #print(z.object_name)
                # try:
                #     subprocess.call('mkdir /home/captainlazarus/hackfest/host/'+fname)
                # except:
                #     pass
                opath = '/home/captainlazarus/hackfest/host/'+z.object_name
                Host.fget_object(meta[1] , z.object_name , opath)
        # dirpath = '../host'
        #a  os.listdir(dirpath)
        arg = 'cat /home/captainlazarus/hackfest/host/' + fname + '*>' +  z.object_name
        subprocess.run(arg , shell=True)

    

#Global
objectList = []

while flag:

    clientBuckets = client.list_buckets()
    #print(clientBuckets[0].name)
    updatedList = list(client.list_objects(clientBuckets[0].name))

    # for x in updatedList:
    #     print(x.object_name)
    # for x in objectList:
    #     print(x.object_name)

    c1 = check(updatedList , objectList)
    c2 = check(objectList , updatedList)
    # for x in updatedList:
    #     print(x.object_name)
    #print("c1 is: " , c1)
    #print("c2 is: " , c2)
    if c1 == 1:
        # for x in updatedList:
        #     print(x.object_name)
        upload(clientBuckets[0].name , updatedList)
    elif c2 == 1:
        a = list( set(objectList) - set(updatedList) )
        print("a is: " , a)
        download(a)


    objectList = updatedList

    #After the loop ends

    loops+=1
    if loops == 0:
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


    time.sleep(3)
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
