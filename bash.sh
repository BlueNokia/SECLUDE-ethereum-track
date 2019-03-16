#! /bin/sh

echo "ENTER THE FILE NAME" 
read STRING
echo "ENTER THE SIZE OF A SEGMENT"
read size
cd /home/uddeshya/Documents/split1
split -b $size $STRING  seg
mkdir para
mv seg* para
zip -r para.zip para
#cd segment 
#cat seg* > combine.txt
#mv combine.txt split1
