import os
import subprocess

# arg = 'cd /home/captainlazarus/hackfest/host/'
# subprocess.run(arg , shell=True)

arg = 'cat /home/captainlazarus/hackfest/host/para*> para.zip'
subprocess.run(arg , shell=True)