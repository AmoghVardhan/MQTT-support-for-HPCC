import os
import sys
import json
import requests
headers = {'Content-Type' : 'application/json;charset=UTF-8'}
url = "http://192.168.43.34:8002/WsEcl/json/query/roxie"
path = "/home/ksav/Desktop/HPCC/src/my_program.fifo"

while True:
    fifo = open(path, "r")
    for line in fifo:
        data1 = line
        print(line)
        r=requests.post(url,data=data1,headers=headers)
        rec = json.loads(r.text)
        print(rec)
    fifo.close()
print('outside')
