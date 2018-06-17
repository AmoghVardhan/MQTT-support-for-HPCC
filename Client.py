# Simulating Clients to send testing messages to the broker
import paho.mqtt.client as mqtt
import time
import sys
import json
# On Connect call back function
def on_connect(client,userdata,flags,rc):
    if rc==0:
        client.connected_flag = True
        print("connected ok")
    else:
        print("bad connection returned code=",rc)

mqtt.Client.connected_flag = False
# MQTT Mosquitto Broker IP and PORT
broker = "127.0.0.1"
port = 1883
#Creating an instance of the client with name python1
client = mqtt.Client("python1")
#Assigning the on_connect callback function
client.on_connect=on_connect
print("Connecting to broker",broker)
try:
    client.connect(broker,port)
except:
    print("can't connect")
    sys.exit(1)
client.loop_start()
while not client.connected_flag:
    print("in wait loop")
    time.sleep(1)
print("in main loop")
for i in range(1,10):
    #Sample Message sent from the Simulating Client
    client.publish("house/main-light",json.dumps({"kafkapublish": {
  "Message": {
    "Row": [
      {
        "relative_timestamp": 100,
        "vehicle_id": 1,
        "x": 12.345,
        "y": 98.765,
        "speed": 10
      }
    ]
  }
}}))
    time.sleep(1)
time.sleep(10)
client.loop_stop()
client.disconnect()
