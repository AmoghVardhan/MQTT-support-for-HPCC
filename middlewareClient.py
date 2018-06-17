#Special Middleware Client which is subscribed to all the other client publishing
# receievs the message published by other clients and push into IPC MESSAGE QUEUE
import paho.mqtt.client as mqtt
import time
from sys import exit
import signal
import os
#Path where the IPC MESSAGE QUEUE is Created
path = "/home/ksav/Desktop/HPCC/src/my_program.fifo"
os.mkfifo(path)

#MQTT Mosquitto Broker IP and PORT and subscribing to all topics
broker="127.0.0.1"
port=1883
sub_topic="#"

# function to delete the IPC MESSAGE Queue file after EXIT
def sigint_handler(signum, frame):
    os.unlink(path)
    print("\nFile Removed")
    exit("error")
signal.signal(signal.SIGINT, sigint_handler)

# on_subscribe callback function
def on_subscribe(client, userdata, mid, granted_qos):   #create function for callback
   print("subscribed with qos",granted_qos, "\n")
   pass
# on_message callback function
def on_message(client, userdata, message):
    fifo = open(path, "w")
    fifo.write(str(message.payload.decode("utf-8")))
    fifo.close()
    print("message received  "  ,str(message.payload.decode("utf-8")))
    print(message.topic)

# on_publish callback function
def on_publish(client,userdata,mid):   #create function for callback
   print("data published mid=",mid, "\n")
   pass

# on_disconnect callback function
def on_disconnect(client, userdata, rc):
   print("client disconnected ok")
# creating an instance of the client clientT
client= mqtt.Client("clienT")

# Assigning the callback functions to the client instance
client.on_subscribe = on_subscribe
client.on_publish = on_publish
client.on_message = on_message
client.on_disconnect = on_disconnect
print("connecting to broker ",broker,"on port ",port)
client.connect(broker,port)
#client.loop_start()
print("subscribing to ",sub_topic)
client.subscribe(sub_topic)
client.loop_forever()
client.disconnect()
