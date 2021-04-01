from kafka import KafkaConsumer
import pymongo
from pymongo import *
import json


c=KafkaConsumer("topic_twitter",bootstrap_servers=['localhost:9092'],api_version=(2,6,0))

def process_msg(msg):
    print(msg.offset)
    print(json.loads(msg.value))

for msg in c :
    print(msg)
    msg = json.loads(msg.value)

process_msg(msg)
