#!/bin/env/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import requests
import json
from pprint import pprint

from requests.api import head, post

hst: str = "localhost"
port: int = 8082

TEST_TOPIC: str = "test"

# testing
# hst = "httpbin.org"
# port = 80
# testin end


post_header = {"Content-Type": "application/vnd.kafka.v2+json"}
accept_headers = {
    "Accept": "application/vnd.kafka.json.v2+json",
}

json_header = {"Content-Type": "application/json"}



def get_cluster_id():

    resp = requests.get(
        f"http://{hst}:{port}/v3/clusters",
    )
    json = resp.json()
    return json.get("data")[0].get("cluster_id")


def create_consumer():
    consumer_data = {"format": "json", "name": "test_consumero"}
    r = requests.post(
        f"http://{hst}:{port}/consumers/cg1",
        data=json.dumps(consumer_data),
        headers=post_header,
    )
    if r.text:
        return r.json().get("instance_id", "")




def create_topic(topic=None):

    topic = topic or TEST_TOPIC

    topic_creation_data = {
        "topic_name": topic,
    }

    r = requests.post(
        f"http://{hst}:{port}/v3/clusters/{cluster_id}/topics",
        data=topic_creation_data,
        headers=json_header,
    )



def subscribe_to_topic(consumer_id):
    post_data = {
        "topics": [TEST_TOPIC],
    }

    post_data_json = json.dumps(post_data)

    resp = requests.post(
        f"http://{hst}:{port}/consumers/cg1/instances/{consumer_id}/subscription",
        data=json.dumps(post_data),
        headers=post_header,
    )
    if resp.text:
        pprint(resp.json())



def create_records(topic):
    records = {
        "records": [
            {"key": "alice", "value": {"count": 0}},
            {"key": "alice", "value": {"count": 1}},
            {"key": "alice", "value": {"count": 2}},
        ]
    }

    headers = {
        "Content-Type": "application/vnd.kafka.json.v2+json",
        "Accept": "application/vnd.kafka.v2+json",
    }

    resp = requests.post(
        f"http://{hst}:{port}/topics/{topic}",
        data=json.dumps(records),
        headers=headers,
    )
    if resp.text:
        pprint(resp.json())

def read_records(consumer_id):

    resp = requests.get(
        f"http://{hst}:{port}/consumers/cg1/instances/{consumer_id}/records",
        headers=accept_headers,
    )
    pprint(resp.json())


# cluster_id = get_cluster_id()
# print(f"cluster id: {cluster_id}")
# create_topic("test")
# print("Topic created")

consumer_id = create_consumer()
consumer_id = consumer_id or "test_consumero"
consumer_id = "test_consumero"
print(f"Consumer created -> {consumer_id}")
subscribe_to_topic(consumer_id)
print("Subscribed to topic")
# create_records(TEST_TOPIC)
# print("Created records in topic")
read_records(consumer_id)
