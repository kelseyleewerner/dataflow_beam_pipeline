import argparse
import pprint
from google.cloud import pubsub_v1

pp = pprint.PrettyPrinter(indent=4)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--project-id",
    dest="project_id")
parser.add_argument(
    "--sub-name",
    dest="sub_name")
args = parser.parse_args()

# Code created from this example: 
# https://cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.subscriber.client.Client#google_cloud_pubsub_v1_subscriber_client_Client_subscribe

# Create PubSub subscriber
subscriber_client = pubsub_v1.SubscriberClient()
subscription = subscriber_client.subscription_path(args.project_id, args.sub_name)

def callback(message):
    pp.pprint(message.data)
    message.ack()

future = subscriber_client.subscribe(subscription, callback)

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()
    future.result()
