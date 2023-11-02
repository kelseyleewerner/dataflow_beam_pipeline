import argparse
import csv
from google.cloud import pubsub_v1



# import pprint
# pp = pprint.PrettyPrinter(indent=4)


# Parse argument from user
parser = argparse.ArgumentParser()
parser.add_argument(
    "--project-id",
    dest="project_id")
parser.add_argument(
    "--topic",
    dest="topic")
args = parser.parse_args()


# Create PubSub publisher
publisher = pubsub_v1.PublisherClient()
topic_name = F"projects/{args.project_id}/topics/{args.topic}"

# Each line of the csv is publiished as a dictionary where the keys are the column names
with open("horror_movies.csv", mode="r") as infile:
    messages = csv.DictReader(infile)

    for message in messages:
        publisher.publish(args.topic, message.encode("utf-8"))
        future.result()
