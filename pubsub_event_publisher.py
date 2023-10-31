import csv

import pprint
pp = pprint.PrettyPrinter(indent=4)

with open("horror_movies.csv", mode="r") as infile:
    messages = csv.DictReader(infile)

    for message in messages:
        pp.pprint(message)
