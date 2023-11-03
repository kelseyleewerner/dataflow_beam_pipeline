import argparse


def run(argv=None, save_main_session=True):
    # Code for parsing command line arguments was copied from https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/streaming_wordcount.py
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--output_topic',
      required=True,
      help='PubSub output topic should have the form of projects/<PROJECT>/topics/<TOPIC>')
    group = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument(
      '--input_topic',
      required=True,
      help='PubSub input topic should have the form of projects/<PROJECT>/topics/<TOPIC>')
    parser.add_argument(
      '--input_subscription',
      required=True,
      help='PubSub input subscription should have the form of projects/<PROJECT>/subscriptions/<SUBSCRIPTION>')
    known_args, pipeline_args = parser.parse_known_args(argv)