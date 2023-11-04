import argparse
import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions




def run(argv=None, save_main_session=True):
    # Code for parsing command line arguments was copied from https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/streaming_wordcount.py
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--output_topic',
      required=True,
      help='PubSub output topic should have the form of projects/<PROJECT>/topics/<TOPIC>')
    parser.add_argument(
      '--input_topic',
      required=True,
      help='PubSub input topic should have the form of projects/<PROJECT>/topics/<TOPIC>')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        messages = (p | 'SubscribeToTopic' >> ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes))

        output = messages | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))


if __name__ == '__main__':
    run()
