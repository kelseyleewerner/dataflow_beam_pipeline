import argparse
import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import window
import json 


class countDoFn(beam.DoFn):
    def process(self, element):
        return [('count', 1)]        


class countFn(beam.CombineFn):
    def create_accumulator(self):
        return 0
    
    def add_input(self, accumulator, input):
        accumulator += 1
        return accumulator

    def merge_accumulators(self, accumulators):
        total = 0
        for item in accumulators:
            total += item
        return total

    def extract_output(self, accumulator):
        return accumulator

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

        output = (
            messages
                | 'DecodeInput' >> beam.Map(lambda x: x.decode('utf-8'))
                | 'CountMovies' >> beam.ParDo(countDoFn())               
                | 'CreateWindows' >> beam.WindowInto(window.FixedWindows(15, 0))
                # | 'CountMovies' >> beam.CombineGlobally(countFn())
                | 'GroupByMysteryKey' >> beam.GroupByKey()
                | 'MakeStrings' >> beam.Map(lambda x: json.dumps(x)) 
                | 'EncodeOutput' >> beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes)
            )

        output | 'PublishToTopic' >> WriteToPubSub(known_args.output_topic)


if __name__ == '__main__':
    run()
