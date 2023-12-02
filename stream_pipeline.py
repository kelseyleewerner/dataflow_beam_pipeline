import apache_beam as beam
import argparse
import json
import time
import traceback
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import window
from google.cloud import logging


LOG_NAME = "horror_movie_dataset"

def log_error(log, err_message):
    log.log_struct(
        {
            "message": err_message,
            "error": traceback.format_exc()
        },
        severity="ERROR"
    )


class countDoFn(beam.DoFn):
    def process(self, element):
        try:
            return [('film_count', 1)]  
        except:
            logging_client = logging.Client()
            logger = logging_client.logger(LOG_NAME)
            log_error(logger, "Error occurred while counting number of Pub/Sub messages received in a window")
            raise      


def run(argv=None, save_main_session=True):
    logging_client = logging.Client()
    logger = logging_client.logger(LOG_NAME)

    try:
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
    except:
        log_error(logger, "Error occurred while parsing command line arguments")
        raise

    logger.log_text("Commencing stream of horror movie data from one Pub/Sub topic to another Pub/Sub topic", severity="INFO")

    with beam.Pipeline(options=pipeline_options) as p:
        try:
            messages = (p | 'SubscribeToTopic' >> ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes))
        except:
            log_error(logger, "Error occurred while reading pipeline input from Pub/Sub topic")
            raise

        try:
            output = (
                messages
                    | 'DecodeInput' >> beam.Map(lambda x: x.decode('utf-8'))
                    | 'CountMovies' >> beam.ParDo(countDoFn())
                    | 'CreateWindows' >> beam.WindowInto(window.FixedWindows(15, 0))
                    | 'GroupByKey' >> beam.GroupByKey()
                    | 'TotalMovies' >> beam.Map(lambda x: {x[0]: len(x[1]), 'timestamp': time.time()})       
                    | 'MakeStrings' >> beam.Map(lambda x: json.dumps(x))
                    | 'EncodeOutput' >> beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes)
                )
        except:
            log_error(logger, "Error occurred while executing body of pipeline")
            raise

        try:
            output | 'PublishToTopic' >> WriteToPubSub(known_args.output_topic)
        except:
            log_error(logger, "Error occurred while writing pipeline output to Pub/Sub topic")
            raise


if __name__ == '__main__':
    run()
