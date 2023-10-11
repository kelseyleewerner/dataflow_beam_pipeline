import apache_beam as beam
from apache_beam.dataframe.io import read_csv
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import argparse
import codecs
import csv



# import pprint
# pp = pprint.PrettyPrinter(indent=4)



# Code for reading in a CSV from a Cloud Storage bucket as text and return an iterable that can be consumed by 
#   a Beam PTransform was copied from the following sources:
# https://stackoverflow.com/questions/68215269/how-to-handle-newlines-when-loading-a-csv-into-apache-beam
# https://stackoverflow.com/questions/58500594/open-file-in-beam-io-filebasedsource-issue-with-python-3
# https://stackoverflow.com/questions/29383475/how-to-create-dict-using-csv-file-with-first-row-as-keys
def read_csv_file(file_path):
  with beam.io.filesystems.FileSystems.open(file_path, mime_type='text/plain') as infile:
    for row in csv.DictReader(codecs.iterdecode(infile, "utf-8")):
        yield row

def run(argv=None, save_main_session=True):
    # Code for parsing command line arguments was copied from https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      help='Input file to process.')
    parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        rows = p | 'ReadCSV' >> beam.Create(read_csv_file(known_args.input))
        
        
        rows | 'Write' >> WriteToText(known_args.output)



if __name__ == '__main__':
    run()