import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe, to_pcollection
from apache_beam.dataframe.io import read_csv
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import argparse


import csv
import codecs

from apache_beam.dataframe.convert import to_pcollection

import pprint
pp = pprint.PrettyPrinter(indent=4)



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


        
        # rows = p | 'ReadCSV' >> read_csv(known_args.input)

        # rows_as_pcoll = to_pcollection(rows)

        # pp.pprint(type(rows))

        # rows | 'Type' >> beam.Map(lambda x: type(x))

        # rows = p | 'ReadCSV' >> ReadFromText(known_args.input)
        rows | 'Write' >> WriteToText(known_args.output)



if __name__ == '__main__':
    run()