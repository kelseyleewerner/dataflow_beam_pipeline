import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import argparse
import codecs
import csv
from datetime import datetime



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

class FilterFlightDataDoFn(beam.DoFn):
    def process(self, element):
        filtered_element = {
            'passenger_id': element['Passenger ID'],
            'age': element['Age'],
            'departure_date': self.sanitize_date_field(element['Departure Date']),
            'arrival_airport': element['Arrival Airport'],
            'flight_status': element['Flight Status']
        }
        return([filtered_element])

    def sanitize_date_field(self, date_value):
        if '/' in date_value:
            date_format = '%m/%d/%Y'
        else:
            date_format = '%m-%d-%Y'

        try:
            filtered_date = datetime.strptime(date_value, date_format)
        except:
            return False

        return filtered_date


def run(argv=None, save_main_session=True):
    # Code for parsing command line arguments was copied from https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Input file to process.')
    parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
    # Date range defaults to 09/01/2022 - 12/29/2022 if not given as command line arguments
    # Validating the --start and --end command line arguments provided from the user is left as a future enhancement
    parser.add_argument(
      '--start',
      dest='start_date',
      default='',
      required=True,
      help='Date range cannot start earlier than Dec 31, 2021 and must be in the format mm/dd/yyyy or mm-dd-yyyy.')
    parser.add_argument(
      '--end',
      dest='end_date',
      required=True,
      help='Date range cannot end after Dec 29, 2022 and must be in the format mm/dd/yyyy or mm-dd-yyyy.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # Start moving data through beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        rows = p | 'ReadCSV' >> beam.Create(read_csv_file(known_args.input))
        output = (
            rows 
                | 'FilterFlightData' >> beam.ParDo(FilterFlightDataDoFn())
            )

        output | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    run()
    