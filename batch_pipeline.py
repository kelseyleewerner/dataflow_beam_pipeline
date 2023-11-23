import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from datetime import datetime
from google.cloud import logging
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


def convert_str_to_date(date_value):
    if '/' in date_value:
        date_format = '%m/%d/%Y'
    else:
        date_format = '%m-%d-%Y'

    return datetime.strptime(date_value, date_format)


class FilterFlightDataDoFn(beam.DoFn):
    def process(self, element, convert_date):        
        filtered_element = {
            'age': int(element['Age']),
            'departure_date': convert_date(element['Departure Date']),
            'arrival_airport': element['Arrival Airport'],
            'flight_status': element['Flight Status']
        }
        
        return([filtered_element])


class FlightsByDateDoFn(beam.DoFn):
    def process(self, element, start_date, end_date):
        if element['departure_date'] >= start_date and element['departure_date'] <= end_date:
            return([element])
        else:
            return([])


class CombineAgeAndArrAirportFn(beam.CombineFn):
    def create_accumulator(self):
        return {
            'total_age': 0,
            'age_count': 0,
            'airport_codes': {}
        }

    def add_input(self, accumulator, input):
        accumulator['total_age'] += input['age']
        accumulator['age_count'] += 1
        arrival_airport = input['arrival_airport']
        
        if arrival_airport not in accumulator['airport_codes']:
            accumulator['airport_codes'][arrival_airport] = 0
        accumulator['airport_codes'][arrival_airport] += 1

        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {
            'total_age': 0,
            'age_count': 0,
            'airport_codes': {}
        }

        for accum in accumulators:
            merged['total_age'] += accum['total_age']
            merged['age_count'] += accum['age_count']
            
            for arrival_airport, count in accum['airport_codes'].items():
                if arrival_airport not in merged['airport_codes']:
                    merged['airport_codes'][arrival_airport] = 0
                merged['airport_codes'][arrival_airport] += count

        return merged

    def extract_output(self, accumulator):
        results = {
            'average_age': accumulator['total_age'] / accumulator['age_count'],
            'airport_codes': accumulator['airport_codes']
        }

        return results


class CombineForGreatestArrAirportFn(beam.CombineFn):
    def create_accumulator(self):
        return {
            'average_age': 0,
            'greatest_airport_count': 0,
            'airport_counts': {}
        }      

    def add_input(self, accumulator, input):
        accumulator['average_age'] = input['average_age']

        for arrival_airport, count in input['airport_codes'].items():
            # there is no airport with a code of '0', so any entries with this arrival airport code aren't included in the final count
            if arrival_airport == '0':
                continue

            if count > accumulator['greatest_airport_count']:
                accumulator['greatest_airport_count'] = count

            if count not in accumulator['airport_counts']:
                accumulator['airport_counts'][count] = []
            accumulator['airport_counts'][count].append(arrival_airport)
        
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {
            'average_age': 0,
            'greatest_airport_count': 0,
            'airport_counts': {}
        }

        # there should only be one accumulator but still writing this portion as if there could be multiple accumulators
        for accum in accumulators:
            merged['average_age'] = accum['average_age']
            
            if accum['greatest_airport_count'] > merged['greatest_airport_count']:
                merged['greatest_airport_count'] = accum['greatest_airport_count']
            
            for count in accum['airport_counts'].keys():
                if count not in merged['airport_counts']:
                    merged['airport_counts'][count] = []
                merged['airport_counts'][count] += accum['airport_counts'][count]

        return merged

    def extract_output(self, accumulator):
        results = {
            'average_age': accumulator['average_age'],
            'most_common_airport_count': accumulator['greatest_airport_count'],
            'most_common_airports': accumulator['airport_counts'][accumulator['greatest_airport_count']]
        }

        return results


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
      dest='start',
      default='09/01/2022',
      help='Date range cannot start earlier than Jan 1, 2022 and must be in the format mm/dd/yyyy or mm-dd-yyyy.')
    parser.add_argument(
      '--end',
      dest='end',
      default='12/29/2022',
      help='Date range cannot end after Dec 30, 2022 and must be in the format mm/dd/yyyy or mm-dd-yyyy.')
    parser.add_argument(
      '--log-name',
      dest='log_name',
      required=True,
      help='Destination for log output.')      
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    start_date = convert_str_to_date(known_args.start)
    end_date = convert_str_to_date(known_args.end)

    logging_client = logging.Client()
    logger = logging_client.logger(known_args.log_name)
    logger.log_text("Starting airline dataset batch pipeline", severity="INFO")

    # Start moving data through beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        try:
            rows = p | 'ReadCSV' >> beam.Create(read_csv_file(known_args.input))
        except BaseException as err:
            logger.log_struct(
                {
                    "message": "Error occurred while reading pipeline input from file",
                    "error_type": F"{type(err)}",
                    "error_body": F"{err}"
                },
                severity="ERROR"
            )
            raise
        
        try:
            output = (
                rows 
                    # Map Pipeline Section   
                    | 'FilterFlightData' >> beam.ParDo(FilterFlightDataDoFn(), convert_str_to_date)
                    | 'FlightsByDate' >> beam.ParDo(FlightsByDateDoFn(), start_date, end_date)
                    # Shuffle Pipeline Section
                    | 'GroupByFlightStatus' >> beam.GroupBy(lambda item: item['flight_status'])
                    # Reduce Pipeline Section
                    | 'CombineAgeAndArrAirport' >> beam.CombineValues(CombineAgeAndArrAirportFn())
                    | 'CombineForGreatestArrAirport' >> beam.CombinePerKey(CombineForGreatestArrAirportFn())
                )
        except BaseException as err:
            logger.log_struct(
                {
                    "message": "Error occurred while executing body of pipeline",
                    "error_type": F"{type(err)}",
                    "error_body": F"{err}"
                },
                severity="ERROR"
            )
            raise

        try:
            output | 'Write' >> WriteToText(known_args.output)
        except BaseException as err:
            logger.log_struct(
                {
                    "message": "Error occurred while writing pipeline output to file",
                    "error_type": F"{type(err)}",
                    "error_body": F"{err}"
                },
                severity="ERROR"
            )
            raise


    logger.log_text("Ending airline dataset batch pipeline", severity="INFO")


if __name__ == '__main__':
    run()
    