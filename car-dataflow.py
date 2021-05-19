import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from pipeline_functions import list_blobs_with_prefix, CloudFunctionFn, FirestoreWriteDoFn

PROJECT = "car-dataflow"
BUCKET = "car-dataflow"
FUNCTION_BASE = "https://europe-west3-car-dataflow.cloudfunctions.net"


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        lines = p | 'Read' >> beam.Create(list_blobs_with_prefix(BUCKET, 'images/'))

        (
                lines
                | 'Vision' >> (beam.ParDo(CloudFunctionFn(FUNCTION_BASE, 'car_vision', 'image_uri'), PROJECT))
                | 'Scrape' >> (beam.ParDo(CloudFunctionFn(FUNCTION_BASE, 'car_scraper', 'car_type')))
                | 'SaveToFireStore' >> (beam.ParDo(FirestoreWriteDoFn(PROJECT)))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
