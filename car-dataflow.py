import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from pipeline_functions import list_blobs_with_prefix, CloudFunctionFn, FirestoreWriteDoFn


class CarDataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--bucket', default='car-dataflow', help='Bucket of Car Images')
        parser.add_argument('--functionBase', default='https://europe-west3-car-dataflow.cloudfunctions.net',
                            help='Base URL of GCP Functions')

        parser.add_argument('--visionAPI', default={
            'function': 'car_vision',
            'param': 'image_uri'
        }, help='Function name and Query Parameter')

        parser.add_argument('--scraperAPI', default={
            'function': 'car_scraper',
            'param': 'car_type'
        }, help='Function name and Query Parameter')


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = CarDataflowOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        project = pipeline_options.get_all_options().get('project')
        lines = p | 'Read' >> beam.Create(list_blobs_with_prefix(pipeline_options.bucket, 'images/'))

        (
                lines
                | 'Vision' >> (beam.ParDo(CloudFunctionFn(pipeline_options.functionBase, pipeline_options.visionAPI)))
                | 'Scrape' >> (beam.ParDo(CloudFunctionFn(pipeline_options.functionBase, pipeline_options.scraperAPI)))
                | 'SaveToFireStore' >> (beam.ParDo(FirestoreWriteDoFn(project)))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
