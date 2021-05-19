from abc import ABC

import apache_beam as beam
import requests
import json
from google.cloud import firestore
from google.cloud import storage


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    result = []

    print("Blobs:")
    for blob in blobs:
        if str(blob.name).endswith(".jpg") or str(blob.name).endswith(".jpeg"):
            print(blob.name)
            result.append(f'gs://{bucket_name}/{blob.name}')

    return result


class CloudFunctionFn(beam.DoFn, ABC):
    def __init__(self, function_base, function, param_name):
        self.function_base = function_base
        self.function = function
        self.param_name = param_name

    def process(self, element, *args, **kwargs):
        print(f'args: {args}')
        print(f'element: {element}')
        params = {self.param_name: element}
        url = f'{self.function_base}/{self.function}'
        result = requests.post(url, params=params)
        print(f'result: {result.text}')
        return [result.text]


class FirestoreWriteDoFn(beam.DoFn, ABC):
    doc_ref = None

    def setup(self):
        super().setup()
        db = firestore.Client(project=self.project)
        self.doc_ref = db.collection('cars')

    def __init__(self, project):
        self.project = project

    def process(self, element, *args, **kwargs):
        print(f'element: {element}')
        data = json.loads(element)

        car_document = self.doc_ref.document('audi')

        print(car_document.set({
            "prices": data['prices'],
            "reviews": data['reviews']
        }))
