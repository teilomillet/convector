# json_gz_file_handler.py

import json
import gzip
import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler

class JSONGZFileHandler(BaseFileHandler):
    """
    JSONGZFileHandler is responsible for reading, processing, and yielding transformed 
    JSON lines from a .json.gz file. It extends BaseFileHandler and utilizes the configuration 
    provided by ConvectorConfig.
    """

    def read_file(self):
        """Generator that reads a JSON Gzip file line by line."""
        with gzip.open(self.file_path, 'rt', encoding='utf-8') as file:
            for line in file:
                yield line

    def transform_data(self, original_data):
        """
        Transforms a line of JSON data from the Gzip file into the desired format.
        """
        try:
            return json.loads(original_data)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON line: {original_data}. Error: {e}")
            raise

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a JSON Gzip file according to the active profile settings and yields 
        transformed JSON objects.
        """
        logging.debug(f"Handling JSON Gzip file with profile: {self.profile}")
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the JSON Gzip file: {e}")
            raise
