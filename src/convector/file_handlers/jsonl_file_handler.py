# jsonl_file_handler.py

import json
import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler

class JSONLFileHandler(BaseFileHandler):
    """
    JSONLFileHandler is responsible for reading, processing, and yielding transformed 
    JSON lines from a .jsonl file. It extends BaseFileHandler and utilizes the configuration 
    provided by ConvectorConfig.
    """

    def read_file(self):
        """Generator that reads a JSONL file line by line."""
        with open(self.file_path, 'r', encoding='utf-8') as file:
            for line in file:
                yield line

    def transform_data(self, original_data):
        """
        Transforms a line of JSONL file into the desired format.
        """
        try:
            return json.loads(original_data)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON line: {original_data}. Error: {e}")
            raise

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a JSONL file according to the active profile settings and yields 
        transformed JSON objects.
        """
        logging.debug(f"Handling JSONL file with profile: {self.profile}")
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the JSONL file: {e}")
            raise
