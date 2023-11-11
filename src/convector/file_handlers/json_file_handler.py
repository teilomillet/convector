# json_file_handler.py

import json
import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler

class JSONFileHandler(BaseFileHandler):
    def read_file(self) -> Generator[str, None, None]:
        """
        Reads a JSON file line by line.
        """
        with open(self.file_path, 'r', encoding='utf-8') as file:
            yield from file

    def transform_data(self, original_data):
        """
        Transforms a line of JSON data into the desired format and then processes it using handle_data.
        """
        try:
            # Decode JSON line if necessary
            decoded_data = json.loads(original_data) if isinstance(original_data, str) else original_data
            # Process data using handle_data from BaseFileHandler
            processed_data = super().handle_data(decoded_data)
            # Apply filters and schema here if needed before returning
            return processed_data
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON line: {original_data}. Error: {e}")
            raise

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a JSON file according to the active profile settings and yields 
        transformed JSON objects.
        """
        logging.debug(f"Handling JSON file with profile: {self.profile}")
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the JSON file: {e}")
            raise
