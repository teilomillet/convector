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
        Transforms a line of JSONL file into the desired format and then processes it using handle_data.
        """
        # Decode JSON line if necessary
        decoded_data = json.loads(original_data) if isinstance(original_data, str) else original_data
        # Process data
        processed_data = super().handle_data(decoded_data)
        # Apply filters and schema here if needed before returning
        return processed_data


    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a JSONL file according to the active profile settings and yields 
        transformed JSON objects.
        """
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the JSONL file: {e}")
            raise
