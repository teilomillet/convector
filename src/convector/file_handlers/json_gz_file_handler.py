# json_gz_file_handler.py

import json
import logging
import gzip
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler

class JSONGZFileHandler(BaseFileHandler):
    """
    JSONGZFileHandler is responsible for reading, processing, and yielding transformed 
    JSON objects from a .json.gz file. It extends BaseFileHandler and utilizes the 
    configuration provided by ConvectorConfig.
    """

    def read_file(self) -> Generator[str, None, None]:
        """
        Generator that reads a gzipped JSON file line by line.
        """
        try:
            with gzip.open(self.file_path, 'rt', encoding='utf-8') as file:
                for line in file:
                    yield line
        except Exception as e:
            logging.error(f"Failed to read the gzipped JSON file at {self.file_path}: {e}")
            raise

    def transform_data(self, original_data: str) -> Dict[str, Any]:
        """
        Transforms a line of gzipped JSON file into the desired format and then processes it 
        using handle_data.
        """
        # Decode JSON line
        decoded_data = json.loads(original_data)
        # Process data using handle_data from BaseFileHandler
        processed_data = super().handle_data(decoded_data)
        # Apply filters and schema here if needed before returning
        return processed_data

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a gzipped JSON file according to the active profile settings and yields 
        transformed JSON objects.
        """
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the gzipped JSON file: {e}")
            raise
