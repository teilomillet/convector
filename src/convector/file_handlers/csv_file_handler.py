# csv_file_handler.py

import json
import csv
import logging
from typing import Generator, Dict, Any, Iterator
from convector.core.base_file_handler import BaseFileHandler

class CSVFileHandler(BaseFileHandler):
    def read_file(self) -> Iterator:
        """
        Generator that reads a CSV file and yields each row as a dictionary.
        """
        with open(self.file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield row

    def transform_data(self, original_data):
        """
        Transforms a row from the CSV file. 
        Override this method if a different transformation is required.
        """
        # Default transformation is to simply return the data as is.
        # Custom transformation logic can be implemented here.
        return original_data

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a CSV file according to the active profile settings and yields 
        transformed rows.
        """
        logging.debug(f"Handling CSV file with profile: {self.profile}")
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the CSV file: {e}")
            raise
