# csv_file_handler.py

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
        Transforms a row from the CSV file and processes it using handle_data.
        """
        # Process data using handle_data from BaseFileHandler
        processed_data = super().handle_data(original_data)
        # Apply filters and schema here if needed before returning
        return processed_data

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
