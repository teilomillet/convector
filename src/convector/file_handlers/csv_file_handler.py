# csv_file_handler.py

import csv
import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler

class CSVFileHandler(BaseFileHandler):
    """
    CSVFileHandler is responsible for reading, processing, and yielding transformed 
    rows from a .csv file. It extends BaseFileHandler and utilizes the configuration 
    provided by ConvectorConfig.
    """

    def read_file(self) -> Generator:
        """
        Generator that reads a CSV file row by row.
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    yield row
        except Exception as e:
            logging.error(f"Failed to read the CSV file at {self.file_path}: {e}")
            raise

    def transform_data(self, original_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforms a row of CSV file into the desired format and then processes it using handle_data.
        """
        # Process data using handle_data from BaseFileHandler
        processed_data = super().handle_data(original_data)
        # Apply filters and schema here if needed before returning
        return processed_data

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a CSV file according to the active profile settings and yields 
        transformed data objects.
        """
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the CSV file: {e}")
            raise
