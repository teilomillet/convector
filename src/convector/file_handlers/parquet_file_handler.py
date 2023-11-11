# parquet_file_handler.py

import pandas as pd
import logging
import json
from typing import Generator, Dict, Any, Iterator
from convector.core.base_file_handler import BaseFileHandler

class ParquetFileHandler(BaseFileHandler):
    def read_file(self) -> Iterator:
        """
        Generator that reads a Parquet file row by row.
        """
        try:
            df = pd.read_parquet(self.file_path)
        except Exception as e:
            logging.error(f"Failed to read the Parquet file at {self.file_path}: {e}")
            raise
        else:
            for _, row in df.iterrows():
                yield row.to_dict()

    def transform_data(self, original_data):
        """
        Transforms a row of Parquet file into the desired format and then processes it using handle_data.
        """
        if isinstance(original_data, str):
            original_data = json.loads(original_data)
        # Process data using handle_data from BaseFileHandler
        processed_data = super().handle_data(original_data)
        # Apply filters and schema here if needed before returning
        return processed_data

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a Parquet file according to the active profile settings and yields 
        transformed data objects.
        """
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the Parquet file: {e}")
            raise
