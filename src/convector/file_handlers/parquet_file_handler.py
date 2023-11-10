# parquet_file_handler.py

import logging
import json
import pandas as pd
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
        Transforms a row of Parquet file into the desired format. In this case,
        the data is already in a dictionary format, so it can be returned directly.
        """
        return original_data

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a Parquet file according to the active profile settings and yields 
        transformed data objects.
        """
        logging.debug(f"Handling Parquet file with profile: {self.profile}")
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the Parquet file: {e}")
            raise
