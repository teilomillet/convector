# txt_file_handler.py

import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler

class TXTFileHandler(BaseFileHandler):
    """
    TXTFileHandler is responsible for reading, processing, and yielding transformed 
    lines from a .txt file. It extends BaseFileHandler and utilizes the configuration 
    provided by ConvectorConfig.
    """

    def read_file(self) -> Generator[str, None, None]:
        """
        Generator that reads a TXT file line by line.
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    yield line.strip()  # Stripping to remove any leading/trailing whitespace
        except Exception as e:
            logging.error(f"Failed to read the TXT file at {self.file_path}: {e}")
            raise

    def transform_data(self, original_data: str):
        """
        Transforms a line of TXT file into the desired format and then processes it using handle_data.
        """
        # Here, the transformation logic depends on how you want to process text data.
        # For example, if the text data is JSON-like, you can use json.loads.
        # Otherwise, implement the desired transformation for your text data format.
        # Example: processed_data = json.loads(original_data) if your text data is JSON-like.
        processed_data = super().handle_data(original_data)
        # Apply filters and schema here if needed before returning
        return processed_data

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a TXT file according to the active profile settings and yields 
        transformed data objects.
        """
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the TXT file: {e}")
            raise
