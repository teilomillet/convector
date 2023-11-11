# txt_file_handler.py

import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler

class TXTFileHandler(BaseFileHandler):
    """
    TXTFileHandler is responsible for reading, processing, and yielding transformed
    text lines from a .txt file. It extends BaseFileHandler and utilizes the configuration
    provided by ConvectorConfig.
    """

    def read_file(self):
        """Generator that reads a TXT file line by line."""
        with open(self.file_path, 'r', encoding='utf-8') as file:
            for line in file:
                yield line

    def transform_data(self, original_data):
        """
        Transforms a line of text file into the desired format and then processes it using handle_data.
        """
        # Strip leading/trailing spaces as an example transformation
        processed_data = original_data.strip()
        # Process the data using handle_data from BaseFileHandler
        return super().handle_data(processed_data)

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a TXT file according to the active profile settings and yields 
        transformed text lines.
        """
        logging.debug(f"Handling TXT file with profile: {self.profile}")
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the TXT file: {e}")
            raise
