# zst_file_handler.py

import logging
import zstandard as zstd
import io
import json
from typing import Generator, Dict, Any, Iterator
from convector.core.base_file_handler import BaseFileHandler

class ZSTFileHandler(BaseFileHandler):
    def read_file(self) -> Iterator[str]:
        """
        Generator that reads a ZST (Zstandard compressed) file line by line.
        """
        with open(self.file_path, 'rb') as fh:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(fh) as reader:
                text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                for line in text_stream:
                    yield line

    def transform_data(self, original_data):
        """
        Transforms a line of ZST file into the desired format and then processes it using handle_data.
        """
        # Decode JSON line if necessary
        decoded_data = json.loads(original_data) if isinstance(original_data, str) else original_data
        # Process data using handle_data from BaseFileHandler
        processed_data = super().handle_data(decoded_data)
        # Apply filters and schema here if needed before returning
        return processed_data

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a ZST file according to the active profile settings and yields 
        transformed JSON objects.
        """
        logging.debug(f"Handling ZST file with profile: {self.profile}")
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the ZST file: {e}")
            raise
