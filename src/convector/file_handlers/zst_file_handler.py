import zstandard as zstd
import json
import logging
import io
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler

class ZSTFileHandler(BaseFileHandler):
    """
    ZSTFileHandler is responsible for reading, processing, and yielding transformed 
    lines from a .zst compressed file. It extends BaseFileHandler and utilizes the 
    configuration provided by ConvectorConfig.
    """

    def read_file(self):
        """Generator that reads a ZST file line by line."""
        try:
            with open(self.file_path, 'rb') as fh:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(fh) as reader:
                    text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                    for line in text_stream:
                        yield line
        except Exception as e:
            logging.error(f"Failed to read the ZST file at {self.file_path}: {e}")
            raise

    def transform_data(self, original_data):
        """
        Transforms a line of ZST file into the desired format and then processes it using handle_data.
        """
        # Decode JSON line if necessary
        decoded_data = json.loads(original_data) if isinstance(original_data, str) else original_data
        # Process data
        processed_data = super().handle_data(decoded_data)
        # Apply filters and schema here if needed before returning
        return processed_data

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a ZST file according to the active profile settings and yields 
        transformed JSON objects.
        """
        try:
            for transformed_item in super().handle_file():
                if transformed_item is not None:
                    yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the ZST file: {e}")
            raise
