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
        Transforms a line of ZST file into the desired format.
        """
        try:
            return json.loads(original_data)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON line: {original_data}. Error: {e}")
            raise

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
