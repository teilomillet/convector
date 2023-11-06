# jsonl_file_handler.py

import json
import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler
from convector.utils.random_selector import LineRandomSelector, ByteRandomSelector

class JSONLFileHandler(BaseFileHandler):
    """
    JSONLFileHandler is responsible for reading, processing, and yielding transformed 
    JSON lines from a .jsonl file. It extends BaseFileHandler and utilizes the configuration 
    provided by ConvectorConfig.
    """

    def read_file(self):
        """Generator that reads a JSONL file line by line."""
        with open(self.file_path, 'r', encoding='utf-8') as file:
            for line in file:
                yield line

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a JSONL file according to the active profile settings and yields 
        transformed JSON objects.
        """
        logging.debug(f"Type of self.config: {type(self.config)}")
        total_bytes = 0
        try:
            lines = self.read_file()
            for line in self.filter_lines(lines):
                original_data = json.loads(line)
                transformed_item = self.transform_data(original_data)

                json_line = json.dumps(transformed_item, ensure_ascii=False)
                line_bytes = len(json_line.encode('utf-8'))

                if self.active_profile.bytes and total_bytes + line_bytes > self.active_profile.bytes:
                    break

                total_bytes += line_bytes
                yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the JSONL file: {e}")
            raise

