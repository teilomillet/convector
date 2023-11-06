# csv_file_handler.py

import json
import csv
import logging
from typing import Generator, Dict, Any, Iterator
from convector.core.base_file_handler import BaseFileHandler

class CSVFileHandler(BaseFileHandler):
    def read_file(self) -> Iterator:
        with open(self.file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield row

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        total_bytes = 0
        try:
            for row in self.filter_lines(self.read_file()):
                transformed_item = self.transform_data(row)
                json_line = json.dumps(transformed_item, ensure_ascii=False)
                line_bytes = len(json_line.encode('utf-8'))

                if self.should_stop_processing(total_bytes, line_bytes):
                    break

                total_bytes += line_bytes
                yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the CSV file: {e}")
            raise
