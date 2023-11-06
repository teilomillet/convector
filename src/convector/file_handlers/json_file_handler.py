import json
import logging
from typing import Generator, Dict, Any, Iterator
from convector.core.base_file_handler import BaseFileHandler


class JSONFileHandler(BaseFileHandler):
    def read_file(self) -> Iterator[str]:
        with open(self.file_path, 'r', encoding='utf-8') as file:
            for line in file:
                yield line

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        total_bytes = 0
        try:
            for line in self.filter_lines(self.read_file()):
                original_data = json.loads(line)
                transformed_item = self.transform_data(original_data)
                json_line = json.dumps(transformed_item, ensure_ascii=False)
                line_bytes = len(json_line.encode('utf-8'))

                if self.should_stop_processing(total_bytes, line_bytes):
                    break

                total_bytes += line_bytes
                yield transformed_item
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON on line: {e}")
        except Exception as e:
            logging.error(f"An error occurred while handling the JSON file: {e}")
            raise