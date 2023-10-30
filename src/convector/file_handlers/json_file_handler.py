import json
import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler


class JSONFileHandler(BaseFileHandler):
    def handle_file(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False, conversation=False) -> Generator[Dict[str, Any], None, None]:
        total_bytes = 0  # Counter to keep track of total bytes processed
        try:
            with open(self.file_path, 'r') as file:
                if random_selection:
                    selected_positions = self.random_selector(file, lines, bytes)

                for i, line in enumerate(file):
                    if random_selection and i not in selected_positions:
                        continue

                    if lines and i >= lines:
                        break  # Stop processing if num_lines is reached
                    
                    original_data = json.loads(line)
                    transformed_item = self.handle_data(original_data, input, output, instruction, add)
                    json_line = json.dumps(transformed_item, ensure_ascii=False)
                    line_bytes = len(json_line.encode('utf-8'))
                    
                    # Check if the bytes limit is reached, if a limit is set
                    if bytes and total_bytes + line_bytes > bytes:
                        break
                    
                    total_bytes += line_bytes  # Update the total bytes counter
                    yield transformed_item  # Yield the transformed item for writing
                    
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON on line {i+1}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while handling the JSON file: {e}")