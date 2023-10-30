import json
import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler


class JSONLFileHandler(BaseFileHandler):
    def handle_file(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False, conversation=False) -> Generator[Dict[str, Any], None, None]:
        total_bytes = 0  # Counter to keep track of total bytes processed
        try:
            with open(self.file_path, 'r') as file:
                if random_selection:
                    selected_positions = self.random_selector(file, lines=lines, bytes=bytes, conversation=conversation)
                    file.seek(0)

                for i, line in enumerate(file):
                    if random_selection and i not in selected_positions:
                        continue  # Skip lines not in the selected positions

                    if lines and i >= lines:
                        break  # Stop processing if num_lines is reached

                    original_data = json.loads(line)
                    transformed_item = self.handle_data(original_data, input=input, output=output, instruction=instruction, add=add)
                    
                    for item in transformed_item:
                        json_line = json.dumps(item, ensure_ascii=False)
                        line_bytes = len(json_line.encode('utf-8'))

                        # Check if the bytes limit is reached, if a limit is set
                        if bytes and total_bytes + line_bytes > bytes:
                            break

                        total_bytes += line_bytes  # Update the total bytes counter
                        yield item  # Yield the transformed item for writing

        except Exception as e:
            logging.error(f"An error occurred while handling the JSONL file: {e}")

