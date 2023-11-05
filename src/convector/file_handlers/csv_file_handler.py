import json
import logging
import csv
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler


class CSVFileHandler(BaseFileHandler):
    def handle_file(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False, conversation=False) -> Generator[Dict[str, Any], None, None]:
        total_bytes = 0  # Counter to keep track of total bytes processed
        
        try:
            with open(self.file_path, newline='') as csvfile:
                if random_selection: 
                    selected_positions = self.random_selector(csvfile, lines, bytes)

                reader = csv.DictReader(csvfile)
                
                for i, row in enumerate(reader):
                    if random_selection and i not in selected_positions:
                        continue
                    if lines and i >= lines:
                        break  # Stop processing if num_lines is reached
                    
                    transformed_item = self.handle_data(row, input, output, instruction, add)
                    json_line = json.dumps(transformed_item, ensure_ascii=False)
                    line_bytes = len(json_line.encode('utf-8'))
                    
                    # Check if the bytes limit is reached, if a limit is set
                    if bytes and total_bytes + line_bytes > bytes:
                        break
                    
                    total_bytes += line_bytes  # Update the total bytes counter
                    yield transformed_item  # Yield the transformed item for writing
                    
        except Exception as e:
            logging.error(f"An error occurred while handling the CSV file: {e}")
