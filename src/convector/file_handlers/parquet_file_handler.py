import logging
import json
import pandas as pd
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler


class ParquetFileHandler(BaseFileHandler):
    def handle_file(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False, conversation=False) -> Generator[Dict[str, Any], None, None]:
        total_bytes = 0
        try:
            df = pd.read_parquet(self.file_path)  # Load the entire Parquet file
            total_rows = len(df)

            if random_selection:
                # Initialize random selector and get selected positions
                selected_rows = self.random_selector(None, lines=lines, bytes=bytes, total_positions=total_rows)
                df = df.iloc[selected_rows]

            # If a line limit is specified, truncate the DataFrame
            if lines:
                df = df.head(lines)

            for i, row in df.iterrows():
                row_dict = row.to_dict()
                transformed_item = self.handle_data(row_dict, input, output, instruction, add)
                json_line = json.dumps(transformed_item, ensure_ascii=False)
                line_bytes = len(json_line.encode('utf-8'))

                # Check if the bytes limit is reached, if a limit is set
                if bytes and total_bytes + line_bytes > bytes:
                    break

                total_bytes += line_bytes  # Update the total bytes counter
                yield transformed_item  # Yield the transformed item for writing

        except Exception as e:
            logging.error(f"An error occurred while handling the Parquet file: {e}")

