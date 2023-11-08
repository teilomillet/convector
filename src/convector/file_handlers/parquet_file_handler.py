# parquet_file_handler.py

import logging
import json
import pandas as pd
from typing import Generator, Dict, Any, Iterator
from convector.core.base_file_handler import BaseFileHandler


class ParquetFileHandler(BaseFileHandler):
    def read_file(self) -> Iterator:
        try:
            df = pd.read_parquet(self.file_path)
        except Exception as e:
            logging.error(f"Failed to read the Parquet file at {self.file_path}: {e}")
            raise
        else:
            for _, row in df.iterrows():
                yield row.to_dict()

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        logging.debug(f"Type of self.profile: {type(self.profile)}")
        total_bytes = 0
        try:
            rows = self.read_file()
            for row_dict in self.filter_lines(rows):
                if not row_dict:
                    logging.debug("Row filtered out or empty.")
                    continue
                
                transformed_item = self.transform_data(row_dict)
                if not transformed_item:
                    logging.debug("Data transformation resulted in no output.")
                    continue

                json_line = json.dumps(transformed_item, ensure_ascii=False)
                line_bytes = len(json_line.encode('utf-8'))

                if self.should_stop_processing(total_bytes, line_bytes):
                    logging.debug("Stopping condition met, ending processing.")
                    break

                total_bytes += line_bytes
                if line_bytes > 0:
                    yield transformed_item
                else:
                    logging.debug("Transformed item resulted in zero bytes.")
                    
        except Exception as e:
            logging.error(f"An error occurred while handling the Parquet file: {e}")
            raise
