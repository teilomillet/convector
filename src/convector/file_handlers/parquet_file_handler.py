import logging
import json
import pandas as pd
from typing import Generator, Dict, Any, Iterator
from convector.core.base_file_handler import BaseFileHandler


class ParquetFileHandler(BaseFileHandler):
    def read_file(self) -> Iterator:
        df = pd.read_parquet(self.file_path)
        for _, row in df.iterrows():
            yield row.to_dict()

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        total_bytes = 0
        try:
            for row_dict in self.filter_lines(self.read_file()):
                transformed_item = self.transform_data(row_dict)
                json_line = json.dumps(transformed_item, ensure_ascii=False)
                line_bytes = len(json_line.encode('utf-8'))

                if self.should_stop_processing(total_bytes, line_bytes):
                    break

                total_bytes += line_bytes
                yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the Parquet file: {e}")
            raise

