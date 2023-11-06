import logging
import zstandard as zstd
import io
import json
from typing import Generator, Dict, Any, Iterator
from convector.core.base_file_handler import BaseFileHandler


class ZSTFileHandler(BaseFileHandler):
    def read_file(self) -> Iterator[str]:
        with open(self.file_path, 'rb') as fh:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(fh) as reader:
                text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                for line in text_stream:
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
        except Exception as e:
            logging.error(f"An error occurred while handling the ZST file: {e}")
            raise

