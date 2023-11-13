import json
import logging
from typing import Generator, Dict, Any
from convector.core.base_file_handler import BaseFileHandler

class JSONFileHandler(BaseFileHandler):
    """
    JSONFileHandler is responsible for reading, processing, and yielding transformed 
    JSON objects from a .json file. It extends BaseFileHandler and utilizes the configuration 
    provided by ConvectorConfig.
    """

    def read_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Generator that reads a JSON file and yields its content.
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                file_content = file.read()

                # Parsing the JSON content
                data = json.loads(file_content)

                # Handling both list and dictionary types of JSON
                if isinstance(data, list):
                    # Iterate and yield each item if data is a list
                    for item in data:
                        yield item
                elif isinstance(data, dict):
                    # Yield the entire dictionary if data is a single dict
                    yield data
                else:
                    logging.error(f"Invalid JSON format in file {self.file_path}")
                    # Optionally, raise an exception or handle this case as needed
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding error in file {self.file_path}: {e}")
            raise
        except Exception as e:
            logging.error(f"Failed to read the JSON file at {self.file_path}: {e}")
            raise

    def transform_data(self, original_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforms JSON data into the desired format and then processes it using handle_data.
        """
        # Process data using handle_data from BaseFileHandler
        processed_data = super().handle_data(original_data)
        # Apply filters and schema here if needed before returning
        return processed_data

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Processes a JSON file according to the active profile settings and yields 
        transformed data objects.
        """
        try:
            for transformed_item in self.read_file():
                if transformed_item is not None:
                    yield self.transform_data(transformed_item)
        except Exception as e:
            logging.error(f"An error occurred while handling the JSON file: {e}")
            raise
