# base_file_handler.py

import json
from abc import ABC, abstractmethod
from typing import Generator, Dict, Any, Iterator
import logging

from ..data_processors.data_processors import IDataProcessor, ConversationDataProcessor, CustomKeysDataProcessor, AutoDetectDataProcessor
from ..utils.random_selector import IRandomSelector, LineRandomSelector, ByteRandomSelector, ConversationRandomSelector
from convector.core.config import ConvectorConfig

logging.basicConfig(level=logging.DEBUG)

class BaseFileHandler(ABC):
    def __init__(self, file_path: str, config: ConvectorConfig):
        self.file_path = file_path
        self.config = config  # Keep the reference to the ConvectorConfig instance
        self.active_profile = config.get_active_profile()  # This is now a Profile instance
        self.is_conversation = self.active_profile.is_conversation
        self.input = self.active_profile.input
        self.output = self.active_profile.output
        self.instruction = self.active_profile.instruction
        self.add_cols = self.active_profile.additional_fields  # Corrected attribute name
        self.lines = self.active_profile.lines
        self.bytes = self.active_profile.bytes
        self.random_selection = self.active_profile.random_selection
        self.data_processor: IDataProcessor = None  # This will be set dynamically

        logging.debug(f"BaseFileHandler initialized with is_conversation: {self.is_conversation}")

    def filter_lines(self, lines: Iterator) -> Iterator:
        """
        Filters lines based on random selection or line limits.
        Subclasses can override this for custom filtering logic.
        """
        if self.random_selection:
            selected_positions = self.random_selector(
                lines,
                lines=self.lines,
                bytes=self.bytes,
                conversation=self.is_conversation
            )
            for i, line in enumerate(lines):
                if i in selected_positions:
                    yield line
        else:
            for i, line in enumerate(lines):
                if self.lines is not None and i >= self.lines:
                    break
                yield line

    @abstractmethod
    def read_file(self) -> Iterator:
        """
        Reads a file and yields its contents line by line.
        Must be implemented by subclasses.
        """
        pass

    def transform_data(self, original_data):
        """
        Default implementation of data transformation that simply returns the data unchanged.
        Subclasses can override this to implement custom transformation logic.
        """
        return self.handle_data(original_data)

    def should_stop_processing(self, total_bytes: int, line_bytes: int) -> bool:
        """
        Determines whether processing should stop based on byte limit.
        """
        if self.bytes is not None and (total_bytes + line_bytes) > self.bytes:
            return True
        return False

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Template method that defines the skeleton of the file processing algorithm.
        """
        total_bytes = 0
        try:
            filtered_lines = self.filter_lines(self.read_file())
            for line in filtered_lines:
                transformed_item = self.transform_data(line)
                json_line = json.dumps(transformed_item, ensure_ascii=False)
                line_bytes = len(json_line.encode('utf-8'))

                if self.should_stop_processing(total_bytes, line_bytes):
                    break

                total_bytes += line_bytes
                yield transformed_item
        except Exception as e:
            logging.error(f"An error occurred while handling the file: {e}")
            raise
    
    def random_selector(self, *args, **kwargs) -> Any:
        """
        Method to randomly select data lines or bytes. This will be a utility function.
        """
        if self.is_conversation:
            self.random_selector_strategy = ConversationRandomSelector()
        elif kwargs.get('lines'):
            self.random_selector_strategy = LineRandomSelector()
        elif kwargs.get('bytes'):
            self.random_selector_strategy = ByteRandomSelector()
        else:
            self.random_selector_strategy = None  # Default, can also be a default strategy

        if self.random_selector_strategy:
            return self.random_selector_strategy.select(*args, **kwargs)

    
    def handle_data(self, data):
        """
        Method to handle transformation logic based on configuration.
        """
        input_key = self.input
        output_key = self.output
        instruction_key = self.instruction
        add_keys = self.add_cols

        if self.is_conversation:
            self.data_processor = ConversationDataProcessor()
            # logging.debug("Using ConversationDataProcessor")
        elif input_key and output_key:
            self.data_processor = CustomKeysDataProcessor()
            # logging.debug("Using CustomKeysDataProcessor")
        else:
            self.data_processor = AutoDetectDataProcessor()
            # logging.debug("Using AutoDetectDataProcessor")

        return self.data_processor.process(data, input=input_key, output=output_key, instruction=instruction_key, add=add_keys)

