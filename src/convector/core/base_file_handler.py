# base_file_handler.py

import json
from abc import ABC, abstractmethod
from typing import Generator, Dict, Any, Iterator
import logging

from ..data_processors.data_processors import IDataProcessor, ConversationDataProcessor, CustomKeysDataProcessor, AutoDetectDataProcessor
from ..utils.random_selector import LineRandomSelector, ByteRandomSelector, ConversationRandomSelector
from convector.core.profile import Profile
from convector.utils.label_filter import LabelFilter

logging.basicConfig(level=logging.DEBUG)

class BaseFileHandler(ABC):
    def __init__(self, file_path: str, profile: Profile):
        self.initialize_handler(file_path, profile)

    def initialize_handler(self, file_path: str, profile: Profile):
        """
        Initializes the file handler with the given file path and profile.
        """
        self.file_path = file_path
        self.profile = profile
        self.is_conversation = profile.is_conversation
        self.input = profile.input
        self.output = profile.output
        self.instruction = profile.instruction
        self.filters = profile.filters
        self.lines = profile.lines
        self.bytes = profile.bytes
        self.random_selection = profile.random
        self.data_processor: IDataProcessor = None
       

    def filter_lines(self, lines: Iterator) -> Iterator:
        """
        Filters lines based on random selection or line limits.
        """
        selected_positions = self.determine_selected_positions(lines)
        for i, line in enumerate(lines):
            if self.is_selected_line(i, selected_positions):
                yield line

    def determine_selected_positions(self, lines: Iterator) -> Any:
        """
        Determines which lines should be selected based on random selection or line limits.
        """
        if self.random_selection:
            return self.random_selector(
                lines,
                lines=self.lines,
                bytes=self.bytes,
                conversation=self.is_conversation
            )
        return None

    def is_selected_line(self, index: int, selected_positions: Any) -> bool:
        """
        Determines if a line at a given index is selected for processing.
        """
        if self.lines is not None and index >= self.lines:
            return False
        return selected_positions is None or index in selected_positions

    @abstractmethod
    def read_file(self) -> Iterator:
        """
        Reads a file and yields its contents line by line.
        """
        pass

    def transform_data(self, original_data):
        """
        Transforms the data using the specified data processor.
        """
        return self.handle_data(original_data)

    def should_stop_processing(self, total_bytes: int, line_bytes: int) -> bool:
        """
        Determines whether processing should stop based on byte limit.
        """
        return self.bytes is not None and (total_bytes + line_bytes) > self.bytes

    def handle_file(self) -> Generator[Dict[str, Any], None, None]:
        """
        Handles the file processing workflow.
        """
        try:
            return self.process_lines()
        except Exception as e:
            logging.error(f"An error occurred while handling the file: {e}")
            raise

    def process_lines(self) -> Generator[Dict[str, Any], None, None]:
        total_bytes = 0
        filtered_lines = self.filter_lines(self.read_file())
        label_filter = LabelFilter(self.filters)  # Initialize the LabelFilter with filters from profile

        for line in filtered_lines:
            processed_line = json.loads(line) if isinstance(line, str) else line  # Check if line is a string

            filtered_batch = label_filter.apply_filters([processed_line])
            for filtered_line in filtered_batch:
                yield self.process_single_line(json.dumps(filtered_line) if isinstance(filtered_line, dict) else filtered_line, total_bytes)


    def process_single_line(self, line: str, total_bytes: int) -> Dict[str, Any]:
        """
        Processes a single line of the file.
        """
        transformed_item = self.transform_data(line)
        json_line = json.dumps(transformed_item, ensure_ascii=False)
        line_bytes = len(json_line.encode('utf-8'))

        if self.should_stop_processing(total_bytes, line_bytes):
            return None

        total_bytes += line_bytes
        return transformed_item

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
            self.random_selector_strategy = None  

        if self.random_selector_strategy:
            return self.random_selector_strategy.select(*args, **kwargs)

    def handle_data(self, data):
        fields_to_include = [condition.field for condition in self.filters]
        # logging.debug(f"data before handle_data: {data}")
        # Process data that meets the criteria
        self.choose_data_processor()
        return self.data_processor.process(data,input=self.input, output=self.output, instruction=self.instruction, fields_to_include=fields_to_include)

    def choose_data_processor(self):
        """Chooses the appropriate data processor based on the data type and configuration."""
        if self.is_conversation:
            self.data_processor = ConversationDataProcessor()
        elif self.input and self.output:
            self.data_processor = CustomKeysDataProcessor()
        else:
            self.data_processor = AutoDetectDataProcessor()

