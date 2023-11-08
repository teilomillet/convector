# base_file_handler.py

import json
from abc import ABC, abstractmethod
from typing import Generator, Dict, Any, Iterator
import logging
import re

from ..data_processors.data_processors import IDataProcessor, ConversationDataProcessor, CustomKeysDataProcessor, AutoDetectDataProcessor
from ..utils.random_selector import IRandomSelector, LineRandomSelector, ByteRandomSelector, ConversationRandomSelector
from convector.core.config import ConvectorConfig, Profile

logging.basicConfig(level=logging.DEBUG)

class BaseFileHandler(ABC):
    def __init__(self, file_path: str, profile: Profile):
        self.file_path = file_path
        self.profile = profile  # Keep the reference to the ConvectorConfig instance
        self.is_conversation = profile.is_conversation
        self.input = profile.input
        self.output = profile.output
        self.instruction = profile.instruction
        self.labels = profile.labels  # Corrected attribute name
        self.lines = profile.lines
        self.bytes = profile.bytes
        self.random_selection = profile.random
        self.data_processor: IDataProcessor = None  # This will be set dynamically

        logging.debug(f"BaseFileHandler initialized with is_conversation: {profile.is_conversation}")

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

    def is_conversational_data(self, data):
        # Assuming 'data' key contains the messages
        messages = data.get('data', [])
        if not isinstance(messages, list) or len(messages) % 2 != 0:
            return False

        # Perform a sample examination to confirm conversational structure
        return self.is_conversational_structure(messages[:10])  # Check first 5 pairs

    def is_conversational_structure(self, sample_data):
        # Placeholder for logic to check if the sample_data follows the conversational pattern
        for i in range(0, len(sample_data), 2):
            if not self.is_valid_message_pair(sample_data[i], sample_data[i+1]):
                return False
        return True

    def is_valid_message_pair(self, user_message, assistant_message):
        # Placeholder for validation logic using keyword and pronoun analysis
        if not user_message or not assistant_message:
            return False

        user_keywords = re.findall(r"\b(what|how|can you)\b", user_message, re.IGNORECASE)
        assistant_keywords = re.findall(r"\b(did|will|understand)\b", assistant_message, re.IGNORECASE)
        return bool(user_keywords) and bool(assistant_keywords)
    
    def handle_data(self, data):
        """
        Method to handle transformation logic based on configuration.
        """
        input_key = self.input
        output_key = self.output
        instruction_key = self.instruction
        labels = self.labels

        if self.is_conversation: # or self.is_conversational_data(data):
            self.data_processor = ConversationDataProcessor()
            logging.debug("Using ConversationDataProcessor")
        elif input_key and output_key:
            self.data_processor = CustomKeysDataProcessor()
            logging.debug("Using CustomKeysDataProcessor")
        else:
            self.data_processor = AutoDetectDataProcessor()
            logging.debug("Using AutoDetectDataProcessor")

        return self.data_processor.process(data, input=input_key, output=output_key, instruction=instruction_key, labels=labels)

