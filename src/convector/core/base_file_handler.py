from typing import Generator, Dict, Any
from abc import ABC, abstractmethod
from typing import Generator, Dict, Any

from ..data_processors.data_processors import IDataProcessor, ConversationDataProcessor, CustomKeysDataProcessor, AutoDetectDataProcessor
from ..utils.random_selector import IRandomSelector, LineRandomSelector, ByteRandomSelector, ConversationRandomSelector

class BaseFileHandler(ABC):
    def __init__(self, file_path: str, conversation: bool = False):
        self.file_path = file_path
        self.conversation = conversation
        self.data_processor: IDataProcessor = None  # This will be set dynamically


    @abstractmethod
    def handle_file(self, *args, **kwargs) -> Generator[Dict[str, Any], None, None]:
        """Main method to handle the file. This will be implemented by each subclass."""
        pass

    
    def random_selector(self, *args, **kwargs) -> Any:
        """
        Method to randomly select data lines or bytes. This will be a utility function.
        """
        if self.conversation:
            self.random_selector_strategy = ConversationRandomSelector()
        elif kwargs.get('lines'):
            self.random_selector_strategy = LineRandomSelector()
        elif kwargs.get('bytes'):
            self.random_selector_strategy = ByteRandomSelector()
        else:
            self.random_selector_strategy = None  # Default, can also be a default strategy

        if self.random_selector_strategy:
            return self.random_selector_strategy.select(*args, **kwargs)

    
    def handle_data(self, data, input=None, output=None, instruction=None, add=None):
        """
        Method to handle transformation logic based on arguments.
        """
        if self.conversation:
            self.data_processor = ConversationDataProcessor()
        elif input and output:
            self.data_processor = CustomKeysDataProcessor()
        else:
            self.data_processor = AutoDetectDataProcessor()

        return self.data_processor.process(data, input=input, output=output, instruction=instruction, add=add)
