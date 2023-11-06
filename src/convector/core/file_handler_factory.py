from pathlib import Path

from ..file_handlers import (
    JSONLFileHandler, 
    JSONFileHandler, 
    CSVFileHandler, 
    ParquetFileHandler, 
    ZSTFileHandler,  
    # Add other file handlers as needed
)

from convector.core.base_file_handler import BaseFileHandler

class FileHandlerFactory:
    # Registry mapping file extensions to their handlers
    _handlers_registry = {
        'json': JSONFileHandler,
        'jsonl': JSONLFileHandler,
        'csv': CSVFileHandler,
        'parquet': ParquetFileHandler,
        'zst': ZSTFileHandler
        # Add new file types and their handlers here as needed
    }

    @staticmethod
    def register_file_handler(file_extension: str, handler_class):
        """
        Class method to register a new file handler for a specific file extension.

        Parameters:
            file_extension (str): The file extension to register the handler for.
            handler_class (BaseFileHandler): The handler class to associate with the file extension.
        """
        FileHandlerFactory._handlers_registry[file_extension] = handler_class

    @staticmethod
    def create_file_handler(file_path: str, config) -> BaseFileHandler:
        """
        Factory method to instantiate the appropriate FileHandler based on file type and configurations.
        """
        file_extension = Path(file_path).suffix[1:].lower()  # Using pathlib for extension extraction

        handler_class = FileHandlerFactory._handlers_registry.get(file_extension)
        if handler_class:
            return handler_class(file_path, config)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")
