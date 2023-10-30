from ..file_handlers import JSONLFileHandler, JSONFileHandler, CSVFileHandler, ParquetFileHandler, ZSTFileHandler  # Add other file handlers as needed
from convector.core.base_file_handler import BaseFileHandler

class FileHandlerFactory:
    @staticmethod
    def create_file_handler(file_path: str, is_conversation: bool) -> BaseFileHandler:
        """
        Factory method to instantiate the appropriate FileHandler based on file type and configurations.

        Parameters:
            file_path (str): The path to the file that needs to be processed.
            is_conversation (bool): Flag indicating whether the data is in a conversational format.

        Returns:
            BaseFileHandler: An instance of the appropriate FileHandler.
        """

        # Extract file extension to determine which file handler to use
        file_extension = file_path.split('.')[-1].lower()

        if file_extension == 'json':
            return JSONFileHandler(file_path, is_conversation)
        elif file_extension == 'jsonl':
            return JSONFileHandler(file_path, is_conversation)
        elif file_extension == 'csv':
            return CSVFileHandler(file_path, is_conversation)
        elif file_extension == 'parquet':
            return ParquetFileHandler(file_path, is_conversation)
        elif file_extension == 'zst':
            return ZSTFileHandler(file_path, is_conversation)
        # Add more conditions here for additional file types

        else:
            raise ValueError(f"Unsupported file type: {file_extension}")
