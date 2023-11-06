import time
import logging
from pathlib import Path
from tqdm import tqdm
from .file_handler_factory import FileHandlerFactory
from ..convector import Convector, UserInteraction
from .user_interaction import UserInteraction

# Configuration for retries
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # in seconds

class DirectoryProcessor:
    def __init__(self, directory_path: str, config: dict, is_conversation: bool, output_file=None, output_dir=None, output_schema=None, **kwargs):
        self.directory_path = Path(directory_path)
        self.config = config  # Save the config dictionary
        self.is_conversation = is_conversation
        self.output_file = output_file
        self.output_dir = output_dir
        self.output_schema = output_schema
        
        # Store the additional keyword arguments that may be passed to the Convector process method
        self.kwargs = kwargs
        self.processed_files = 0
        self.skipped_files = 0
        self.skipped_files_details = []



    def process_directory(self):
        """
        Main method to process all files in the directory recursively.
        """
        files = list(self.directory_path.rglob('*'))  # Define files here to use it in len(files)
        with tqdm(total=len(files), unit='file') as progress_bar:
            for file_path in files:
                if file_path.is_file():  # Make sure it's a file
                    attempts = 0
                    while attempts < RETRY_ATTEMPTS:
                        try:
                            # Create an instance of Convector for each file
                            convector = Convector(
                                config=self.config,  # The configuration dictionary
                                user_interaction=UserInteraction(),  # Instance for user interaction
                                file_path=str(file_path),  # Current file path
                            )
                            # Now call the process method from Convector
                            convector.process()
                            self.processed_files += 1
                            logging.info(f"Processed file: {file_path}")
                            break  # Success, break out of the retry loop
                        except ValueError as e:
                            self.handle_error(file_path, e)
                            break  # Non-retryable error
                        except IOError as e:
                            # Handle transient I/O errors by retrying
                            attempts += 1
                            if attempts < RETRY_ATTEMPTS:
                                logging.warning(f"Transient error on {file_path}: {e}. Retrying in {RETRY_DELAY} seconds...")
                                time.sleep(RETRY_DELAY)
                            else:
                                self.handle_error(file_path, f"Reached maximum retry attempts for: {e}")
                                break
                    # Update the progress bar after each file attempt
                    progress_bar.update(1)
                else:
                    # This else part is for skipping non-file paths (like directories)
                    progress_bar.set_postfix_str(f"Skipped: {file_path} (Not a file)")
                    progress_bar.update(1)

    def handle_error(self, file_path, error):
        self.skipped_files += 1
        self.skipped_files_details.append((file_path, str(error)))
        logging.warning(f"Skipping file: {file_path} - Reason: {error}")

    def print_summary(self):
        """
        Output a summary of the processing after completion.
        """
        logging.info(f"Processing completed. Total files processed: {self.processed_files}")
        if self.skipped_files > 0:
            logging.info(f"Files skipped: {self.skipped_files}. See details below.")
            for file_detail in self.skipped_files_details:
                logging.info(f"Skipped file: {file_detail[0]} - Reason: {file_detail[1]}")
        else:
            logging.info("No files were skipped.")
