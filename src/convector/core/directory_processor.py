import time
import logging
from pathlib import Path
from tqdm import tqdm
from .file_handler_factory import FileHandlerFactory
from ..convector import Convector
from .user_interaction import UserInteraction
from convector.core.profile import Profile

RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # in seconds

class DirectoryProcessor:
    def __init__(self, directory_path: str, profile: Profile, **kwargs):
        self.directory_path = Path(directory_path)
        self.profile = profile 
        self.is_conversation = profile.is_conversation
        self.output_file = profile.output_file
        self.output_dir = profile.output_dir
        self.output_schema = profile.output_schema
        
        self.kwargs = kwargs
        self.processed_files = 0
        self.skipped_files = 0
        self.skipped_files_details = []



    def process_directory(self):
        """
        Main method to process all files in the directory recursively.
        """
        files = list(self.directory_path.rglob('*'))  
        with tqdm(total=len(files), unit='file') as progress_bar:
            for file_path in files:
                if file_path.is_file(): 
                    attempts = 0
                    while attempts < RETRY_ATTEMPTS:
                        try:
                            convector = Convector(
                                profile=self.profile,  
                                user_interaction=UserInteraction(),  
                                file_path=str(file_path),  
                            )

                            convector.process()
                            self.processed_files += 1
                            logging.info(f"Processed file: {file_path}")
                            break  
                        except ValueError as e:
                            self.handle_error(file_path, e)
                            break 
                        except IOError as e:
                            attempts += 1
                            if attempts < RETRY_ATTEMPTS:
                                logging.warning(f"Transient error on {file_path}: {e}. Retrying in {RETRY_DELAY} seconds...")
                                time.sleep(RETRY_DELAY)
                            else:
                                self.handle_error(file_path, f"Reached maximum retry attempts for: {e}")
                                break
                    progress_bar.update(1)
                else:
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
