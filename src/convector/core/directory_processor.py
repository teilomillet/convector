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
        Process all files in the directory recursively.
        """
        files = self._get_all_files()
        with tqdm(total=len(files), unit='file') as progress_bar:
            for file_path in files:
                self._process_or_skip_file(file_path, progress_bar)

    def _get_all_files(self):
        """
        Retrieve all files in the directory.
        """
        return list(self.directory_path.rglob('*'))

    def _process_or_skip_file(self, file_path, progress_bar):
        """
        Decide whether to process a file or skip it.
        """
        if file_path.is_file(): 
            self._process_file(file_path, progress_bar)
        else:
            self._update_progress_bar_for_skipped_file(file_path, progress_bar)

    def _process_file(self, file_path, progress_bar):
        """
        Process an individual file.
        """
        for attempt in range(RETRY_ATTEMPTS):
            if self._try_processing_file(file_path, attempt):
                break
        progress_bar.update(1)

    def _try_processing_file(self, file_path, attempt):
        """
        Attempt to process a file, handling errors and retries.
        """
        try:
            self._handle_file_processing(file_path)
            self.processed_files += 1
            logging.info(f"Processed file: {file_path}")
            return True
        except ValueError as e:
            self.handle_error(file_path, e)
            return True
        except IOError as e:
            return self._retry_or_skip(file_path, e, attempt)

    def _handle_file_processing(self, file_path):
        """
        Handle the processing of a single file.
        """
        convector = Convector(profile=self.profile, user_interaction=UserInteraction(), file_path=str(file_path))
        convector.process()

    def _retry_or_skip(self, file_path, error, attempt):
        """
        Determine whether to retry processing a file or skip it.
        """
        if attempt < RETRY_ATTEMPTS - 1:
            logging.warning(f"Transient error on {file_path}: {error}. Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            return False
        else:
            self.handle_error(file_path, f"Reached maximum retry attempts for: {error}")
            return True

    def _update_progress_bar_for_skipped_file(self, file_path, progress_bar):
        """
        Update the progress bar for a skipped file.
        """
        progress_bar.set_postfix_str(f"Skipped: {file_path} (Not a file)")
        progress_bar.update(1)

    def handle_error(self, file_path, error):
        """
        Handle errors during file processing.
        """
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
