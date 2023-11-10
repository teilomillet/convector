import json
import os
import logging
from tqdm import tqdm
from pathlib import Path
from contextlib import contextmanager

from convector.core.file_handler_factory import FileHandlerFactory
from convector.utils.output_schema_handler import OutputSchemaHandler
from convector.core.config import Profile

logging.basicConfig(level=logging.INFO)

class FileHandler:
    def __init__(self, profile, file_handler):
        self.profile = profile
        self.file_handler = file_handler
        self.output_dir = profile.output_dir

    def get_output_file_path(self):
        if hasattr(self.profile, 'output_file') and self.profile.output_file:
            output_file_path = Path(self.output_dir) / self.profile.output_file
        else:
            input_path = Path(self.file_handler.file_path)
            output_base_name = input_path.stem + '_tr.jsonl'
            output_file_path = Path(self.output_dir) / output_base_name

        output_file_path.parent.mkdir(parents=True, exist_ok=True)

        absolute_path = output_file_path.resolve()
        logging.info(f"Output will be saved to: {absolute_path}")
        print(f"Output will be saved to: {absolute_path}")
        return output_file_path

    def validate_input_file(self):
        if not os.path.exists(self.file_handler.file_path):
            logging.error(f"The file '{self.file_handler.file_path}' does not exist.")
            return False
        return True
    
    def display_results(self, output_file_path, lines_written, total_bytes_written):
        print("Displaying results...")  # Debug
        absolute_path = Path(output_file_path).resolve()
        print(f"\nDelivered to file://{absolute_path} \n({lines_written} lines, {total_bytes_written} bytes)")

class DataTransformer:
    def __init__(self, profile, output_schema_handler):
        self.profile = profile
        self.output_schema_handler = output_schema_handler

    def transform_item(self, item):
        """
        Apply the transformation schema to the item.
        """
        if self.output_schema_handler is not None:
            transformed_item = self.output_schema_handler.apply_schema(item, labels=self.profile.labels)
        else:
            transformed_item = item

        # You could add additional transformation logic here if needed.
        # For example, if there are other fields that need to be modified or added,
        # you could do so before returning the transformed item.

        return transformed_item
    
class FileWriter:
    def __init__(self, output_file_path, mode='a'):
        self.output_file_path = output_file_path
        self.mode = mode
        self.buffer = []

    def write_item(self, item):
        self.buffer.append(json.dumps(item, ensure_ascii=False) + '\n')
        if len(self.buffer) >= 100:  # Flush every 100 items
            self.flush()

    def flush(self):
        with open(self.output_file_path, self.mode, encoding='utf-8') as file:
            file.writelines(self.buffer)
            self.mode = 'a'  # After the first write, always append
        self.buffer.clear()

    def close(self):
        if self.buffer:
            self.flush()

@contextmanager
def managed_progress_bar(total_lines):
    progress_bar = tqdm(total=total_lines, unit=" lines", position=0, desc="Processing", leave=True)
    try:
        yield progress_bar
    finally:
        progress_bar.close()

class DataSaver:
    def __init__(self, profile, output_file_path, data_transformer):
        self.profile = profile
        self.output_file_path = output_file_path
        self.data_transformer = data_transformer
        self.file_writer = FileWriter(output_file_path)

    def save_data(self, transformed_data_generator, total_lines, bytes, append):
        lines_written = 0
        total_bytes_written = 0

        with managed_progress_bar(total_lines or 0) as progress_bar:
            for items in transformed_data_generator:
                if isinstance(items, dict):
                    items = [items]
                for item in items:
                    transformed_item = self.data_transformer.transform_item(item)
                    self.file_writer.write_item(transformed_item)

                    lines_written += 1
                    # Update the progress bar and bytes written as needed...
                    progress_bar.update(1)

                    if total_lines and lines_written >= total_lines:
                        break
                    # More conditions based on bytes or other stopping criteria...
        
        self.file_writer.close()  # Ensure the buffer is flushed at the end


class ProcessingOrchestrator:
    def __init__(self, profile, file_handler_module, data_transformer):
        self.profile = profile
        self.file_handler_module = file_handler_module
        self.data_transformer = data_transformer

    def orchestrate(self):
        if not self.file_handler_module.validate_input_file():
            return

        logging.info("Starting processing...")
        output_file_path = self.file_handler_module.get_output_file_path()
        transformed_data_generator = self.file_handler_module.file_handler.handle_file()

        data_saver = DataSaver(
            self.profile, 
            output_file_path, 
            self.data_transformer
        )
        data_saver.save_data(
            transformed_data_generator,
            total_lines=self.profile.lines,
            bytes=self.profile.bytes,
            append=self.profile.append
        )
        logging.info("Data processing and saving complete.")

class Convector:
    """
    Convector is the class that sets up the processing environment and starts the process.
    """
    def __init__(self, profile: Profile, user_interaction, file_path):
        """
        Initialization of the Convector object with necessary handlers and configurations.
        """
        self.profile = profile 
        self.user_interaction = user_interaction
        # FileHandlerFactory creates an appropriate file handler based on the file_path and profile.
        self.file_handler = FileHandlerFactory.create_file_handler(file_path, profile)
        # OutputSchemaHandler is initialized here and will be passed where needed.
        self.output_schema_handler = OutputSchemaHandler(profile.output_schema, labels=profile.labels)

        # The FileHandler module handles file validation and getting the output file path.
        self.file_handler_module = FileHandler(profile, self.file_handler)
        # The DataTransformer module will be responsible for transforming the data according to the schema.
        self.data_transformer = DataTransformer(profile, self.output_schema_handler)

    def process(self):
        # Processing is now delegated to the ProcessingOrchestrator.
        orchestrator = ProcessingOrchestrator(self.profile, self.file_handler_module, self.data_transformer)
        orchestrator.orchestrate()

