# convector.py
import json
import os
import logging
from tqdm import tqdm
from pathlib import Path
from contextlib import contextmanager

from convector.core.file_handler_factory import FileHandlerFactory
from convector.utils.output_schema_handler import OutputSchemaHandler
from convector.core.base_file_handler import BaseFileHandler
from convector.core.profile import Profile, FilterCondition
from convector.utils.label_filter import LabelFilter

logging.basicConfig(level=logging.INFO)

class FileProcessing:
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
        print(f"\nOutput will be saved to: {absolute_path}\n")
        return output_file_path

    def validate_input_file(self):
        if not os.path.exists(self.file_handler.file_path):
            logging.error(f"The file '{self.file_handler.file_path}' does not exist.")
            return False
        return True
  
    def display_results(self, output_file_path, lines_written, total_bytes_written):
        absolute_path = Path(output_file_path).resolve()
        print(f"\nDelivered to file://{absolute_path} \n({lines_written} lines, {total_bytes_written} bytes)")

class DataTransformer:
    def __init__(self, profile, output_schema_handler, file_handler):
        self.profile = profile
        self.output_schema_handler = output_schema_handler
        self.file_handler = file_handler

    def transform_item(self, item):
        processed_item = self.file_handler.transform_data(item)

        if not processed_item or not any(processed_item):  # Check if filtered_items is empty or contains empty dicts
            return []  # Return empty list if no items to process

        transformed_items = []
        for item in processed_item:
            if self.output_schema_handler is not None:
                transformed_item = self.output_schema_handler.apply_schema(item)
            else:
                transformed_item = item
            transformed_items.append(transformed_item)

        return transformed_items

class FileWriter:
    def __init__(self, output_file_path, mode='a', source=None):
        self.output_file_path = output_file_path
        self.mode = mode
        self.source = source  # The origin file name
        self.buffer = []

    def write_item(self, item):
        item_with_origin = item.copy()  # Copy the item
        item_with_origin['origin'] = self.source  # Add the 'source' field
        self.buffer.append(json.dumps(item_with_origin, ensure_ascii=False) + '\n')
        if len(self.buffer) >= 100:
            self.flush()

    def flush(self):
        with open(self.output_file_path, self.mode, encoding='utf-8') as file:
            file.writelines(self.buffer)
            self.mode = 'a'  # After the first write, always append
        self.buffer.clear()

    def close(self):
        if self.buffer:
            self.flush()
   
class DataSaver:
    def __init__(self, profile, output_file_path, data_transformer):
        self.profile = profile
        self.output_file_path = output_file_path
        self.data_transformer = data_transformer
        source_file_name = Path(self.data_transformer.file_handler.file_path).name 
        self.file_writer = FileWriter(output_file_path, source=source_file_name)

    def save_data(self, transformed_data_generator, total_lines, bytes, append):
        lines_written = 0
        total_bytes_written = 0

        with managed_progress_bar(total_lines or 0) as progress_bar:
            for items in transformed_data_generator:
                # Check if items is a list and iterate through each item if so
                if isinstance(items, list):
                    for item in items:
                        transformed_item = self.data_transformer.transform_item(item)

                        if isinstance(transformed_item, dict):
                            self.file_writer.write_item(transformed_item)
                        else:
                            for single_item in transformed_item:
                                self.file_writer.write_item(single_item)

                        lines_written += 1
                        progress_bar.update(1)

                        if total_lines and lines_written >= total_lines:
                            break
                else:
                    transformed_item = self.data_transformer.transform_item(items)

                    self.file_writer.write_item(transformed_item)

                    lines_written += 1
                    progress_bar.update(1)

                    if total_lines and lines_written >= total_lines:
                        break
        
        self.file_writer.close() # Ensure the buffer is flushed at the end
    
@contextmanager
def managed_progress_bar(total_lines):
    progress_bar = tqdm(total=total_lines, unit=" lines", position=0, desc="Processing", leave=True)
    try:
        yield progress_bar
    finally:
        progress_bar.close()

class ProcessingOrchestrator:
    def __init__(self, profile, file_handler_module, data_transformer, output_schema_handler):
        self.profile = profile
        self.file_handler_module = file_handler_module
        self.data_transformer = data_transformer
        self.output_schema_handler = output_schema_handler

    def orchestrate(self):
        if not self.file_handler_module.validate_input_file():
            return

        output_file_path = self.file_handler_module.get_output_file_path()

        # Correctly initialize the DataTransformer with the file handler and output schema handler
        self.data_transformer = DataTransformer(self.profile, self.output_schema_handler, self.file_handler_module.file_handler)

        # Start processing the file
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
        self.output_schema_handler = OutputSchemaHandler(profile.output_schema, filters=profile.filters)

        # The FileHandler module handles file validation and getting the output file path.
        self.file_handler_module = FileProcessing(profile, self.file_handler)
        # The DataTransformer module will be responsible for transforming the data according to the schema.
        self.data_transformer = DataTransformer(profile, self.output_schema_handler, self.file_handler)

    def process(self):
        # Processing is now delegated to the ProcessingOrchestrator.
        orchestrator = ProcessingOrchestrator(self.profile, self.file_handler_module, self.data_transformer, self.output_schema_handler)
        orchestrator.orchestrate()

