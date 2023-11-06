import json
import os
import logging
from tqdm import tqdm
from pathlib import Path

from convector.core.file_handler_factory import FileHandlerFactory
from convector.utils.output_schema_handler import OutputSchemaHandler
from convector.core.user_interaction import UserInteraction
from convector.core.config import ConvectorConfig

logging.basicConfig(level=logging.INFO)

# Convector is the main class handling the transformation process of the conversational data.
class Convector:
    """
    Convector is the main class handling the transformation process of conversational data.
    """
    def __init__(self, config, user_interaction, file_path):
        """Initialization of the Convector object with necessary handlers and configurations."""
        self.config = config  # This is now an instance of ConvectorConfig
        self.user_interaction = user_interaction
        self.file_handler = FileHandlerFactory.create_file_handler(file_path, config)

        logging.debug(f"Config object type: {type(self.config)}")
        logging.debug(f"Config object attributes: {dir(self.config)}")


        #This will help determine if the config object is correct and has the get_active_profile method.
        self.output_dir = self.config.get_active_profile().output_dir
        
        # Initialize OutputSchemaHandler with the output schema from ConvectorConfig
        self.output_schema_handler = OutputSchemaHandler(self.config.get_active_profile().output_schema)
        
        # No need to prompt for CONVECTOR_ROOT_DIR; it should be handled by ConvectorConfig
        self.convector_root_dir = self.config.convector_root_dir

    # Determine the output file path based on the provided or default configurations.
    def get_output_file_path(self):
        # Check if 'output_file' attribute exists in the config and is not None
        if hasattr(self.config, 'output_file') and self.config.output_file:
            output_file_path = Path(self.output_dir) / self.config.output_file
        else:
            # Use default naming convention: original name + '_tr.jsonl'
            input_path = Path(self.file_handler.file_path)
            output_base_name = input_path.stem + '_tr.jsonl'
            output_file_path = Path(self.output_dir) / output_base_name

        # Ensure the directory for the output file exists
        output_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Log the output path
        absolute_path = output_file_path.resolve()
        logging.info(f"Output will be saved to: {absolute_path}")
        print(f"Output will be saved to: {absolute_path}")  # This will print to console
        return output_file_path

    def write_to_file(self, file, item, lines_written, total_bytes_written, total_lines, bytes, output_schema_handler):
        """
        Write transformed items to the output file.
        """
        try:
            if total_lines and lines_written >= total_lines:
                return lines_written, total_bytes_written, True

            # Transform the item based on the output schema
            if output_schema_handler is not None:  
                transformed_item = output_schema_handler.apply_schema(item)
            else:
                transformed_item = item

            try:
                if not isinstance(transformed_item, dict):
                    print(f"Error: Expected a dictionary but got {type(transformed_item)}")
                    # handle the error appropriately, perhaps with a 'raise' or 'return'
                else:
                    transformed_item['source'] = os.path.basename(self.file_handler.file_path)
            except Exception as e:
                print(f"An exception occurred: {e}")  # Debug
                raise

            json_line = json.dumps(transformed_item, ensure_ascii=False) + '\n'
            line_bytes = len(json_line.encode('utf-8'))

            if bytes and (total_bytes_written + line_bytes) > bytes:
                return lines_written, total_bytes_written, True

            file.write(json_line)
            return lines_written + 1, total_bytes_written + line_bytes, False
        except Exception as e:
            logging.error(f"An error occurred while writing to the file: {e}")
            raise

    def process_and_save(self, transformed_data_generator, total_lines=None, bytes=None, append=False):
        """
        Process the transformed data and save it to the output file.
        """
        profile = self.config.get_active_profile()  # Retrieve the active profile
        try:
            progress_bar = None
            output_file_path = self.get_output_file_path()
            lines_written, total_bytes_written = 0, 0
            
            # Ensure the output directory exists before opening the file
            output_file_path.parent.mkdir(parents=True, exist_ok=True)

            mode = 'a' if output_file_path.exists() and profile.append else 'w'
            with open(output_file_path, mode, encoding='utf-8') as file:
                total = profile.bytes or profile.lines or 0  # Use profile values
                unit = " bytes" if profile.bytes else " lines"
                progress_bar = tqdm(total=total, unit=unit, position=0, desc="Processing", leave=True)
                
                for items in transformed_data_generator:
                    if isinstance(items, dict):  # if items is a single dictionary
                        items = [items]
                    for item in items:
                        lines_written, total_bytes_written, done = self.write_to_file(
                            file, item, lines_written, total_bytes_written, total_lines, bytes, self.output_schema_handler
                        )
                        progress_bar.update(1)
                        if done:
                            break
                
                progress_bar.close()
                
            # Display the results after processing is complete
            self.display_results(output_file_path, lines_written, total_bytes_written)
            
        except FileNotFoundError:
            logging.error("Error: Output directory does not exist and could not be created.")
        except IOError as e:
            logging.error(f"IO Error: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing and saving the file: {e}")
        finally:
            if progress_bar:
                progress_bar.close()

    def display_results(self, output_file_path, lines_written, total_bytes_written):
        print("Displaying results...")  # Debugging print
        absolute_path = Path(output_file_path).resolve()
        print(f"\nDelivered to file://{absolute_path} \n({lines_written} lines, {total_bytes_written} bytes)")

    def validate_input_file(self):
        if not os.path.exists(self.file_handler.file_path):
            logging.error(f"The file '{self.file_handler.file_path}' does not exist.")
            return False
        return True

    def process(self):
        """
        Processes the input file and writes the output to a file.
        This method orchestrates the entire process, from reading the input,
        processing the data, and writing the output.
        """
        try:
            if not self.validate_input_file():
                return
            
            logging.info("Starting processing...")

            output_file_path = self.get_output_file_path()
            output_file_path.parent.mkdir(parents=True, exist_ok=True)

            # No CLI arguments are passed to the handle_file method.
            # The file handler uses the configuration object to control its behavior.
            transformed_data_generator = self.file_handler.handle_file()

            logging.info("Data generation complete...")

            # The process_and_save method handles the writing of transformed data to the output file.
            self.process_and_save(
                transformed_data_generator,
                total_lines = getattr(self.config, 'lines', None),
                bytes=getattr(self.config, 'bytes', None),
                append=getattr(self.config, 'append', None),
            )

            logging.info("Data processing and saving complete...")
        except FileNotFoundError:
            logging.error(f"The file '{self.file_handler.file_path}' does not exist.")
        except Exception as e:
            logging.error(f"An unexpected error occurred while transforming the file: {e}")
