import argparse
import json
import os
import yaml
import logging
import random
import traceback
import urllib.parse
from tqdm import tqdm
from pathlib import Path

from convector.core.base_file_handler import BaseFileHandler
from convector.core.file_handler_factory import FileHandlerFactory
from convector.data_processors.data_processors import IDataProcessor, ConversationDataProcessor, CustomKeysDataProcessor, AutoDetectDataProcessor
from convector.utils.output_schema_handler import OutputSchemaHandler
from convector.utils.random_selector import IRandomSelector, LineRandomSelector, ByteRandomSelector, ConversationRandomSelector


# Config file
CONFIG_FILE = 'setup.yaml'

# Default Config
DEFAULT_CONFIG = {
    'version': 1.0,
    'settings': {
        'CONVECTOR_ROOT_DIR': str(Path.home() / 'convector'),
        'default_profile': 'default',
    },
    'profiles': {
        'default': {
            'output_dir': 'silo',
            'is_conversation': False,
            'input': None,
            'output': None,
            'instruction': None,
            'additional_fields': [],
            'lines': 1000,
            'bytes': None,
            'append': False,
            'random_selection': False,
            'output_schema': 'default',
        },
        'Conversation': {
            'output_dir': 'silo',
            'is_conversation': True,
            'input': None,
            'output': None,
            'instruction': None,
            'additional_fields': [],
            'lines': None,
            'bytes': None,
            'append': False,
            'random_selection': False,
            'output_schema': 'default',
        },
    }
}

def check_or_create_config():
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'w') as file:
            yaml.safe_dump(DEFAULT_CONFIG, file)
        print(f"Created default configuration file at {CONFIG_FILE}")

# Call this function at the beginning of your program
check_or_create_config()

# Configuration Class
class Configuration:
    def __init__(self, config_file='setup.yaml'):
        self.config_file = config_file
        self.config_data = self.load_or_create_config()
        
    def load_or_create_config(self):
        try:
            with open(self.config_file, 'r') as file:
                config = yaml.safe_load(file)
                if 'data_processor' not in config:
                    config['data_processor'] = 'AutoDetect'  # Default value
                return config
        except FileNotFoundError:
            print(f"Creating default configuration file at {self.config_file}")
            self.save_config(DEFAULT_CONFIG)
            return DEFAULT_CONFIG
        except Exception as e:
            print(f"An error occurred while reading the configuration file: {e}")
            return {}

    def save_config(self, config_data):
        with open(self.config_file, 'w') as file:
            yaml.safe_dump(config_data, file)

    def prompt_for_convector_root_dir(self):
        # Load existing config data
        config_data = self.load_or_create_config()
        
        # Check if CONVECTOR_ROOT_DIR is already set
        convector_root_dir = config_data.get('settings', {}).get('CONVECTOR_ROOT_DIR', None)
        
        # If not set, prompt the user and update the YAML
        if convector_root_dir is None:
            convector_root_dir = UserInteraction.prompt_for_convector_root_dir()
            
            # Update YAML settings
            if 'settings' not in config_data:
                config_data['settings'] = {}
            config_data['settings']['CONVECTOR_ROOT_DIR'] = convector_root_dir
            self.save_config(config_data)
            
        return convector_root_dir
    
# Initialize Configuration
config_manager = Configuration()

# ConfigLoader Class
class ConfigLoader:
    def __init__(self, config_manager, cli_args):
        self.config_manager = config_manager
        self.yaml_config = self.config_manager.config_data
        self.cli_args = cli_args
        self.final_config = self.merge_configs()

    def merge_configs(self):
        default_profile_name = self.yaml_config.get('settings', {}).get('default_profile', 'default')
        final_config = self.yaml_config.get('profiles', {}).get(default_profile_name, {}).copy()
        
        profile_name = self.cli_args.get('profile')
        if profile_name:
            profile_config = self.yaml_config.get('profiles', {}).get(profile_name)
            if profile_config:
                final_config.update(profile_config)
            else:
                logging.warning(f"Profile {profile_name} not found in YAML.")
        
        # Adding the new key 'data_processor' from YAML if it exists
        data_processor = self.yaml_config.get('data_processor')
        if data_processor:
            final_config['data_processor'] = data_processor

        final_config.update({k: v for k, v in self.cli_args.items() if v is not None})
        return final_config

class UserInteraction:
    """
    UserInteraction handles interactions with the user, such as receiving inputs.
    """
    @staticmethod
    def load_existing_config():
        """Load existing configuration from YAML file."""
        try:
            with open(CONFIG_FILE, 'r') as file:
                config = yaml.safe_load(file)
            return config.get('CONVECTOR_ROOT_DIR', None)
        except FileNotFoundError:
            return None

    @staticmethod
    def display_warning():
        """Display a warning message about CONVECTOR_ROOT_DIR not being set."""
        print("Warning: CONVECTOR_ROOT_DIR is not set.")
        print("You can set it by running: export CONVECTOR_ROOT_DIR=/path/to/your/convector/repository")

    @staticmethod
    def display_ascii_art():
        """Display ASCII art."""
        # ASCII Art
        print("                           ___====-_  _-====___")
        print("                     _--^^^#####//      \#####^^^--_")
        print("                  _-^##########// (    ) \##########^-_")
        print("                 -############//  |\^^/|  \############-")
        print("               _/############//   (@::@)   \############\_")
        print("              /#############((     \//\    ))#############\  ")
        print("             -###############\\    (oo) \   //###############-")
        print("            -#################\\  / UUU  \ //#################-")
        print("           -###################\\/  (v)   \/###################-")
        print("          _#/|##########/\######(   /  \   )######/\##########|\#_")
        print("          |/ |#/\#/\#/\/  \#/\#/\  (/|||\) /\#/\#/  \/\#/\#/\|  \|")
        print("          `  |/  V  V  `   V  V /  ||(_)|| \ V  V    ' V  V  '")
        print("                              (ooo / / \ \ ooo)")
        print("                           `~  CONVECTOR  ~'")
            
    @staticmethod
    def prompt_for_convector_root_dir():
        """
        Prompt the user to set the CONVECTOR_ROOT_DIR.
        :return: The path to the CONVECTOR_ROOT_DIR.
        """
        convector_root_dir = UserInteraction.load_existing_config() or os.environ.get('CONVECTOR_ROOT_DIR')
        
        if not convector_root_dir:
            UserInteraction.display_warning()
            UserInteraction.display_ascii_art()
            
            # Define the default directory
            default_dir = os.path.join(os.path.expanduser('~'), 'convector')
            print(f"\nIf you continue, CONVECTOR_ROOT_DIR will be set to the default path: {default_dir}")
            
            # Ask for user confirmation
            user_input = input("Do you want to continue with this directory? (y/n): ").strip().lower()
            
            if user_input == 'y':
                convector_root_dir = default_dir
                print(f"Continuing with CONVECTOR_ROOT_DIR set to {convector_root_dir}")
                
                # Save the configuration to a YAML file
                config = {'CONVECTOR_ROOT_DIR': convector_root_dir}
                with open(CONFIG_FILE, 'w') as file:
                    yaml.safe_dump(config, file)
            else:
                print("Operation aborted.")
                exit()
        
        return convector_root_dir
    
    @staticmethod
    def prompt_for_data_processor():
        """
        Prompt the user to select a data processor if it's not set in the configuration.
        """
        print("Data Processor is not set.")
        print("Available Options: AutoDetect, CustomKeys, Conversation")
        user_input = input("Select a data processor from the options: ").strip()
        
        if user_input not in ['AutoDetect', 'CustomKeys', 'Conversation']:
            print("Invalid choice. Using 'AutoDetect' as the default.")
            return 'AutoDetect'
        
        return user_input

logging.basicConfig(level=logging.INFO)

# Convector is the main class handling the transformation process of the conversational data.
class Convector:
    """
    Convector is the main class handling the transformation process of conversational data.
    """
    def __init__(self, config, user_interaction, file_path, is_conversation, output_schema, output_file=None, output_dir=None):
        """Initialization of the Convector object with necessary handlers and configurations."""
        self.config = config  # Final merged configuration
        self.user_interaction = user_interaction
        self.output_file = output_file
        self.output_dir = self.config.get('output_dir', 'silo')

        # Get output_schema from config if it is not passed explicitly
        output_schema = output_schema or self.config.get('output_schema', 'default')
        # Initialize OutputSchemaHandler
        self.output_schema_handler = OutputSchemaHandler(output_schema)
        

        # Initialize the FileHandler using the FileHandlerFactory
        self.file_handler = FileHandlerFactory.create_file_handler(file_path, is_conversation)
        
        # Retrieve or prompt for CONVECTOR_ROOT_DIR
        self.convector_root_dir = config_manager.prompt_for_convector_root_dir()
        
        # If CONVECTOR_ROOT_DIR is still None, then call a method to determine it
        if self.convector_root_dir is None:
            self.convector_root_dir = config_manager.config_data.get('settings', {}).get('CONVECTOR_ROOT_DIR', None)


    # Determine the output file path based on the provided or default configurations.
    def get_output_file_path(self):
        if self.output_file:
            output_file_path = Path(self.output_dir) / self.output_file
        else:
            input_path = Path(self.file_handler.file_path)
            output_base_name = input_path.stem + '_tr.jsonl'
            output_file_path = Path(self.output_dir) / output_base_name

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
        try:
            progress_bar = None
            output_file_path = self.get_output_file_path()
            lines_written, total_bytes_written = 0, 0
            
            # Ensure the output directory exists before opening the file
            output_file_path.parent.mkdir(parents=True, exist_ok=True)

            mode = 'a' if output_file_path.exists() and append else 'w'
            with open(output_file_path, mode, encoding='utf-8') as file:
                total = bytes or total_lines or 0  # Ensure total is never None
                unit = " bytes" if bytes else " lines"
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

    def process(self, *args, **kwargs):
        try:
            # Validate the existence of the input file
            if not self.validate_input_file():
                return
            
            print("Starting processing...")  # Debugging print
            
            output_file_path = self.get_output_file_path()
            output_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create an instance of OutputSchemaHandler
            output_schema_handler = OutputSchemaHandler(self.config.get('output_schema'))

            # Generate transformed data
            transformed_data_generator = self.file_handler.handle_file(
                input=kwargs.get('input'), 
                output=kwargs.get('output'), 
                instruction=kwargs.get('instruction'), 
                add=kwargs.get('add'), 
                lines=kwargs.get('lines'), 
                bytes=kwargs.get('bytes'), 
                random_selection=kwargs.get('random_selection'),
            )

            print("Data generation complete...")  # Debugging print

            # Process and save the transformed data
            self.process_and_save(transformed_data_generator, total_lines=kwargs.get('lines'), bytes=kwargs.get('bytes'), append=kwargs.get('append'))

            print("Data processing and saving complete...")  # Debugging print

        except FileNotFoundError:
            logging.error(f"The file '{self.file_handler.file_path}' does not exist.")
        except Exception as e:
            logging.error(f"An unexpected error occurred while transforming the file: {e}")
