import argparse
import json
import os
import yaml
import logging
import random
import urllib.parse
from tqdm import tqdm
from pathlib import Path

# Importing the FileHandler class
from .handlers import FileHandler  

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
            'conversation': False,
            'input': None,
            'output': None,
            'instruction': None,
            'additional_fields': [],
            'lines': 1000,
            'bytes': None,
            'append': False,
            'random_selection': False,
        },
        'Conversation': {
            'output_dir': 'silo',
            'conversation': True,
            'input': None,
            'output': None,
            'instruction': None,
            'additional_fields': [],
            'lines': None,
            'bytes': None,
            'append': False,
            'random_selection': False,
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


# ConfigLoader is responsible for managing configurations such as CONVECTOR_ROOT_DIR.
class ConfigLoader:
    def __init__(self, yaml_file, cli_args):
        self.yaml_config = self.load_yaml(yaml_file)
        self.cli_args = cli_args
        self.final_config = self.merge_configs()
        print(self.yaml_config)

    def load_yaml(self, yaml_file):
        try:
            with open(yaml_file, 'r') as file:
                config = yaml.safe_load(file)
            # Optionally validate config here
            return config
        except FileNotFoundError:
            logging.error(f"YAML file {yaml_file} not found.")
            return {}
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {e}")
            return {}

    def merge_configs(self):
        # Start with settings from the default profile
        default_profile_name = self.yaml_config.get('settings', {}).get('default_profile', 'default')
        final_config = self.yaml_config.get('profiles', {}).get(default_profile_name, {}).copy()
        
        # If another profile is specified, update the final config with it
        profile_name = self.cli_args.get('profile')
        if profile_name:
            profile_config = self.yaml_config.get('profiles', {}).get(profile_name)
            if profile_config:
                final_config.update(profile_config)
            else:
                logging.warning(f"Profile {profile_name} not found in YAML.")
        
        # CLI arguments should override YAML config and selected profile
        final_config.update({k: v for k, v in self.cli_args.items() if v is not None})
        return final_config



# UserInteraction handles interactions with the user, such as receiving inputs.
class UserInteraction:
    @staticmethod

    # Functionality for user interaction to set the CONVECTOR_ROOT_DIR.
    def prompt_for_convector_root_dir():
        # Load existing configuration from YAML file
        try:
            with open(CONFIG_FILE, 'r') as file:
                config = yaml.safe_load(file)
                convector_root_dir = config.get('CONVECTOR_ROOT_DIR', None)
        except FileNotFoundError:
            config = {}
            convector_root_dir = None
            
        convector_root_dir = convector_root_dir or os.environ.get('CONVECTOR_ROOT_DIR')
        
        if not convector_root_dir:
            print("Warning: CONVECTOR_ROOT_DIR is not set.")
            print("You can set it by running: export CONVECTOR_ROOT_DIR=/path/to/your/convector/repository")
            
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
            
            # Define the default directory
            default_dir = os.path.join(os.path.expanduser('~'), 'convector')
            print(f"\nIf you continue, CONVECTOR_ROOT_DIR will be set to the default path: {default_dir}")
            
            # Ask for user confirmation
            user_input = input("Do you want to continue with this directory? (y/n): ").strip().lower()
            
            if user_input == 'y':
                convector_root_dir = os.path.join(os.path.expanduser('~'), 'convector')
                print(f"Continuing with CONVECTOR_ROOT_DIR set to {convector_root_dir}")
                
                # Save the configuration to a YAML file
                config['CONVECTOR_ROOT_DIR'] = convector_root_dir
                with open(CONFIG_FILE, 'w') as file:
                    yaml.safe_dump(config, file)
            else:
                print("Operation aborted.")
                exit()  # or return to the previous menu, depending on your flow
        
        return convector_root_dir

# Convector is the main class handling the transformation process of the conversational data.
class Convector:
    # Initialization of the Convector object with necessary handlers and configurations.
    def __init__(self, config, user_interaction, file_handler, output_file=None, output_dir=None):
        self.config = config  # Initialize self.config here
        self.user_interaction = user_interaction
        self.file_handler = file_handler
        self.output_file = output_file
        self.output_dir = self.config.get('output_dir', 'silo')

        self.convector_root_dir = self.config.get('CONVECTOR_ROOT_DIR')
        if self.convector_root_dir is None:
            self.convector_root_dir = self.default_convector_root_dir()

    def default_convector_root_dir(self):
        # Define and return the default directory
        return str(Path.home() / 'convector')

    def set_output_dir(self, output_dir):
        if self.convector_root_dir is None:
            raise ValueError("convector_root_dir is not set.")
        
        default_output_dir = os.path.join(self.convector_root_dir, 'silo')
    
    # Determine the output file path based on the provided or default configurations.
    def get_output_file_path(self):
        if self.output_file:
            return os.path.join(self.output_dir, self.output_file)
        else:
            output_base_name = os.path.splitext(os.path.basename(self.file_handler.file_path))[0] + '_tr.jsonl'
            return os.path.join(self.output_dir, output_base_name)

    # Write transformed items to the output file.
    def write_to_file(self, file, item, lines_written, total_bytes_written, total_lines, bytes):
        try:
            if total_lines and lines_written >= total_lines:
                return lines_written, total_bytes_written, True
            
            item['source'] = os.path.basename(self.file_handler.file_path)
            json_line = json.dumps(item, ensure_ascii=False) + '\n'
            line_bytes = len(json_line.encode('utf-8'))
            
            if bytes and (total_bytes_written + line_bytes) > bytes:
                return lines_written, total_bytes_written, True
            
            file.write(json_line)
            return lines_written + 1, total_bytes_written + line_bytes, False
        except Exception as e:
            print(f"An error occurred while writing to the file: {e}")
            raise

    # Process the transformed data and save it to the output file.
    def process_and_save(self, transformed_data_generator, total_lines=None, bytes=None, append=False):
        try:
            progress_bar = None
            output_file_path = self.get_output_file_path()
            lines_written, total_bytes_written = 0, 0
            
            try:
                os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
                mode = 'a' if os.path.exists(output_file_path) and append else 'w'
                with open(output_file_path, mode, encoding='utf-8') as file:
                    total = bytes or total_lines or 0  # Ensure total is never None
                    unit = " bytes" if bytes else " lines"
                    progress_bar = tqdm(total=total, unit=unit, position=0, desc="Processing", leave=True)
                    
                    for items in transformed_data_generator:
                        for item in items:
                            lines_written, total_bytes_written, done = self.write_to_file(file, item, lines_written, total_bytes_written, total_lines, bytes)
                            progress_bar.update(1)
                            if done:
                                return
                    
                    progress_bar.close()
                    
            except Exception as e:
                    print(f"An error occurred while processing and saving the file: {e}")
            finally:
                if progress_bar:
                    progress_bar.close()
                self.display_results(output_file_path, lines_written, total_bytes_written)
        except FileNotFoundError:
            print("Error: Output directory does not exist and could not be created.")
        except IOError as e:
            print(f"IO Error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while processing and saving the file: {e}")

    def display_results(self, output_file_path, lines_written, total_bytes_written):
        absolute_path = Path(output_file_path).resolve()
        print(f"\nDelivered to file://{absolute_path} \n({lines_written} lines, {total_bytes_written} bytes)")

   # Main transformation function to handle the transformation of conversational data.
    def transform(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, append=False, random_selection=False):
        try:    
            # Validating the existence of the input file
            if not os.path.exists(self.file_handler.file_path):
                print(f"Error: The file '{self.file_handler.file_path}' does not exist.")
                return
            
            # Identifying the file type based on its extension
            file_extension = os.path.splitext(self.file_handler.file_path)[-1].lower()

            print(f"File: {self.file_handler.file_path}")
            handler_method = getattr(self.file_handler, f"handle_{file_extension[1:]}", None)

            # Prompting user for number of lines or bytes if random_selection is True and neither is specified
            if random_selection and not lines and not bytes:
                if True:
                    print("Error: Please specify the number of lines or bytes when using random selection.")
                    return

            if handler_method:
                print(f"Charging .{file_extension[1:]} file...")
                
                transformed_data_generator = handler_method(input=input, output=output,
                                                            instruction=instruction, add=add,
                                                            lines=lines, bytes=bytes, random_selection=random_selection)
                
                # Processing and saving the transformed data considering the byte size limit
                self.process_and_save(transformed_data_generator, total_lines=lines, bytes=bytes, append=append)
            else:
                print(f"Error: Unsupported file extension '{file_extension}'")
        except FileNotFoundError:
            print(f"Error: The file '{self.file_handler.file_path}' does not exist.")
        except AttributeError:
            print(f"Error: Unsupported file extension '{file_extension}'.")
        except Exception as e:
            print(f"An unexpected error occurred while transforming the file: {e}")