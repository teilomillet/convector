import click
import yaml
import logging
import shutil
from pathlib import Path
from enum import Enum, auto
from typing import List, Dict, Any, Optional, Union, NoReturn
from tempfile import NamedTemporaryFile
from logging.handlers import RotatingFileHandler

# Importing necessary classes and functions
from .core.base_file_handler import BaseFileHandler
from .core.file_handler_factory import FileHandlerFactory
from .core.directory_processor import DirectoryProcessor
from .convector import Convector, config_manager, ConfigLoader, UserInteraction

CONFIG_FILE = 'setup.yaml'

# Custom exceptions are defined here to provide more meaningful error messages and 
# to facilitate the capture and handling of specific error conditions within the application.
class ConfigurationError(Exception):
    """Exception raised for errors in the configuration."""
    pass

class ProcessingError(Exception):
    """Exception raised during processing of the conversational data."""
    pass


# The dedicated logger is set up here. This allows the application to have a more 
# flexible and controlled logging system, separate from the root logger.
logger = logging.getLogger('convector')
logger.setLevel(logging.INFO)  # Default level can be INFO

# Console handler for the logger to output logs to the console.
# This is where the format of the log messages is defined.
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# This function is used to dynamically set the logging level. If verbose output is
# requested by the user (via a CLI flag), the logging level is set to DEBUG; otherwise, it remains INFO.
def set_logging_level(verbose: bool) -> None:
    """
    Set the logging level to DEBUG if verbose is True, otherwise leave it as the default.

    Args:
        verbose (bool): If true, set logging to DEBUG, otherwise INFO.
    """
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

# The ConfigValidator class is designed to ensure that the provided configuration
# contains all the necessary keys. It raises a ConfigurationError if any are missing.
class ConfigValidator:
    # Constructor
    def __init__(self, required_keys: Optional[List[str]] = None) -> None:
        if required_keys is None:
            # Default required keys using the Enum
            required_keys = [ConfigKeys.FILE_PATH.value, ConfigKeys.CONVERSATION.value]
        self.required_keys = required_keys

    def validate(self, config: Dict[str, Any]) -> None:
        missing_keys = [key for key in self.required_keys if key not in config]
        if missing_keys:
            raise ConfigurationError(f"Missing required configuration keys: {', '.join(missing_keys)}")

# Factory classes like ConvectorFactory and DirectoryProcessorFactory encapsulate the 
# creation logic of Convector and DirectoryProcessor objects, respectively. This follows 
# the Factory design pattern, making the instantiation process more adaptable to changes.
class ConvectorFactory:
    @staticmethod
    def create_from_config(final_config, file_path):
        return Convector(
            final_config,
            UserInteraction(),
            str(file_path),
            final_config.get('is_conversation'),
            final_config.get('output_file'),
            final_config.get('output_dir'),
            final_config.get('output_schema'),
        )

class DirectoryProcessorFactory:
    @staticmethod
    def create_from_config(final_config, file_path):
        config_kwargs = {k: v for k, v in final_config.items() if k not in ['output_dir', 'output_file', 'output_schema']}
        return DirectoryProcessor(
            directory_path=str(file_path),
            config=final_config,
            **config_kwargs
        )

# The safe_update_yaml function is responsible for safely updating the YAML configuration file.
# It uses a temporary file to avoid data loss in case the update process is interrupted.
def safe_update_yaml(file_path: Union[str, Path], key: str, value: Any) -> None:
    with NamedTemporaryFile('w', delete=False) as temp_file:
        with open(file_path, 'r') as config_file:
            config = yaml.safe_load(config_file) or {}
            config[key] = value
            yaml.safe_dump(config, temp_file)
    shutil.move(temp_file.name, file_path)

# The echo_info and echo_error functions are wrappers around click's echo function, providing
# a consistent style for information and error messages throughout the CLI.
def echo_info(message: str) -> None:
    click.echo(click.style(message, fg='green'))

def echo_error(message: str) -> NoReturn:
    click.echo(click.style(f"Error: {message}", fg='red', bold=True))

# The main data processing function orchestrates the processing of conversational data.
# It uses the provided file path and configuration to determine the mode of processing (file or directory)
# and to initialize the appropriate processing class (Convector or DirectoryProcessor).
def process_conversational_data(file_path: Union[str, Path], final_config: Dict[str, Any]) -> None:
    file_path = Path(file_path)
    if file_path.is_dir():
        echo_info("Directory processing mode selected.")
        directory_processor = DirectoryProcessorFactory.create_from_config(final_config, file_path)
        directory_processor.process_directory()
        directory_processor.print_summary()
    elif file_path.is_file():
        echo_info(f"Processing file: {file_path}")
        convector = ConvectorFactory.create_from_config(final_config, file_path)
        convector.process()
    else:
        raise ProcessingError("The path provided is neither a file nor a directory.")

# CLI commands and options are defined below. Each command or option has a decorator that
# specifies its name, parameters, and help text. The use of decorators for CLI definition is 
# part of the Click library's design, which provides a declarative approach to command-line interfaces.

# The @click decorators are used to define the CLI structure, including commands, options, and arguments.
# Each CLI command function below has a corresponding docstring that explains its purpose and how to use it.

# The convector group is the main entry point for the CLI. It provides help information and serves as the parent for subcommands.
@click.group()
def convector():
    """
    Convector - A tool for transforming conversational data to a unified format.

    For more detailed information and examples, use --verbose.
    """
    pass

# Define the Enum for Configuration Keys
class ConfigKeys(Enum):
    FILE_PATH = 'file_path'
    CONVERSATION = 'is_conversation'
    INPUT = 'input'
    OUTPUT = 'output'
    INSTRUCTION = 'instruction'
    ADD = 'add'
    LINES = 'lines'
    BYTES = 'bytes'
    OUTPUT_FILE = 'output_file'
    OUTPUT_DIR = 'output_dir'
    APPEND = 'append'
    VERBOSE = 'verbose'
    RANDOM = 'random'
    PROFILE = 'profile'
    OUTPUT_SCHEMA = 'output_schema'
        
# The process command is the core of the CLI, allowing users to process conversational data files.
@convector.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--profile', default=None, help='Profile to use from the YAML config file.')
@click.option('--is_conversation', is_flag=True, help='Specify if the data is in a conversational format.')
@click.option('--input', help='Key for user inputs in the data.')
@click.option('--output', help='Key for bot outputs in the data.')
@click.option('--instruction', help='Key for instructions or system messages in the data.')
@click.option('--add', help='Comma-separated column names to keep in the transformed data.')
@click.option('--lines', type=int, help='Number of lines to process from the file.')
@click.option('--bytes', type=int, help='Number of bytes to process from the file.')
@click.option('--output_file', type=click.Path(), help='Path to the output file where transformed data will be saved.')
@click.option('--output_dir', type=click.Path(), help='Specify a custom directory where the output file will be saved.')
@click.option('--append', is_flag=True, help='Specify whether to append to or overwrite an existing file.')
@click.option('--verbose', is_flag=True, help='Enable verbose mode for detailed logs.')
@click.option('--random', is_flag=True, help='Randomly select lines from the file.')
@click.option('--output_schema', default=None, help='Output schema to use for the transformed data.')
def process(file_path: str, 
            is_conversation: bool, 
            input: Optional[str], 
            output: Optional[str], 
            instruction: Optional[str], 
            add: Optional[str], 
            lines: Optional[int], 
            bytes: Optional[int], 
            output_file: Optional[str], 
            output_dir: Optional[str], 
            append: bool, 
            verbose: bool, 
            random: bool, 
            profile: Optional[str], 
            output_schema: Optional[str]) -> None:
    """
    Process conversational data in FILE_PATH to a unified format.

    Args:
    file_path (str): The path to the conversational data file or directory.
    conversation (bool): Flag to specify if the data is in a conversational format.
    input (str): Key for user inputs in the data.
    output (str): Key for bot outputs in the data.
    instruction (str): Key for instructions or system messages in the data.
    add (str): Comma-separated column names to keep in the transformed data.
    lines (int): Number of lines to process from the file.
    bytes (int): Number of bytes to process from the file.
    output_file (str): Path to the output file where transformed data will be saved.
    output_dir (str): Directory where the output file will be saved.
    append (bool): Flag to append to or overwrite an existing file.
    verbose (bool): Flag to enable verbose mode for detailed logs.
    random (bool): Flag to randomly select lines from the file.
    profile (str): Profile to use from the YAML config file.
    output_schema (str): Output schema to use for the transformed data.
    
    Example:
        convector process /path/to/data --conversation --input user --output bot
    """
    try:
        # Set logging level based on verbose
        set_logging_level(verbose)

        # Define the CLI arguments in a dictionary
        cli_args = {
            ConfigKeys.FILE_PATH: file_path,
            ConfigKeys.CONVERSATION: is_conversation,
            ConfigKeys.INPUT: input,
            ConfigKeys.OUTPUT: output,
            ConfigKeys.INSTRUCTION: instruction,
            ConfigKeys.ADD: add,
            ConfigKeys.LINES: lines,
            ConfigKeys.BYTES: bytes,
            ConfigKeys.OUTPUT_FILE: output_file,
            ConfigKeys.OUTPUT_DIR: output_dir,
            ConfigKeys.APPEND: append,
            ConfigKeys.VERBOSE: verbose,
            ConfigKeys.RANDOM: random,
            ConfigKeys.PROFILE: profile,
            ConfigKeys.OUTPUT_SCHEMA: output_schema,
        }

        # Convert Enum keys to strings for compatibility with other parts of the code
        config_dict = {key.value: value for key, value in cli_args.items()}

        # Create an instance of ConfigLoader with the config_dict
        config_loader = ConfigLoader(config_manager, config_dict)
        final_config = config_loader.final_config

        # Validate the final configuration
        validator = ConfigValidator()
        validator.validate(final_config)

        # Process the conversational data with the final configuration
        process_conversational_data(file_path, final_config)

    except ConfigurationError as e:
        # Handle specific errors (e.g., validation errors)
        echo_error(f"Configuration Error: {e}")
        exit(1)

    except ProcessingError as e:
        # Handle any other unexpected errors
        echo_error(f"Unexpected Error: {e}")
        exit(1)

    else:
        # This block runs if no exceptions were raised
        echo_info("Processing completed.")

# Configuration management commands allow users to get and set configuration values.
@convector.group()
def config():
    """
    Manage Convector configurations.
    """
    pass

@config.command()
@click.argument('key')
@click.argument('value')
def set(key, value):
    """
    Set a configuration KEY with a specified VALUE.
    """
    try:
        safe_update_yaml(CONFIG_FILE, key, value)
        echo_info(f"Configuration updated: {key} = {value}")
    except Exception as e:
        echo_error(f"An error occurred while updating the configuration: {e}")

@config.command()
@click.argument('key')
def get(key):
    """
    Get the value of a configuration KEY.
    """
    try:
        with open(CONFIG_FILE, 'r') as file:
            config = yaml.safe_load(file) or {}
            value = config.get(key)
            click.echo(f"{key} = {value}")
    except Exception as e:
        click.echo(f"An error occurred while retrieving the configuration: {e}")

# The entry point check ensures that the script is being run directly and not imported as a module.
if __name__ == "__main__":
    convector()