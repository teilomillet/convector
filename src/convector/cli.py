import click
import yaml
import logging
import shutil
import json
import logging.config
from pydantic import BaseModel, Field
from pathlib import Path
from typing import Any, Optional, Union
from tempfile import NamedTemporaryFile


# Importing necessary classes and functions
from .core.directory_processor import DirectoryProcessor
from .core.config import ConvectorConfig, Profile
from .core.user_interaction import UserInteraction
from .convector import Convector
from .core.file_handler_factory import FileHandlerFactory

PERSISTENT_CONFIG_PATH = Path.home() / '.convector_config'

# Custom exceptions are defined here to provide more meaningful error messages and 
# to facilitate the capture and handling of specific error conditions within the application.
class ConfigurationError(Exception):
    """Exception raised for errors in the configuration."""
    pass

class ProcessingError(Exception):
    """Exception raised during processing of the conversational data."""
    pass

def setup_logging(default_path: str = None, default_level=logging.INFO):
    """Setup logging configuration from a YAML file."""
    # If no default path is provided, try to read the convector_root_dir from .convector_config
    if default_path is None:
        # Check if .convector_config exists to get the root directory
        if PERSISTENT_CONFIG_PATH.exists():
            with open(PERSISTENT_CONFIG_PATH, 'r') as file:
                convector_root_dir = file.read().strip()
                default_path = Path(convector_root_dir) / 'config.yaml'
        else:
            # If .convector_config doesn't exist, use the default ConvectorConfig to determine the root directory
            config = ConvectorConfig()
            default_path = Path(config.convector_root_dir) / 'config.yaml'
    
    try:
        # Open the YAML configuration file
        with open(default_path, 'rt') as file:
            config = yaml.safe_load(file)
            logging.config.dictConfig(config)
    except FileNotFoundError:
        # If the file is not found, fall back to the default logging configuration
        logging.warning(f"Logging configuration file is not found at '{default_path}'. Using default configs.")
        logging.basicConfig(level=default_level)

# Factory classes like ConvectorFactory and DirectoryProcessorFactory encapsulate the 
# creation logic of Convector and DirectoryProcessor objects, respectively. This follows 
# the Factory design pattern, making the instantiation process more adaptable to changes.
class ConvectorFactory:
    @staticmethod
    def create_from_profile(profile: Profile, file_path):
        # Assuming `is_conversation` is intended to be a flag that indicates
        # whether the file contains conversational data or not.
        # If 'is_conversation' is an expected command-line argument,
        # ensure that it's being set in the config object somewhere after parsing CLI args.

        # Check if 'is_conversation' is part of the configuration
        is_conversation = profile.is_conversation

        # Pass 'is_conversation' explicitly
        convector = Convector(profile=profile, file_path=file_path, user_interaction=UserInteraction())

        # The FileHandlerFactory.create_file_handler call should be within the Convector class
        # if it relies on the config being passed to the Convector constructor.
        # Otherwise, pass 'is_conversation' directly to the FileHandlerFactory here
        # and then to the Convector object.

        return convector

class DirectoryProcessorFactory:
    @staticmethod
    def create_from_profile(profile, file_path):
        try:
            logging.debug(f'profile as dict: {profile.dict()}')
            # Extract is_conversation from the final_config
            is_conversation = profile.is_conversation

            # Now pass is_conversation to the DirectoryProcessor constructor
            return DirectoryProcessor(
                directory_path=str(file_path),
                profile=profile,
                is_conversation=is_conversation,
                # Include other keyword arguments as needed
            )
        except AttributeError as e:
            logging.error(f"Attribute error in DirectoryProcessorFactory: {e}")



# The echo_info and echo_error functions are wrappers around click's echo function, providing
# a consistent style for information and error messages throughout the CLI.
def echo_info(message: str) -> None:
    click.echo(click.style(message, fg='green'))


# Correct the process_conversational_data function
def process_conversational_data(file_path: Path, profile: Profile) -> None:
    if file_path.is_dir():
        echo_info("Directory processing mode selected.")
        directory_processor = DirectoryProcessorFactory.create_from_profile(profile, file_path)
        directory_processor.process_directory()
        directory_processor.print_summary()
    elif file_path.is_file():
        UserInteraction.show_message(f"Processing file: {file_path}")
        convector = Convector(profile, UserInteraction(), file_path)
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
    try:
        # Attempt to setup the environment and load the configuration
        config = UserInteraction.setup_environment()

        # Store the configuration in the Click context for use in other commands
        ctx = click.get_current_context()
        ctx.obj = config

        echo_info("Convector is ready to use.")

    except Exception as e:
        click.echo(f"An error occurred during setup: {e}")
        exit(1)
   
# The process command is the core of the CLI, allowing users to process conversational data files.
@convector.command()
@click.pass_context  # This decorator allows us to pass the Click context into the command function
@click.argument('file_path', type=click.Path(exists=True), metavar='<file_path>')
@click.option('-p', '--profile', default='default', help='Use a predefined profile from the config.')
@click.option('-c', '--conversation', 'is_conversation', is_flag=True, help='Treat the data as conversational exchanges.')
@click.option('--instruction', help='Key for instructions or system messages in the data.')
@click.option('-i', '--input-key', 'input', help='Key for user inputs.')
@click.option('-o', '--output-key', 'output', help='Key for bot responses.')
@click.option('-s', '--schema', 'output_schema', default=None, help='Schema for output data.')
@click.option('--label', 'labels', help='Columns to include in the output, separated by commas.')
@click.option('-l', '--limit', 'lines', type=int, help='Limit processing to this number of lines.')
@click.option('--bytes', type=int, help='Limit processing to this number of bytes.')
@click.option('-f', '--file-out', 'output_file', type=click.Path(), help='File to write the transformed data to.')
@click.option('-d', '--dir-out', 'output_dir', type=click.Path(), help='Directory for output files.')
@click.option('--append', is_flag=True, help='Add to an existing file instead of overwriting.')
@click.option('-v', '--verbose', is_flag=True, help='Print detailed logs of the process.')
@click.option('--random', is_flag=True, help='Randomly select data to process.')
def process(ctx, file_path: str, 
            is_conversation: bool, 
            input: Optional[str], 
            output: Optional[str], 
            instruction: Optional[str], 
            labels: Optional[str], 
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
        convector process <file_path> -c -i message -o response
        convector process <file_path> --profile conversation -l 100 -f output.json

    """
    # Retrieve the config object from the context
    config = ctx.obj

    try:
        # Retrieve the profile specified by the user or use the 'default' profile
        selected_profile_name = profile if profile else 'default'
        selected_profile = config.profiles.get(selected_profile_name)

        if not selected_profile:
            raise ConfigurationError(f"Profile '{selected_profile_name}' not found in configuration.")

        # Collect CLI arguments
        cli_args = {
            'is_conversation': is_conversation,
            'input': input,
            'output': output,
            'instruction': instruction,
            'labels': labels.split(',') if labels else labels,  # Convert comma-separated string to list
            'lines': lines,
            'bytes': bytes,
            'output_file': output_file,
            'output_dir': output_dir,
            'append': append,
            'verbose': verbose,
            'random': random,
            'output_schema': output_schema,
        }

        # Update profile settings with CLI arguments if provided
        for key, value in cli_args.items():
            if value is not None:  # Only update attributes that have been provided
                setattr(selected_profile, key, value)

        # Display messages using UserInteraction
        UserInteraction.show_message("Processing started.", "info")

        # Save the profile if needed
        if selected_profile_name != 'default':
            config.save_profile_to_yaml(selected_profile_name)

        # Process the conversational data with the selected profile
        process_conversational_data(Path(file_path), selected_profile)

    except ConfigurationError as e:
        UserInteraction.show_message(f"Configuration Error: {e}", "error")
        exit(1)
    except ProcessingError as e:
        UserInteraction.show_message(f"Processing Error: {e}", "error")
        exit(1)
    else:
        UserInteraction.show_message("Processing completed.", "info")

# Configuration management commands allow users to get and set configuration values.
@convector.group()
def config():
    """
    Manage Convector configurations.
    """
    pass

@config.command()
@click.pass_context
@click.argument('key')
@click.argument('value')
def set(ctx, key, value):
    """
    Set a configuration KEY with a specified VALUE.
    """
    try:
        # Use the ConvectorConfig's method to update and save the configuration
        config = ctx.obj
        config.set_and_save(key, value)
        echo_info(f"Configuration updated: {key} = {value}")
    except Exception as e:
        click.error(f"An error occurred while updating the configuration: {e}")

# The entry point check ensures that the script is being run directly and not imported as a module.
if __name__ == "__main__":
    try:
        # Set up the application environment and configuration
        app_config = UserInteraction.setup_environment(ConvectorConfig)
        # Set up logging based on the loaded configuration
        setup_logging()
        # Run the CLI application
        convector()
    except Exception as e:
        logging.error(f"An error occurred during setup: {e}")
        exit(1)