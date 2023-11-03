import click
import os
import yaml
import logging
from pathlib import Path

# Importing necessary classes and functions
from .core.base_file_handler import BaseFileHandler
from .core.file_handler_factory import FileHandlerFactory
from .core.directory_processor import DirectoryProcessor
from .convector import Convector, config_manager, ConfigLoader, UserInteraction


# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CONFIG_FILE = 'setup.yaml'


@click.group()
def convector():
    """
    Convector - A tool for transforming conversational data to a unified format.

    For more detailed information and examples, use --verbose.
    """
    pass

def validate_config(final_config):
    required_keys = ['file_path', 'conversation']  # Add other required keys
    for key in required_keys:
        if key not in final_config:
            raise ValueError(f"Missing required configuration: {key}")
        

def create_convector_instance(final_config, file_path):
    return Convector(
        final_config,
        UserInteraction(),
        str(file_path),
        final_config.get('conversation'),
        final_config.get('output_file'),
        final_config.get('output_dir'),
        final_config.get('output_schema'),
    )

def create_directory_processor_instance(final_config, file_path):
    # Create a copy of the final_config to manipulate
    config_kwargs = final_config.copy()

    # Remove keys that are already explicitly passed to the constructor
    for key in ['output_dir', 'output_file', 'output_schema']:
        config_kwargs.pop(key, None)  # Remove the key if it exists, do nothing otherwise

    # Now pass the remaining configuration as keyword arguments
    return DirectoryProcessor(
        directory_path=str(file_path),
        config=final_config,
        is_conversation=final_config.get('conversation'),
        output_file=final_config.get('output_file'),
        output_dir=final_config.get('output_dir'),
        output_schema=final_config.get('output_schema'),
        **config_kwargs  # Pass all remaining configuration parameters as keyword arguments
    )
def process_conversational_data(file_path, final_config):
    file_path = Path(file_path)
    if file_path.is_dir():
        logging.info("Directory processing mode selected.")
        directory_processor = create_directory_processor_instance(final_config, file_path)
        directory_processor.process_directory()
        directory_processor.print_summary()
    elif file_path.is_file():
        logging.info(f"Processing file: {file_path}")
        convector = create_convector_instance(final_config, file_path)
        convector.process()
    else:
        logging.error("The path provided is neither a file nor a directory.")
        raise ValueError("Invalid path provided")
        
@convector.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--profile', default=None, help='Profile to use from the YAML config file.')
@click.option('--conversation', is_flag=True, help='Specify if the data is in a conversational format.')
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
def process(file_path, conversation, input, output, instruction, add, lines, bytes, output_file, output_dir, append, verbose, random, profile, output_schema):
    """
    Process conversational data in FILE_PATH to a unified format.

    Example:
        convector process /path/to/data --conversation --input user --output bot
    """
    # Define the CLI arguments in a dictionary
    cli_args = {
        'file_path': file_path,
        'conversation': conversation,
        'input': input,
        'output': output,
        'instruction': instruction,
        'add': add,
        'lines': lines,
        'bytes': bytes,
        'output_file': output_file,
        'output_dir': output_dir,
        'append': append,
        'verbose': verbose,
        'random': random,
        'profile': profile, # This is for selecting a profile from YAML
        'output_schema': output_schema,  
    }

    # Create an instance of ConfigLoader to get the final configuration
    config_loader = ConfigLoader(config_manager, cli_args)
    final_config = config_loader.final_config

    # Validate the final config
    try:
        validate_config(final_config)
    except ValueError as e:
        print(f"Configuration Error: {e}")
        exit(1)

     # Enable verbose logging if needed
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Process the conversational data
    try:
        process_conversational_data(file_path, final_config)
    except ValueError as e:
        logging.error(f"Error during processing: {e}")
        exit(1)

    logging.info("Processing completed.")

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
        with open(CONFIG_FILE, 'r+') as file:
            config = yaml.safe_load(file) or {}
            config[key] = value
            yaml.safe_dump(config, file)
        click.echo(f"Configuration updated: {key} = {value}")
    except Exception as e:
        click.echo(f"An error occurred while updating the configuration: {e}")


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


if __name__ == "__main__":
    convector()
