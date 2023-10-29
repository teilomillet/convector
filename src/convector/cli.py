import click
import os
import yaml

# Importing necessary classes and functions
from .convector import Convector, config_manager, ConfigLoader, UserInteraction, FileHandler


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
def transform(file_path, conversation, input, output, instruction, add, lines, bytes, output_file, output_dir, append, verbose, random, profile):
    """
    Transform conversational data in FILE_PATH to a unified format.

    Example:
        convector transform /path/to/data --conversation --input user --output bot
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
        'profile': profile  # This is for selecting a profile from YAML
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

    # Create an instance of the Convector class with the final configuration
    convector = Convector(
        final_config, 
        UserInteraction(), 
        FileHandler(final_config['file_path'], final_config['conversation']), 
        final_config.get('output_file'),  # Use get() for optional keys
        final_config.get('output_dir')    # Use get() for optional keys
    )

    convector.transform(
        input=final_config['input'] if 'input' in final_config else None,
        output=final_config['output']if 'output' in final_config else None,
        instruction=final_config['instruction'] if 'instruction' in final_config else None,
        add=final_config['add'] if 'add' in final_config else None,
        lines=final_config['lines'] if 'lines' in final_config else None,
        bytes=final_config['bytes'] if 'bytes' in final_config else None,
        append=final_config['append'] if 'append' in final_config else None,
        random_selection=final_config['random'] if 'random' in final_config else None
    )



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
