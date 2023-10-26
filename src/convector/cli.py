import click
import os
import yaml

# Importing necessary classes and functions
from .convector import Convector, ConfigurationHandler, UserInteraction, FileHandler


CONFIG_FILE = 'convector_config.yaml'


@click.group()
def convector():
    """
    Convector - A tool for transforming conversational data to a unified format.
    """
    pass


@convector.command()
@click.argument('file_path', type=click.Path(exists=True))
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
def transform(file_path, conversation, input, output, instruction, add, lines, bytes, output_file, output_dir, append, verbose, random):
    """
    Transform conversational data in FILE_PATH to a unified format.
    """
    config_handler = ConfigurationHandler(CONFIG_FILE)
    user_interaction = UserInteraction()
    file_handler = FileHandler(file_path, conversation)

    convector = Convector(config_handler, user_interaction, file_handler, output_file, output_dir)
    convector.transform(input=input, output=output, instruction=instruction, add=add, lines=lines, bytes=bytes, append=append, random_selection=random)


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
