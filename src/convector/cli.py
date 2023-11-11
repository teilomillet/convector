import click
import yaml
import logging
import re
import logging.config
from pathlib import Path
from typing import Optional, List



from convector.core.directory_processor import DirectoryProcessor
from convector.core.profile import Profile, FilterCondition
from convector.core.convector_config import ConvectorConfig
from convector.core.user_interaction import UserInteraction
from .convector import Convector


PERSISTENT_CONFIG_PATH = Path.home() / '.convector_config'

class ConfigurationError(Exception):
    """Exception raised for errors in the configuration."""
    pass

class ProcessingError(Exception):
    """Exception raised during processing of the conversational data."""
    pass

def setup_logging(default_path: str = None, default_level=logging.INFO):
    """Setup logging configuration from a YAML file."""
    if default_path is None:
        if PERSISTENT_CONFIG_PATH.exists():
            with open(PERSISTENT_CONFIG_PATH, 'r') as file:
                convector_root_dir = file.read().strip()
                default_path = Path(convector_root_dir) / 'config.yaml'
        else:
            config = ConvectorConfig()
            default_path = Path(config.convector_root_dir) / 'config.yaml'
    
    try:
        with open(default_path, 'rt') as file:
            config = yaml.safe_load(file)
            logging.config.dictConfig(config)
    except FileNotFoundError:
        logging.warning(f"Logging configuration file is not found at '{default_path}'. Using default configs.")
        logging.basicConfig(level=default_level)

class ConvectorFactory:
    @staticmethod
    def create_from_profile(profile: Profile, file_path):
        convector = Convector(profile=profile, file_path=file_path, user_interaction=UserInteraction())
        return convector

class DirectoryProcessorFactory:
    @staticmethod
    def create_from_profile(profile, file_path):
        try:
            is_conversation = profile.is_conversation

            return DirectoryProcessor(
                directory_path=str(file_path),
                profile=profile,
                is_conversation=is_conversation,
            )
        except AttributeError as e:
            logging.error(f"Attribute error in DirectoryProcessorFactory: {e}")

def echo_info(message: str) -> None:
    click.echo(click.style(message, fg='green'))

def process_conversational_data(file_path: Path, profile: Profile) -> None:
    if file_path.is_dir():
        directory_processor = DirectoryProcessorFactory.create_from_profile(profile, file_path)
        directory_processor.process_directory()
        directory_processor.print_summary()
    elif file_path.is_file():
        UserInteraction.show_message(f"Processing file: {file_path}")
        convector = Convector(profile, UserInteraction(), file_path)
        convector.process()
    else:
        raise ProcessingError("The path provided is neither a file nor a directory.")
    
def parse_filter_conditions(filter_conditions: str) -> List[FilterCondition]:
    """
    Parses the filter conditions string into a list of FilterCondition objects.
    """
    filter_objs = []
    for condition in filter_conditions.split(';'):
        match = re.match(r"([\w\.]+)(!=|==|=|<|>|<=>)?(.*)", condition.strip())
        if match:
            field, operator, value = match.groups()
            filter_objs.append(FilterCondition(field=field, operator=operator, value=value))
    return filter_objs

@click.group()
def convector():
    """
    Convector - A tool for transforming conversational data to a unified format.
    For more detailed information and examples, use --verbose.
    """
    try:
        config = UserInteraction.setup_environment()

        ctx = click.get_current_context()
        ctx.obj = config

        echo_info("Convector is ready to use.")

    except Exception as e:
        click.echo(f"An error occurred during setup: {e}")
        exit(1)
   
@convector.command()
@click.pass_context  # This decorator allows us to pass the Click context into the command function
@click.argument('file_path', type=click.Path(exists=True), metavar='<file_path>')
@click.option('-p', '--profile', default='default', help='Use a predefined profile from the config.')
@click.option('-c', '--conversation', 'is_conversation', is_flag=True, help='Treat the data as conversational exchanges.')
@click.option('--instruction', help='Key for instructions or system messages in the data.')
@click.option('-i', '--input-key', 'input', help='Key for user inputs.')
@click.option('-o', '--output-key', 'output', help='Key for bot responses.')
@click.option('-s', '--schema', 'output_schema', default=None, help='Schema for output data.')
@click.option('--filter', 'filters', help='Filter conditions in the format "field,operator,value".')
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
            lines: Optional[int],
            filters: Optional[str],
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
    
    Example:
        convector process <file_path> -c -i message -o response
        convector process <file_path> --profile conversation -l 100 -f output.json

    """
    config = ctx.obj

    try:
        selected_profile_name = profile if profile else 'default'
        # Check if the profile exists, if not, initialize it
        if profile not in config.profiles:
            config.profiles[profile] = Profile()

        selected_profile = config.profiles[profile]

        # Now, since we've ensured the profile exists, we can update it with CLI args
        cli_args = {
            'is_conversation': is_conversation,
            'input': input,
            'output': output,
            'instruction': instruction,
            'lines': lines,
            'bytes': bytes,
            'output_file': output_file,
            'output_dir': output_dir,
            'append': append,
            'verbose': verbose,
            'random': random,
            'output_schema': output_schema,
        }

        # Use update_from_cli to set the profile attributes
        config.update_from_cli(profile=selected_profile_name, **cli_args)

        # Save the updated profile
        if selected_profile_name != 'default':
            config.save_profile_to_yaml(selected_profile_name)

        if filters:
            filters = parse_filter_conditions(filters)
            selected_profile.filters = filters

        # Proceed with data processing
        process_conversational_data(Path(file_path), selected_profile)

    except ConfigurationError as e:
        UserInteraction.show_message(f"Configuration Error: {e}", "error")
        exit(1)
    except ProcessingError as e:
        UserInteraction.show_message(f"Processing Error: {e}", "error")
        exit(1)
    else:
        UserInteraction.show_message("Processing completed.", "info")

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
        config = ctx.obj
        config.set_and_save(key, value)
        echo_info(f"Configuration updated: {key} = {value}")
    except Exception as e:
        click.error(f"An error occurred while updating the configuration: {e}")

if __name__ == "__main__":
    try:
        app_config = UserInteraction.setup_environment(ConvectorConfig)
        setup_logging()
        convector()
    except Exception as e:
        logging.error(f"An error occurred during setup: {e}")
        exit(1)