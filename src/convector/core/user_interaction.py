import click
import os
import logging
from pathlib import Path
from convector.core.convector_config import ConvectorConfig

PERSISTENT_CONFIG_PATH = Path.home() / '.convector_config'

class UserInteraction:

    @staticmethod
    def setup_environment(config_class=ConvectorConfig):
        """ Set up the environment by defining the root directory for Convector"""
        convector_dir = UserInteraction.get_convector_directory()
        config = config_class()
        config.convector_root_dir = str(convector_dir)
        config.save_to_yaml()

        config_path = convector_dir / 'config.yaml'
        return config_class.from_yaml(str(config_path))

    @staticmethod
    def get_convector_directory():
        """ Get the Convector directory, either by reading from persistent config or prompting the user"""
        if PERSISTENT_CONFIG_PATH.exists():
            return UserInteraction.read_convector_directory()
        else:
            return UserInteraction.prompt_for_convector_directory()

    @staticmethod
    def read_convector_directory():
        """ Read the Convector directory path from a persistent file"""
        try:
            with open(PERSISTENT_CONFIG_PATH, 'r') as file:
                return Path(file.read().strip())
        except Exception as e:
            logging.error(f"Error reading Convector directory: {e}")
            raise

    @staticmethod
    def prompt_for_convector_directory():
        """ Prompt the user for the Convector directory, offering a default"""
        suggested_dir = Path.home() / 'convector'
        UserInteraction.display_ascii_art()

        user_input = input(f"Enter the directory for Convector (press Enter for default: {suggested_dir}): ").strip()
        convector_dir = Path(user_input) if user_input else suggested_dir
        UserInteraction.ensure_directory_exists(convector_dir)
        UserInteraction.save_convector_directory(convector_dir)
        return convector_dir

    @staticmethod
    def ensure_directory_exists(directory):
        """ Ensure the specified directory exists, create if it doesn't"""
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logging.error(f"Could not create the directory {directory}: {e}")
            raise

    @staticmethod
    def save_convector_directory(directory):
        """ Save the Convector directory path to a persistent file"""
        try:
            with open(PERSISTENT_CONFIG_PATH, 'w') as file:
                file.write(str(directory))
        except Exception as e:
            logging.error(f"Could not save Convector directory: {e}")
            raise

    @staticmethod
    def confirm_action(prompt, default=False):
        """ Confirm an action with the user, logging the response"""
        response = click.confirm(prompt, default=default)
        logging.info(f"User response to confirm action '{prompt}': {response}")
        return response

    @staticmethod
    def prompt_for_input(prompt, default=None, type=None):
        """ Prompt the user for input, logging the response"""
        response = click.prompt(prompt, default=default, type=type)
        logging.info(f"User provided input for '{prompt}': {response}")
        return response

    @staticmethod
    def show_message(message, message_type="info"):
        """ Display a styled message to the user"""
        if message_type == "info":
            click.echo(click.style(message, fg='green'))
        elif message_type == "warning":
            click.echo(click.style(message, fg='yellow'))
        elif message_type == "error":
            click.echo(click.style(message, fg='red', bold=True))

    @staticmethod
    def display_ascii_art():
        """Display ASCII art."""
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
        print("                              `~  CONVECTOR  ~'")
        print("                                                                             ")

    @staticmethod
    def display_progress(iterable, length, label="Processing"):
        """ Display a progress bar for an iterable process"""
        with click.progressbar(iterable, length=length, label=label) as bar:
            for item in bar:
                yield item