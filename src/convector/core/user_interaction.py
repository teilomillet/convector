import click
import os
import logging
from pathlib import Path
from convector.core.config import ConvectorConfig

class UserInteraction:

    @staticmethod
    def setup_environment(config_class=ConvectorConfig):
        # Check for the default convector directory
        convector_dir = Path.home() / 'convector'
        if not convector_dir.exists():
            if UserInteraction.confirm_action(f"The default directory {convector_dir} does not exist. Do you want to create it?"):
                convector_dir.mkdir(parents=True, exist_ok=True)
                UserInteraction.show_message(f"Created the directory {convector_dir}.")
            else:
                convector_dir = Path(UserInteraction.prompt_for_input("Please specify the directory for Convector:", type=Path))
                if not convector_dir.exists():
                    convector_dir.mkdir(parents=True, exist_ok=True)
                    UserInteraction.show_message(f"Created the directory {convector_dir}.")

        # Set the new convector root directory in configuration
        config = config_class()
        config.convector_root_dir = str(convector_dir)
        config.save_to_yaml()  # Save the new configuration immediately

        # Check for the default config file
        config_path = convector_dir / 'config.yaml'
        if not config_path.exists():
            if UserInteraction.confirm_action(f"The configuration file does not exist in {convector_dir}. Do you want to create a new one with default settings?"):
                config.create_default_config(config_path)
                UserInteraction.show_message(f"Created the configuration file {config_path}.")
            else:
                config_path = Path(UserInteraction.prompt_for_input("Please specify the path to the configuration file:", type=Path))
                if not config_path.exists():
                    config_class.create_default_config(config_path)
                    UserInteraction.show_message(f"Created the configuration file {config_path}.")

        # Load and return the configuration
        return config_class.from_yaml(str(config_path))

    @staticmethod
    def confirm_action(prompt, default=False):
        response = click.confirm(prompt, default=default)
        logging.info(f"User response to confirm action '{prompt}': {response}")
        return response

    @staticmethod
    def prompt_for_input(prompt, default=None, type=None):
        response = click.prompt(prompt, default=default, type=type)
        logging.info(f"User provided input for '{prompt}': {response}")
        return response

    @staticmethod
    def show_message(message, message_type="info"):
        if message_type == "info":
            click.echo(click.style(message, fg='green'))
        elif message_type == "warning":
            click.echo(click.style(message, fg='yellow'))
        elif message_type == "error":
            click.echo(click.style(message, fg='red', bold=True))

    @staticmethod
    def display_ascii_art():
        # Your ASCII art display logic here
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
    def display_progress(iterable, length, label="Processing"):
        with click.progressbar(iterable, length=length, label=label) as bar:
            for item in bar:
                yield item