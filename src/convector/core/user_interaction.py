import click
import os
import logging
from pathlib import Path
from convector.core.config import ConvectorConfig

PERSISTENT_CONFIG_PATH = Path.home() / '.convector_config'

class UserInteraction:

    @staticmethod
    def setup_environment(config_class=ConvectorConfig):
        if PERSISTENT_CONFIG_PATH.exists():
            with open(PERSISTENT_CONFIG_PATH, 'r') as file:
                convector_dir = Path(file.read().strip())
        else:
            convector_dir = UserInteraction.prompt_for_convector_directory()

        config = config_class()
        config.convector_root_dir = str(convector_dir)
        config.save_to_yaml()  

        config_path = convector_dir / 'config.yaml'
        return config_class.from_yaml(str(config_path))

    @staticmethod
    def prompt_for_convector_directory():
        suggested_dir = Path.home() / 'convector'
        UserInteraction.display_ascii_art()
        
        user_input = input(f"Enter the directory for Convector (press Enter for default: {suggested_dir}): ").strip()
        
        if user_input.lower() in ['yes', 'y', '']:
            convector_dir = suggested_dir
        else:
            convector_dir = Path(user_input)
        
        if not convector_dir.exists():
            try:
                convector_dir.mkdir(parents=True, exist_ok=True)
                UserInteraction.show_message(f"Created the directory {convector_dir}.")
            except Exception as e:
                UserInteraction.show_message(f"Could not create the directory {convector_dir}: {e}", "error")
                exit(1)
        
        with open(PERSISTENT_CONFIG_PATH, 'w') as file:
            file.write(str(convector_dir))
        
        return convector_dir

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
        with click.progressbar(iterable, length=length, label=label) as bar:
            for item in bar:
                yield item