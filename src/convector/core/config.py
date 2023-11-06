import yaml
import logging
import os
from pydantic import BaseSettings, Field, validator, ValidationError, root_validator
from typing import Optional, Dict, Any
from pathlib import Path

# Configure logging for the application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class Profile(BaseSettings):
    """
    Represents a set of configurations for a particular processing profile.
    Attributes can be overridden by CLI arguments or environment variables.
    """
    output_dir: str = str(Path.home() / 'convector' / 'silo')  # Default directory for output files
    is_conversation: bool = False  # Flag to indicate if the data is conversational
    input: Optional[str] = None  # Key for user inputs
    output: Optional[str] = None  # Key for bot responses
    instruction: Optional[str] = None  # Instruction/message key
    additional_fields: Optional[str] = None  # Additional fields to be included
    lines: Optional[int] = None  # Line limit for processing
    bytes: Optional[int] = None  # Byte limit for processing
    append: bool = False  # Flag to append to an existing file
    verbose: bool = False  # Verbose logging
    random_selection: bool = False  # Random data selection
    output_file: Optional[str] = None # Output file name
    output_schema: Optional[str] = 'default'  # Output schema name
    # Additional fields can be added as required

    @validator('output_dir', pre=True, always=True)
    def validate_output_dir(cls, v):
        """
        Validates that the output directory exists and is writable.
        If it does not exist, attempts to create it.
        """
        path = Path(v)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir() or not os.access(path, os.W_OK):
            raise ValueError('output_dir must be a writable directory')
        return str(path)
    
class ConvectorConfig(BaseSettings):
    """
    Holds the global configuration for Convector, including version and profiles.
    Provides methods to load configurations from a YAML file and update settings via CLI.
    """
    version: float = 1.0  # Configuration version
    profiles: Dict[str, Profile] = Field(default_factory=lambda: {'default': Profile()}) # Profiles dictionary
    convector_root_dir: str = str(Path.home() / 'convector') # Default root directory
    default_profile: str = 'default'

    def get_active_profile(self) -> Profile:
        logging.debug(f"Current profiles: {self.profiles}")
        logging.debug(f"Default profile key: {self.default_profile}")
        profile = self.profiles.get(self.default_profile, Profile())
        if not isinstance(profile, Profile):
            logging.error("Active profile is not an instance of Profile. Check configuration.")
        return profile

    
    def set_and_save(self, key: str, value: Any):
        """
        Sets a configuration value based on a key and saves the updated configuration.

        Args:
            key (str): The configuration key to update.
            value (Any): The new value for the key.
        """
        # Convert the string to a dictionary key path (nested keys)
        keys = key.split('.')
        cfg = self.dict()

        # Update the configuration recursively
        sub_cfg = cfg
        for k in keys[:-1]:  # Traverse the nested dictionaries except for the last key
            sub_cfg = sub_cfg.setdefault(k, {})  # Get the nested dict, or create an empty one

        # Set the value for the final key
        sub_cfg[keys[-1]] = value

        # Load the updated dictionary into the Pydantic model
        self.__dict__.update(**cfg)

        # Save the updated configuration to YAML
        self.save_to_yaml()

        # Log the update
        logging.info(f"Configuration '{key}' set to '{value}'. Configuration saved.")

    class Config:
        env_prefix = 'CONVECTOR_' # Prefix for environment variables to update the settings

    @classmethod
    def create_default_config(cls, config_path: Path):
        if config_path.exists():
            logging.info(f"Default configuration file already exists at: {config_path}")
            return

        default_config = cls()
        try:
            with open(config_path, 'w') as file:
                yaml.dump(default_config.dict(), file)
            logging.info(f"Created default configuration file at: {config_path}")
        except Exception as e:
            logging.error(f"Failed to create default configuration file at {config_path}: {e}")
            raise e
        
    @classmethod
    def from_yaml(cls, file_path: str, profile_name: str = 'default'):
        """
        Loads configuration from a YAML file and returns a ConvectorConfig instance.
        Handles YAML syntax errors and unexpected issues during file reading.
        """
        try:
            with open(file_path, 'r') as file:
                config_data = yaml.safe_load(file)
            profiles = config_data.get('profiles', {})
            selected_profile = profiles.get(profile_name, {})
            return cls(profiles={profile_name: Profile(**selected_profile)})
        except yaml.YAMLError as e:
            logging.error(f'YAML syntax error: {e}')
            raise
        except Exception as e:
            logging.error(f'Unexpected error: {e}')
            raise
    
    def update_from_cli(self, profile: str, **cli_args):
        """
        Updates the configuration with values from CLI arguments.

        Args:
            profile (str): The name of the profile to update.
            **cli_args: Command-line arguments passed as keyword arguments.
        """
        # Check if the profile exists, if not create a new one with default settings
        active_profile = self.profiles.get(profile, Profile())

        

        # Apply CLI arguments to the active profile
        for arg_name, arg_value in cli_args.items():
            # Pydantic models throw errors if you try to set an attribute that isn't defined
            # as a field, so we need to ensure only defined fields are updated.
            if arg_value is not None and hasattr(active_profile, arg_name):
                setattr(active_profile, arg_name, arg_value)

        # Update the profiles dictionary with the new or updated profile
        self.profiles[profile] = active_profile

        # Save the updated configuration to the YAML file
        self.save_to_yaml()

    def save_to_yaml(self):
        """Save the current configuration to the YAML file."""
        try:
            with open(self.get_config_file_path(), 'w') as file:
                yaml.safe_dump(self.dict(), file)
            logging.info(f"Configuration saved to {self.get_config_file_path()}")
        except Exception as e:
            logging.error(f"Failed to save configuration to {self.get_config_file_path()}: {e}")
            raise e

    def get_config_file_path(self):
        """Return the expected path of the configuration YAML file."""
        return Path(self.convector_root_dir) / 'config.yaml'
    