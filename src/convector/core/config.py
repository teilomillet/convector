import yaml
import logging
import os
from pydantic import BaseSettings, Field, validator, ValidationError, root_validator
from typing import Optional, Dict, Any, List
from pathlib import Path

PERSISTENT_CONFIG_PATH = Path.home() / '.convector_config'

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
    labels: Optional[List[str]] = []  # Define 'add' as a list of strings  # Additional fields to be included
    lines: Optional[int] = None  # Line limit for processing
    bytes: Optional[int] = None  # Byte limit for processing
    append: bool = False  # Flag to append to an existing file
    verbose: bool = False  # Verbose logging
    random: bool = False  # Random data selection
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
    version: float = 1.0  
    profiles: Dict[str, Profile] = {} 
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
        keys = key.split('.')
        cfg = self.dict()

        sub_cfg = cfg
        for k in keys[:-1]:  
            sub_cfg = sub_cfg.setdefault(k, {})  

        sub_cfg[keys[-1]] = value

        self.__dict__.update(**cfg)

        self.save_to_yaml()

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
    def from_yaml(cls, file_path: str):
        try:
            with open(file_path, 'r') as file:
                config_data = yaml.safe_load(file) or {}
            
            profiles = {}
            for name, data in config_data.get('profiles', {}).items():
                profiles[name] = Profile(**data)

            if 'default' not in profiles:
                profiles['default'] = Profile()

            return cls(profiles=profiles)

        except Exception as e:
            logging.error(f"An error occurred while loading the configuration: {e}")
            raise

    def update_from_cli(self, profile: str, **cli_args):
        """
        Updates the configuration with values from CLI arguments.
        If the profile does not exist, it is created with the provided arguments.
        If the profile is 'default', changes are not saved to the YAML file.
        """
        if profile not in self.profiles:
            self.profiles[profile] = Profile()

        active_profile = self.profiles[profile]

        for arg_name, arg_value in cli_args.items():
            if arg_value is not None and hasattr(active_profile, arg_name):
                setattr(active_profile, arg_name, arg_value)

        if profile != 'default':
            self.save_to_yaml()


    def save_to_yaml(self):
        """Save the profiles configuration to the YAML file, merging with existing profiles."""
        try:
            config_path = self.get_config_file_path()
            current_config = {}

            if config_path.exists():
                with open(config_path, 'r') as file:
                    current_config = yaml.safe_load(file) or {}

            updated_profiles = {name: profile.dict() for name, profile in self.profiles.items()}
            current_profiles = current_config.get('profiles', {})
            merged_profiles = {**current_profiles, **updated_profiles}

            current_config['profiles'] = merged_profiles

            with open(config_path, 'w') as file:
                yaml.safe_dump(current_config, file)

            logging.info(f"Profiles configuration saved to {config_path}")
        except Exception as e:
            logging.error(f"Failed to save profiles configuration to {config_path}: {e}")
            raise e
        
    def save_profile_to_yaml(self, profile_name: str):
        """Save the updated profile to the YAML file."""
        try:
            config_path = self.get_config_file_path()
            current_config = {}

            if config_path.exists():
                with open(config_path, 'r') as file:
                    current_config = yaml.safe_load(file) or {}

            current_config['profiles'][profile_name] = self.profiles[profile_name].dict()

            with open(config_path, 'w') as file:
                yaml.safe_dump(current_config, file)

            logging.info(f"Profile '{profile_name}' configuration saved to {config_path}")
        except Exception as e:
            logging.error(f"Failed to save profile '{profile_name}' configuration to {config_path}: {e}")
            raise e

    def get_config_file_path(self):
        """Return the expected path of the configuration YAML file."""
        if PERSISTENT_CONFIG_PATH.exists():
            with open(PERSISTENT_CONFIG_PATH, 'r') as file:
                convector_root_dir = file.read().strip()
        else:
            convector_root_dir = self.convector_root_dir

        return Path(convector_root_dir) / 'config.yaml'
    