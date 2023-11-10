from pydantic import BaseSettings
from typing import Dict, Any
from .profile import Profile
import yaml
import logging
from pathlib import Path

PERSISTENT_CONFIG_PATH = Path.home() / '.convector_config'

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

    def get_config_file_path(self):
        """Return the expected path of the configuration YAML file."""
        if PERSISTENT_CONFIG_PATH.exists():
            with open(PERSISTENT_CONFIG_PATH, 'r') as file:
                convector_root_dir = file.read().strip()
        else:
            convector_root_dir = self.convector_root_dir

        return Path(convector_root_dir) / 'config.yaml'