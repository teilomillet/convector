from pydantic import BaseSettings
from typing import Dict, Any
from .profile import Profile
import yaml
import logging
from pathlib import Path

PERSISTENT_CONFIG_PATH = Path.home() / '.convector_config'

class ConvectorConfig(BaseSettings):
    """Class attributes with default values"""
    version: float = 1.0  
    profiles: Dict[str, Profile] = {} 
    convector_root_dir: str = str(Path.home() / 'convector') 
    default_profile: str = 'default'

    def get_active_profile(self) -> Profile:
        """Retrieve the currently active profile"""
        return self.retrieve_profile(self.default_profile)

    def retrieve_profile(self, profile_name: str) -> Profile:
        """Retrieve a specific profile by name, returning a default Profile instance if not found"""
        profile = self.profiles.get(profile_name, Profile())
        self.validate_profile_instance(profile)
        return profile

    @staticmethod
    def validate_profile_instance(profile):
        """Ensure the provided profile is an instance of the Profile class"""
        if not isinstance(profile, Profile):
            logging.error("Provided profile is not an instance of Profile. Check configuration.")
            raise TypeError("Invalid profile instance")

    def save_to_yaml(self):
        """Save the current configuration to a YAML file"""
        config_path = self.get_config_file_path()
        self.write_config_to_yaml(config_path, self.compile_profiles_data())

    def compile_profiles_data(self) -> Dict[str, Any]:
        """Compile and merge current and updated profiles data"""
        current_config = self.read_current_config()
        updated_profiles = {name: profile.dict() for name, profile in self.profiles.items()}
        current_profiles = current_config.get('profiles', {})
        merged_profiles = {**current_profiles, **updated_profiles}
        current_config['profiles'] = merged_profiles
        return current_config

    def write_config_to_yaml(self, config_path: Path, config_data: Dict[str, Any]):
        """ Write configuration data to a YAML file"""
        try:
            with open(config_path, 'w') as file:
                yaml.safe_dump(config_data, file)
        except Exception as e:
            logging.error(f"Failed to save configuration to {config_path}: {e}")
            raise

    def read_current_config(self) -> Dict[str, Any]:
        """ Read the current configuration from a YAML file"""
        config_path = self.get_config_file_path()
        if config_path.exists():
            with open(config_path, 'r') as file:
                return yaml.safe_load(file) or {}
        return {}

    def save_profile_to_yaml(self, profile_name: str):
        """ Save a specific profile to the YAML configuration file"""
        config_path = self.get_config_file_path()
        current_config = self.read_current_config()
        current_config['profiles'][profile_name] = self.profiles[profile_name].dict()
        self.write_config_to_yaml(config_path, current_config)

    @classmethod
    def from_yaml(cls, file_path: str):
        """ Create an instance of ConvectorConfig from a YAML file"""
        config_data = cls.load_config_data(file_path)
        profiles = cls.extract_profiles_from_data(config_data)
        return cls(profiles=profiles)

    @staticmethod
    def load_config_data(file_path: str) -> Dict[str, Any]:
        """ Load configuration data from a YAML file"""
        try:
            with open(file_path, 'r') as file:
                return yaml.safe_load(file) or {}
        except Exception as e:
            logging.error(f"Error loading configuration from {file_path}: {e}")
            raise

    @staticmethod
    def extract_profiles_from_data(config_data: Dict[str, Any]) -> Dict[str, Profile]:
        """ Extract profile data from configuration data and create Profile instances"""
        profiles = {}
        for name, data in config_data.get('profiles', {}).items():
            profiles[name] = Profile(**data)
        if 'default' not in profiles:
            profiles['default'] = Profile()
        return profiles

    def update_from_cli(self, profile: str, **cli_args):
        """ Update or create a profile from CLI arguments"""
        self.create_or_update_profile(profile, **cli_args)
        if profile != 'default':
            self.save_to_yaml()

    def create_or_update_profile(self, profile: str, **cli_args):
        """ Create a new profile or update an existing one with given CLI arguments"""
        if profile not in self.profiles:
            self.profiles[profile] = Profile()
        active_profile = self.profiles[profile]
        self.set_profile_attributes(active_profile, cli_args)

    @staticmethod
    def set_profile_attributes(profile, cli_args):
        """ Set attributes of a profile based on provided CLI arguments"""
        for arg_name, arg_value in cli_args.items():
            if arg_value is not None and hasattr(profile, arg_name):
                setattr(profile, arg_name, arg_value)

    def get_config_file_path(self) -> Path:
        """ Get the path to the configuration file"""
        convector_root_dir = self.read_convector_root_dir()
        return Path(convector_root_dir) / 'config.yaml'

    def read_convector_root_dir(self) -> str:
        """ Read the Convector root directory from a persistent file or use the default"""
        if PERSISTENT_CONFIG_PATH.exists():
            with open(PERSISTENT_CONFIG_PATH, 'r') as file:
                return file.read().strip()
        else:
            return self.convector_root_dir
