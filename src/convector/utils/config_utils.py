import yaml
import logging
from pathlib import Path
from convector.core.convector_config import ConvectorConfig

PERSISTENT_CONFIG_PATH = Path.home() / '.convector_config'

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