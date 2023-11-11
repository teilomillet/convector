from pydantic import BaseSettings, Field, validator
from typing import Optional, List
from pathlib import Path
import os

class FilterCondition(BaseSettings):
    field: str
    operator: Optional[str] = None
    value: Optional[str] = None

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
    filters : Optional[List[FilterCondition]] = []
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
        path = Path(v)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir() or not os.access(path, os.W_OK):
            raise ValueError('output_dir must be a writable directory')
        return str(path)