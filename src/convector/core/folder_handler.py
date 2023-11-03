# core/folder_handler.py
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..convector import Convector

def process_folder(folder_path, config):
    """
    Process each file within a folder.

    Parameters:
    - folder_path (str): The path to the folder containing files to be processed.
    - config (dict): The configuration parameters for file processing.
    """
    folder = Path(folder_path)
    files = [file for file in folder.glob('*') if file.is_file()]

    with ThreadPoolExecutor(max_workers=config.get('max_workers', 5)) as executor:
        # Map each file to the executor, which will call `process_file` on each
        futures = {executor.submit(process_file, file, config): file for file in files}
        for future in as_completed(futures):
            file = futures[future]
            try:
                future.result()  # If you need the result, you can use it here
            except Exception as exc:
                print(f'{file} generated an exception: {exc}')

def process_file(file_path, config):
    """
    Process a single file with the provided configuration.
    """
    # Assuming 'config' dictionary has all necessary keys to pass to Convector's init
    convector_instance = Convector(
        config=config,
        user_interaction=None,  # Assuming you have a default or it's optional
        file_path=file_path,  # The current file to process
        conversation=config.get('conversation', False),  # Defaulting to False if not provided
        output_schema=config.get('output_schema', 'default'),  # Default schema if not provided
        output_file=config.get('output_file'),  # Output file if specified
        output_dir=config.get('output_dir', 'silo'),  # Output directory with default 'silo'
    )
    
    # Call the process method with the appropriate arguments
    convector_instance.process(
        input=config.get('input'),
        output=config.get('output'),
        instruction=config.get('instruction'),
        add=config.get('add'),
        lines=config.get('lines'),
        bytes=config.get('bytes'),
        append=config.get('append'),
        random_selection=config.get('random'),
    )