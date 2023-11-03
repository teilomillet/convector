from pathlib import Path

def detect_path_type(input_path):
    """
    Detect whether the given path is a file or a folder.
    
    Parameters:
    - input_path (str): The path to be checked.
    
    Returns:
    - str: 'file' if it's a file, 'folder' if it's a folder, and 'unknown' otherwise.
    """
    path = Path(input_path)
    if path.is_file():
        return 'file'
    elif path.is_dir():
        return 'folder'
    else:
        return 'unknown'

# Example usage:
# path_type = detect_path_type("/some/path/to/file_or_folder")
