import os
from pathlib import Path

def get_output_file_path(profile, file_handler):
    """
    Generate the output file path based on the given profile settings and input file path.
    """
    if hasattr(profile, 'output_file') and profile.output_file:
        output_file_path = Path(profile.output_dir) / profile.output_file
    else:
        input_path = Path(file_handler.file_path)
        output_base_name = input_path.stem + '_tr.jsonl'
        output_file_path = Path(profile.output_dir) / output_base_name

    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    return output_file_path.resolve()

def display_results(output_file_path, lines_written, total_bytes_written):
    """
    Display the results of the file processing to the user.
    """
    absolute_path = Path(output_file_path).resolve()
    print(f"\nDelivered to file://{absolute_path} \n({lines_written} lines, {total_bytes_written} bytes)")
