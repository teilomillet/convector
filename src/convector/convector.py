import argparse
import json
import os
import urllib.parse
from tqdm import tqdm
from pathlib import Path

# Importing the FileHandler class
from .handlers import FileHandler  

class Convector:
    def __init__(self, file_path=None, output_file=None, conversation=False, output_dir=None):
        self.file_handler = FileHandler(file_path, conversation)
        self.output_file = output_file

        self.convector_root_dir = self.check_convector_root_dir()
        
        # Define the default output directory relative to the Convector root directory
        default_output_dir = os.path.join(self.convector_root_dir, 'silo')
        
        # If output_dir is not specified, use the default output directory
        if output_dir is None:
            self.output_dir = os.path.abspath(default_output_dir)
        else:
            self.output_dir = os.path.abspath(output_dir)

    def check_convector_root_dir(self):
        convector_root_dir = os.environ.get('CONVECTOR_ROOT_DIR')
        
        if not convector_root_dir:
            print("Warning: CONVECTOR_ROOT_DIR is not set.")
            print("You can set it by running: export CONVECTOR_ROOT_DIR=/path/to/your/convector/repository")
            
            # ASCII Art
            print("                 ___====-_  _-====___")
            print("           _--^^^#####//      \#####^^^--_")
            print("        _-^##########// (    ) \##########^-_")
            print("       -############//  |\^^/|  \############-")
            print("     _/############//   (@::@)   \############\_")
            print("    /#############((     \//\    ))#############\  ")
            print("   -###############\\    (oo) \   //###############-")
            print("  -#################\\  / UUU  \ //#################-")
            print(" -###################\\/  (v)   \/###################-")
            print("_#/|##########/\######(   /  \   )######/\##########|\#_")
            print("|/ |#/\#/\#/\/  \#/\#/\  (/|||\) /\#/\#/  \/\#/\#/\|  \|")
            print("`  |/  V  V  `   V  V /  ||(_)|| \ V  V    ' V  V  '")
            print("                    (ooo / / \ \ ooo)")
            print("                    `~  CONVECTOR  ~'")
            
            # Define the default directory
            # script_dir = os.path.dirname(__file__)
            # default_dir = os.path.join(script_dir, '..', '..', 'silo')
            default_dir = os.path.join(os.path.expanduser('~'), 'convector')
            print(f"\nIf you continue, CONVECTOR_ROOT_DIR will be set to the default path: {default_dir}")
            
            # Ask for user confirmation
            user_input = input("Do you want to continue with this directory? (y/n): ").strip().lower()
            
            if user_input == 'y':
                convector_root_dir = default_dir
                print(f"Continuing with CONVECTOR_ROOT_DIR set to {convector_root_dir}")
            else:
                print("Operation aborted.")
                exit()  # or return to the previous menu, depending on your flow
        
        return convector_root_dir
            
    def process_and_save(self, transformed_data_generator, total_lines=None, bytes=None, append=False):
        progress_bar=None # Initialize here
        output_file_path = None  # Initialize here
        lines_written = 0  # Initialize here
        total_bytes_written = 0  # Initialize here
        
        try:
            # If an output_file is specified, use it; otherwise, create a name based on the input file
            if self.output_file:
                output_file_path = os.path.join(self.output_dir, self.output_file)
            else:
                output_base_name = os.path.splitext(os.path.basename(self.file_handler.file_path))[0] + '_tr.jsonl'
                output_file_path = os.path.join(self.output_dir, output_base_name)
                
            # Check if the file exists and whether to append or overwrite
            if os.path.exists(output_file_path) and not append:
                user_input = input(f"File {output_file_path} already exists. Do you want to overwrite it? (y/n): ").strip().lower()
                if user_input != 'y':
                    print("Transformation aborted.")
                    return
            
            mode = 'a' if os.path.exists(output_file_path) and append else 'w'
            
            lines_written = 0
            total_bytes_written = 0  # Track the total bytes written to the file

            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)  # Create directories
            
            with open(output_file_path, mode, encoding='utf-8') as file:
                # Progress bar will show even if total_lines is not known
                total = bytes or total_lines
                unit = " bytes" if bytes else " lines"
                progress_bar = tqdm(total=total, unit=unit, position=0, desc="Processing")
                
                for items in transformed_data_generator:
                    for item in items:
                        if total_lines and lines_written >= total_lines:
                            # print("\nReached line limit. Stopping writing.")
                            return
                        
                        item['source'] = os.path.basename(self.file_handler.file_path)
                        json_line = json.dumps(item, ensure_ascii=False) + '\n'
                        line_bytes = len(json_line.encode('utf-8'))
                        
                        if bytes and (total_bytes_written + line_bytes) > bytes:
                            # print("\nReached byte limit. Stopping writing.")
                            return
                        
                        file.write(json_line)
                        total_bytes_written += line_bytes  # Update the total bytes written
                        lines_written += 1
                        progress_bar.update(1)
                
                progress_bar.close()
                
        except Exception as e:
            print(f"An error occurred while processing and saving the file: {e}")

        finally:
            # This block will execute whether an exception was raised or not
            if progress_bar:  # Check if progress_bar is not None
                progress_bar.close()
        
            # Create a clickable URL
            if output_file_path:  # Check if output_file_path is not None
                relative_path = os.path.relpath(output_file_path, start=Path.cwd())
                print(f"\nDelivered to file://{relative_path} \n({lines_written} lines, {total_bytes_written} bytes)")

    def transform(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, append=False):
            # Validating the existence of the input file
            if not os.path.exists(self.file_handler.file_path):
                print(f"Error: The file '{self.file_handler.file_path}' does not exist.")
                return
            
            # Identifying the file type based on its extension
            file_extension = os.path.splitext(self.file_handler.file_path)[-1].lower()

            print(f"File: {self.file_handler.file_path}")
            handler_method = getattr(self.file_handler, f"handle_{file_extension[1:]}", None)
            
            if handler_method:
                print(f"Charging .{file_extension[1:]} file...")
                transformed_data_generator = handler_method(input=input, output=output,
                                                            instruction=instruction, add=add,
                                                            lines=lines, bytes=bytes)
                
                # Processing and saving the transformed data considering the byte size limit
                self.process_and_save(transformed_data_generator, total_lines=lines, bytes=bytes, append=append)
            else:
                print(f"Error: Unsupported file extension '{file_extension}'")


# CLI execution
def main():
    parser = argparse.ArgumentParser(description='Transform conversational data to a unified format.')
    parser.add_argument('file_path', help='Path to the file to be processed.')
    parser.add_argument('--conversation', action='store_true', help='Specify if the data is in a conversational format.')
    parser.add_argument('--input', help='Key for user inputs in the data.')
    parser.add_argument('--output', help='Key for bot outputs in the data.')
    parser.add_argument('--instruction', help='Key for instructions or system messages in the data.')
    parser.add_argument('--add', help='Comma-separated column names to keep in the transformed data.')
    parser.add_argument('--lines', type=int, help='Number of lines to process from the file.')
    parser.add_argument('--bytes', type=int, help='Number of bytes to process from the file.')
    parser.add_argument('--output_file', help='Path to the output file where transformed data will be saved.')
    parser.add_argument('--output_dir', help='Specify a custom directory where the output file will be saved.')
    parser.add_argument('--append', action='store_true', help='Specify whether to append to or overwrite an existing file.')

    args = parser.parse_args()   
    convector = Convector(args.file_path, args.output_file, args.conversation, args.output_dir)
    convector.transform(input=args.input, output=args.output, instruction=args.instruction, 
                        add=args.add, lines=args.lines, bytes=args.bytes, append=args.append)

if __name__ == "__main__":
    main()