import json
import csv
import pandas as pd
import polars as pl
import zstandard as zstd
import io
import os
import random

class FileHandler:
    def __init__(self, file_path, conversation=False):
        self.file_path = file_path
        self.conversation = conversation
        self.printed = False

    def transform_data(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None):
        file_extension = os.path.splitext(self.file_path)[-1].lower()
        handler_method = getattr(self, f"handle_{file_extension[1:]}", None)

        if handler_method:
            yield from handler_method(input=input, output=output, instruction=instruction, add=add, lines=lines, bytes=bytes)
        else:
            print(f"Error: Unsupported file extension '{file_extension}'")


    # JSON
    def handle_json(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None):
        total_bytes = 0  # Counter to keep track of total bytes processed
        
        try:
            with open(self.file_path, 'r') as file:
                for i, line in enumerate(file):
                    if lines and i >= lines:
                        break  # Stop processing if num_lines is reached
                    
                    original_data = json.loads(line)
                    transformed_item = self.handle_data(original_data, input, output, instruction, add)
                    json_line = json.dumps(transformed_item, ensure_ascii=False)
                    line_bytes = len(json_line.encode('utf-8'))
                    
                    # Check if the bytes limit is reached, if a limit is set
                    if bytes and total_bytes + line_bytes > bytes:
                        break
                    
                    total_bytes += line_bytes  # Update the total bytes counter
                    yield transformed_item  # Yield the transformed item for writing
                    
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON on line {i+1}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while handling the JSON file: {e}")


    # JSONL
    def handle_jsonl(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None):
        total_bytes = 0  # Counter to keep track of total bytes processed
        
        try:
            with open(self.file_path, 'r') as file:
                for i, line in enumerate(file):
                    if lines and i >= lines:
                        break  # Stop processing if num_lines is reached
                    
                    original_data = json.loads(line)
                    transformed_item = self.handle_data(original_data, input, output, instruction, add)
                    json_line = json.dumps(transformed_item, ensure_ascii=False)
                    line_bytes = len(json_line.encode('utf-8'))
                    
                    # Check if the bytes limit is reached, if a limit is set
                    if bytes and total_bytes + line_bytes > bytes:
                        break
                    
                    total_bytes += line_bytes  # Update the total bytes counter
                    yield transformed_item  # Yield the transformed item for writing
                    
        except Exception as e:
            print(f"An error occurred while handling the JSONL file: {e}")

    # CSV
    def handle_csv(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None):
        total_bytes = 0  # Counter to keep track of total bytes processed
        
        try:
            with open(self.file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for i, row in enumerate(reader):
                    if lines and i >= lines:
                        break  # Stop processing if num_lines is reached
                    
                    transformed_item = self.handle_data(row, input, output, instruction, add)
                    json_line = json.dumps(transformed_item, ensure_ascii=False)
                    line_bytes = len(json_line.encode('utf-8'))
                    
                    # Check if the bytes limit is reached, if a limit is set
                    if bytes and total_bytes + line_bytes > bytes:
                        break
                    
                    total_bytes += line_bytes  # Update the total bytes counter
                    yield transformed_item  # Yield the transformed item for writing
                    
        except Exception as e:
            print(f"An error occurred while handling the CSV file: {e}")


    # PARQUET
    def handle_parquet(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None):
        total_bytes = 0  # Counter to keep track of total bytes processed
        
        try:
            df = pd.read_parquet(self.file_path)  # Load the entire Parquet file
            
            # If a line limit is specified, truncate the DataFrame
            if lines:
                df = df.head(lines)
            
            for i, row in df.iterrows():
                row_dict = row.to_dict()
                transformed_item = self.handle_data(row_dict, input, output, instruction, add)
                json_line = json.dumps(transformed_item, ensure_ascii=False)
                line_bytes = len(json_line.encode('utf-8'))
                
                # Check if the bytes limit is reached, if a limit is set
                if bytes and total_bytes + line_bytes > bytes:
                    break
                
                total_bytes += line_bytes  # Update the total bytes counter
                yield transformed_item  # Yield the transformed item for writing
                
        except Exception as e:
            print(f"An error occurred while handling the Parquet file: {e}")



    # ZST
    def handle_zst(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None):
        total_bytes = 0  # Counter to keep track of total bytes processed
        
        try:
            with open(self.file_path, 'rb') as file:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(file) as reader:
                    decompressed = io.TextIOWrapper(reader, encoding='utf-8')
                    
                    for i, line in enumerate(decompressed):
                        if lines and i >= lines:
                            break  # Stop processing if num_lines is reached
                        
                        original_data = json.loads(line)
                        transformed_item = self.handle_data(original_data, input, output, instruction, add)
                        json_line = json.dumps(transformed_item, ensure_ascii=False)
                        line_bytes = len(json_line.encode('utf-8'))
                        
                        # Check if the bytes limit is reached, if a limit is set
                        if bytes and total_bytes + line_bytes > bytes:
                            break
                        
                        total_bytes += line_bytes  # Update the total bytes counter
                        yield transformed_item  # Yield the transformed item for writing
                        
        except Exception as e:
            print(f"An error occurred while handling the ZST file: {e}")


    # Handling the data
    def handle_data(self, data, input=None, output=None, instruction=None, add=None):
        """Main method to determine the transformation strategy based on the provided arguments."""
        transformed_data = []
        
        # Check if the data is in a conversational format
        if self.conversation:
            transformed_data.extend(self.handle_conversation(data))
        elif input and output:
            transformed_data.extend(self.handle_custom_keys(data, input, output, instruction, add))
        else:
            transformed_data.extend(self.auto_detect_and_transform(data))
        
        return transformed_data

    # For nested conversation
    def handle_conversation(self, data):
        """Method to handle conversation data by extracting user and assistant messages."""
        transformed_data = []

        # Extracting the 'data' field from the JSON object
        conversation_data = data.get('data', [])
        
        # Iterating over each element in the 'data' list
        for i in range(0, len(conversation_data), 2):  # Increment by 2 to process pairs of messages
            user_input = conversation_data[i] if i < len(conversation_data) else ""
            assistant_output = conversation_data[i+1] if i+1 < len(conversation_data) else ""
            
            # Creating an object for each pair of messages
            transformed_data.append({
                "instruction": "",
                "input": user_input,
                "output": assistant_output
            })
            
        return transformed_data

    def handle_custom_keys(self, data, input, output, instruction, add=None):
        """Method to handle data with custom keys as specified by the user."""
        if not all(key in data for key in (input, output)):
            raise ValueError("The specified keys do not match the data structure.")
        
        transformed_data = {
            "instruction": data.get(instruction, ""),
            "input": data.get(input, ""),
            "output": data.get(output, "")
        }
        
        # Adding additional columns if specified
        if add:
            additional_columns = add.split(',')  # Assuming additional columns are comma-separated
            for col in additional_columns:
                transformed_data[col] = data.get(col, "")
    
        return [transformed_data]

    def auto_detect_and_transform(self, data):
        """Method to automatically detect and transform the data based on common key variants."""
        transformed_data = []
        keys_variants = [
            ("question", "answer", "instruction"),
            ("Q", "A", "instruction"),
            ("user_message", "bot_message", "instruction"),
            ("user", "bot", "instruction"),
            ("input", "output", "instruction"),
            ("user_query", "bot_reply", "system")
        ]
        
        for variant in keys_variants:
            input, output, instruction = variant
            if input in data and output in data:
                transformed_data.append({
                    "instruction": data.get(instruction, ""),
                    "input": data[input],
                    "output": data[output]
                })
                break  # Stop the loop once a suitable variant is found and processed
            
        return transformed_data
