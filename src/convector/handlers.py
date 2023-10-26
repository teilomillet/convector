import json
import csv
import pandas as pd
import polars as pl
import zstandard as zstd
import uuid
import io
import os
import random

class FileHandler:
    def __init__(self, file_path, conversation=False):
        self.file_path = file_path
        self.conversation = conversation
        self.printed = False

    def transform_data(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False):
        file_extension = os.path.splitext(self.file_path)[-1].lower()
        if random_selection:
            if not lines and not bytes:
                user_input = input("Enter number of lines (L) or bytes (B): ")
                lines, bytes = self.process_user_input(user_input)
                
        handler_method = getattr(self, f"handle_{file_extension[1:]}", None)
        if handler_method:
            yield from handler_method(input=input, output=output, instruction=instruction, add=add, lines=lines, bytes=bytes, random_selection=random_selection)
        else:
            print(f"Error: Unsupported file extension '{file_extension}'")
    
    def process_user_input(self, user_input):
        try:
            if 'L' in user_input.upper():
                return int(user_input.upper().replace('L', '')), None
            elif 'B' in user_input.upper():
                return None, int(user_input.upper().replace('B', ''))
            else:
                return int(user_input), None
        except ValueError:
            print("Error: Invalid input. Please enter a valid number of lines or bytes.")
            return None, None

    def random_selector(self, file, lines=None, bytes=None, conversation=False):
        try:
            if conversation:  # Enhanced logic for conversation-based random selection
                all_conversations = []
                current_conversation = []
                
                for line in file:
                    line_data = json.loads(line)
                    # Assuming a 'system' role indicates a new conversation
                    # Modify this condition based on the actual indicator of a new conversation in your data
                    if line_data.get('role') == 'system' and current_conversation:
                        all_conversations.append(current_conversation)
                        current_conversation = []
                    
                    current_conversation.append(line_data)
                
                # Adding the last conversation if it hasn't been added
                if current_conversation:
                    all_conversations.append(current_conversation)
                
                selected_conversations = random.sample(all_conversations, min(lines, len(all_conversations)))
                return selected_conversations
            
            else:  # Existing logic for line-based random selection
                selected_lines = []
                if lines:
                    total_lines = sum(1 for _ in file)
                    file.seek(0)  # Reset the file pointer to the beginning of the file
                    if lines > total_lines:
                        lines = total_lines  # Ensure lines do not exceed the actual number of lines in the file
                    selected_lines = random.sample(range(total_lines), lines)
                elif bytes:
                    return self.select_lines_within_bytes(file, bytes)
                return selected_lines
            
        except Exception as e:
            print(f"Error during random selection: {e}")
            return []

    def select_lines_within_bytes(self, file, target_bytes):
        selected_lines = []
        current_bytes = 0
        current_line = 0
        
        while current_bytes < target_bytes:
            line = file.readline()
            if not line:
                break  # End of file reached
            
            line_bytes = len(line.encode('utf-8'))
            if current_bytes + line_bytes > target_bytes:
                break  # Stop if adding the next line exceeds the target bytes
            
            selected_lines.append(current_line)
            current_bytes += line_bytes
            current_line += 1
        
        file.seek(0)  # Reset file pointer to the start
        return selected_lines

    # JSON
    def handle_json(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False):
        total_bytes = 0  # Counter to keep track of total bytes processed
        
        try:
            with open(self.file_path, 'r') as file:
                if random_selection:
                    selected_positions = self.random_selector(file, lines, bytes)

                for i, line in enumerate(file):
                    if random_selection and i not in selected_positions:
                        continue

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
    def handle_jsonl(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False, conversation=False):
        total_bytes = 0  # Counter to keep track of total bytes processed
        try:
            with open(self.file_path, 'r') as file:
                if random_selection and conversation:  # Special handling for conversation-based random selection
                    selected_conversations = self.random_selector(file, lines, conversation=True)
                    for conv in selected_conversations:
                        for item in self.handle_conversation(conv):
                            json_line = json.dumps(item, ensure_ascii=False)
                            yield item  # Yield each message pair within the selected conversations
                else:
                    if random_selection:
                        selected_positions = self.random_selector(file, lines, bytes)
                        file.seek(0)

                    for i, line in enumerate(file):
                        if random_selection and i not in selected_positions:
                            print(f"Processing line number: {i}")
                            continue
                            
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
    def handle_csv(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False):
        total_bytes = 0  # Counter to keep track of total bytes processed
        
        try:
            with open(self.file_path, newline='') as csvfile:
                if random_selection: 
                    selected_positions = self.random_selector(csvfile, lines, bytes)

                reader = csv.DictReader(csvfile)
                
                for i, row in enumerate(reader):
                    if random_selection and i not in selected_positions:
                        continue
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

    # For the Parquet handler, you might consider handling DataFrame directly for random selection
    def handle_parquet(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False):
        total_bytes = 0

        try:
            df = pd.read_parquet(self.file_path)  # Load the entire Parquet file

            if random_selection:
                # Here, handling a DataFrame directly for random selection
                total_rows = len(df)
                selected_rows = random.sample(range(total_rows), min(lines if lines else total_rows, total_rows))
                df = df.iloc[selected_rows]
            
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
    def handle_zst(self, input=None, output=None, instruction=None, add=None, lines=None, bytes=None, random_selection=False):
        total_bytes = 0  # Counter to keep track of total bytes processed
        
        try:
            with open(self.file_path, 'rb') as file:
                if random_selection: 
                    selected_positions = self.random_selector(file, lines, bytes)

                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(file) as reader:
                    decompressed = io.TextIOWrapper(reader, encoding='utf-8')
                    
                    for i, line in enumerate(decompressed):
                        if random_selection and i not in selected_positions:
                            continue
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
    def handle_conversation(self, data, conversation_id=None):
        """Method to handle conversation data by extracting user and assistant messages."""
        transformed_data = []

        conversation_id = conversation_id or uuid.uuid4().hex[:5]

        # Extracting the 'data' field from the JSON object
        conversation_data = data.get('data', [])
        
        # Iterating over each element in the 'data' list
        for i in range(0, len(conversation_data), 2):  # Increment by 2 to process pairs of messages
            try:
                user_input = conversation_data[i] if i < len(conversation_data) else ""
                assistant_output = conversation_data[i+1] if i+1 < len(conversation_data) else ""
                
                # Creating an object for each pair of messages
                transformed_data.append({
                    "conversation_id": conversation_id,
                    "instruction": "",
                    "input": user_input,
                    "output": assistant_output
                })
            except AttributeError as e:
                print(f"Attribute Error: {e}. Skipping this pair of messages.")
            except Exception as e:
                print(f"An unexpected error occurred: {e}. Skipping this pair of messages.")
            
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
