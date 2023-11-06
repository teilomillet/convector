# data_processors.py

import uuid
import logging
from typing import List, Dict, Any

class IDataProcessor:
    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("This method should be implemented by subclass")

class ConversationDataProcessor(IDataProcessor):
    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        transformed_data = []
        conversation_id = kwargs.get('conversation_id', uuid.uuid4().hex[:5])
        conversation_data = data.get('data', [])
        
        for i in range(0, len(conversation_data), 2):
            try:
                user_input = conversation_data[i] if i < len(conversation_data) else ""
                assistant_output = conversation_data[i+1] if i+1 < len(conversation_data) else ""
                
                transformed_data.append({
                    "conversation_id": conversation_id,
                    "instruction": "",
                    "input": user_input,
                    "output": assistant_output
                })
            except Exception as e:
                logging.error(f"Error processing conversation pair at index {i}: {e}")
                continue  # Skip this pair of messages and continue

        return transformed_data



class CustomKeysDataProcessor(IDataProcessor):
    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        input_key = kwargs.get('input')
        output_key = kwargs.get('output')
        instruction_key = kwargs.get('instruction')
        add_keys = kwargs.get('add', None)
        
        # Check if the necessary keys are in data
        if not (input_key and output_key) or input_key not in data or output_key not in data:
            logging.error(f"The necessary keys are missing or do not match the data structure. Data keys: {list(data.keys())}")
            raise ValueError("The necessary keys are missing or do not match the data structure.")
        
        transformed_data = {
            "instruction": data.get(instruction_key, ""),
            "input": data.get(input_key, ""),
            "output": data.get(output_key, "")
        }
        
        if add_keys:
            additional_columns = add_keys.split(',')
            for col in additional_columns:
                transformed_data[col] = data.get(col, "")

        return [transformed_data]

class AutoDetectDataProcessor(IDataProcessor):
    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
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
                break
        
        return transformed_data