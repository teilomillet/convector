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
        labels = kwargs.get('labels', [])  # Default to an empty list if not provided
        
        # Check if the necessary keys are in data
        if not (input_key and output_key) or input_key not in data or output_key not in data:
            logging.error(f"The necessary keys are missing or do not match the data structure. Data keys: {list(data.keys())}")
            raise ValueError("The necessary keys are missing or do not match the data structure.")
        
        transformed_data = {
            "instruction": data.get(instruction_key, ""),
            "input": data.get(input_key, ""),
            "output": data.get(output_key, "")
        }
        
        # Directly iterate over labels since it is already a list
        for col in labels:
            transformed_data[col] = data.get(col, "")

        return [transformed_data]


class AutoDetectDataProcessor(IDataProcessor):
    def __init__(self):
        # Schema patterns can be loaded from a config or defined here.
        self.schema_patterns = {
            "instruction": ["instruction", "system", "system_prompt"],
            "input": ["question", "input", "user_query"],
            "output": ["answer", "output", "bot_reply", "response"]
        }
        self.detected_schema = {}

    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        # Detect schema once and store it
        self.detect_schema(data)

        # Use detected schema to transform data
        transformed_data = [{
            "instruction": data.get(self.detected_schema.get("instruction", ""), ""),
            "input": data.get(self.detected_schema.get("input", ""), ""),
            "output": data.get(self.detected_schema.get("output", ""), "")
        }] if self.detected_schema else []

        return transformed_data

    def detect_schema(self, data: Dict[str, Any]):
        # Iterate once over the data keys and match with schema patterns
        for key in data.keys():
            for schema, variants in self.schema_patterns.items():
                if key in variants:
                    self.detected_schema[schema] = key
                    break  # Move to the next key after a match is found
            if len(self.detected_schema) == len(self.schema_patterns):  # Early exit if all schema keys are found
                break
