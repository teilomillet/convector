# data_processors.py

import uuid
import logging
from typing import List, Dict, Any

class IDataProcessor:
    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("This method should be implemented by subclass")

class ConversationDataProcessor(IDataProcessor):
    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        fields_to_include = kwargs.get('fields_to_include', [])
        transformed_data = []

        # Generate a conversation ID once per conversation
        conversation_id = data.get('conversation_id', uuid.uuid4().hex[:5])

        if 'data' in data:
            conversation_data = data.get('data', [])
            for i in range(0, len(conversation_data), 2):
                # Use the same conversation_id for each pair
                transformed_data.extend(self.extract_conversation_piece(conversation_data, i, conversation_id))
        else:
            # For individual conversation pieces, use the generated conversation_id
            data['conversation_id'] = conversation_id
            transformed_data.append(data)

        if not transformed_data:
            logging.warning(f"No data transformed in ConversationDataProcessor for: {data}")
            return []

        # Include specified fields if present
        for item in transformed_data:
            for field in fields_to_include:
                if field in data:
                    item[field] = data[field]

        return transformed_data

    def extract_conversation_piece(self, conversation_data, index, conversation_id):
        try:
            user_input = conversation_data[index] if index < len(conversation_data) else ""
            assistant_output = conversation_data[index + 1] if index + 1 < len(conversation_data) else ""

            conversation_piece = {
                "conversation_id": conversation_id,
                "instruction": "",
                "input": user_input,
                "output": assistant_output
            }

            return [conversation_piece]

        except Exception as e:
            logging.error(f"Error processing conversation pair at index {index}: {e}")
            return [] 
            
    
    def transform_using_schema(self, conversation_piece, output_schema_handler):
        if output_schema_handler:
            return output_schema_handler.apply_schema(conversation_piece)
        return conversation_piece

class CustomKeysDataProcessor(IDataProcessor):
    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        fields_to_include = kwargs.get('fields_to_include', [])
        input_key = kwargs.get('input')
        output_key = kwargs.get('output')
        instruction_key = kwargs.get('instruction')
        
        if not (input_key and output_key) or input_key not in data or output_key not in data:
            logging.error("The necessary keys are missing or do not match the data structure.")
            raise ValueError("The necessary keys are missing or do not match the data structure.")
        
        transformed_data = {
            "instruction": data.get(instruction_key, ""),
            "input": data.get(input_key, ""),
            "output": data.get(output_key, "")
        }

        # Include specified fields if present
        for field in fields_to_include:
            if field in data:
                transformed_data[field] = data[field]

        return [transformed_data]

class AutoDetectDataProcessor(IDataProcessor):
    def __init__(self):
        self.schema_patterns = {
            "instruction": ["instruction", "system", "system_prompt"],
            "input": ["question", "input", "user_query"],
            "output": ["answer", "output", "bot_reply", "response"]
        }
        self.detected_schema = {}

    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        fields_to_include = kwargs.get('fields_to_include', [])
        self.detect_schema(data)

        if not all(key in self.detected_schema for key in ['instruction', 'input', 'output']):
            logging.error("Required keys not detected in data.")
            return []

        transformed_data = {
            "instruction": data.get(self.detected_schema.get("instruction", ""), ""),
            "input": data.get(self.detected_schema.get("input", ""), ""),
            "output": data.get(self.detected_schema.get("output", ""), "")
        }

        # Include specified fields if present
        for field in fields_to_include:
            if field in data:
                transformed_data[field] = data[field]

        return [transformed_data]

    def detect_schema(self, data: Dict[str, Any]):
        # Reset detected schema
        self.detected_schema = {}

        # Mapping for possible field names in the data
        possible_field_names = {
            "instruction": ["system_prompt", "instruction"],
            "input": ["question", "input"],
            "output": ["response", "output"]
        }

        # Detect and map the fields
        for schema_key, possible_names in possible_field_names.items():
            for field_name in possible_names:
                if field_name in data:
                    self.detected_schema[schema_key] = field_name
                    break
