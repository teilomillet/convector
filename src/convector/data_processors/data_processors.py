# data_processors.py

import uuid
import logging
from typing import List, Dict, Any

class IDataProcessor:
    def process(self, data: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("This method should be implemented by subclass")

class ConversationDataProcessor(IDataProcessor):
    def process(self, data: Any, **kwargs) -> List[Dict[str, Any]]:
        fields_to_include = kwargs.get('fields_to_include', [])
        transformed_data = []

        # Check if data is a list and process each item in the list
        if isinstance(data, list):
            for item in data:
                transformed_data.extend(self.process_single_item(item, fields_to_include))
        elif isinstance(data, dict):
            # Process a single item
            transformed_data.extend(self.process_single_item(data, fields_to_include))
        else:
            logging.error(f"Unexpected data type: {type(data)}")
            return []

        return transformed_data

    def process_single_item(self, item: Dict[str, Any], fields_to_include: List[str]) -> List[Dict[str, Any]]:
        transformed_data = []
        conversation_id = item.get('conversation_id', uuid.uuid4().hex[:5])
        system_prompt = item.get('system_prompt', '') 

        if 'conversations' in item:
            # Initialize variables to store the human input and GPT output
            human_input, gpt_output = "", ""

            for conv in item['conversations']:
                if conv.get('from') == 'human':
                    human_input = conv.get('value', '')
                elif conv.get('from') == 'gpt':
                    gpt_output = conv.get('value', '')

                    # Create a conversation piece with system_prompt as instruction
                    conversation_piece = {
                        "conversation_id": conversation_id,
                        "instruction": system_prompt,  # Use the system prompt here
                        "input": human_input,
                        "output": gpt_output
                    }
                    transformed_data.append(conversation_piece)

                    # Reset human_input and gpt_output for the next pair
                    human_input, gpt_output = "", ""

        elif 'data' in item:
            # Process the existing structure
            conversation_data = item['data']
            for i in range(0, len(conversation_data), 2):
                transformed_data.extend(self.extract_conversation_piece(conversation_data, i, conversation_id))
        else:
            # Process a single conversation piece
            item['conversation_id'] = conversation_id
            transformed_data.append(item)

        if not transformed_data:
            logging.warning(f"No data transformed in ConversationDataProcessor for: {item}")
            return []

        # Include specified fields if present
        for data_item in transformed_data:
            for field in fields_to_include:
                if field in item:
                    data_item[field] = item[field]

        return transformed_data

    def extract_conversation_piece(self, conversation_data, index, conversation_id):
        try:
            # Initialize user input and assistant output
            user_input, assistant_output = "", ""

            # Ensure that we are accessing valid indices
            if index < len(conversation_data):
                user_input = conversation_data[index]
                # Check if the next element exists and is for the assistant's response
                if index + 1 < len(conversation_data):
                    assistant_output = conversation_data[index + 1]

            conversation_piece = {
                "conversation_id": conversation_id,
                "instruction": "",
                "input": user_input,
                "output": assistant_output
            }

            return [conversation_piece]

        except Exception as e:
            logging.error(f"Error processing conversation data: {e}")
            return []

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
