# data_processors.py

import uuid
from typing import List, Dict, Any

class IDataProcessor:
    def process(self, data: Dict[str, Any], *args, **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("This method should be implemented by subclass")

class ConversationDataProcessor(IDataProcessor):
    def process(self, data: Dict[str, Any], conversation_id: str = None, *args, **kwargs) -> List[Dict[str, Any]]:
        transformed_data = []
        conversation_id = conversation_id or uuid.uuid4().hex[:5]
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
                print(f"An error occurred: {e}. Skipping this pair of messages.")

        return transformed_data


class CustomKeysDataProcessor(IDataProcessor):
    def process(self, data: Dict[str, Any], input: str, output: str, instruction: str, add: str = None, *args, **kwargs) -> List[Dict[str, Any]]:
        if not all(key in data for key in (input, output)):
            raise ValueError("The specified keys do not match the data structure.")
        
        transformed_data = {
            "instruction": data.get(instruction, ""),
            "input": data.get(input, ""),
            "output": data.get(output, "")
        }
        
        if add:
            additional_columns = add.split(',')
            for col in additional_columns:
                transformed_data[col] = data.get(col, "")

        return [transformed_data]


class AutoDetectDataProcessor(IDataProcessor):
    def process(self, data: Dict[str, Any], *args, **kwargs) -> List[Dict[str, Any]]:
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