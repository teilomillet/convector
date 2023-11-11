from typing import List, Dict, Optional, Any, Generator
from inspect import signature

from convector.core.profile import FilterCondition, Profile

class OutputSchemaHandler:
    def __init__(self, schema_name: Optional[str] = "default", filters: Optional[List[FilterCondition]] = None):
        self.schema_name = schema_name
        self.filters = filters
        self.fields_to_include = self.extract_fields_from_filters() if filters else None

    def extract_fields_from_filters(self) -> List[str]:
        """
        Extracts field names from filter conditions to determine which fields to include in the output.
        """
        fields = [condition.field for condition in self.filters]
        return fields
    
    def apply_schema(self, data: Any, **kwargs) -> Any:
        """
        Apply the selected schema to the data.
        """
        is_single_item = isinstance(data, dict)
        if is_single_item:
            data = [data]  

        if not all(isinstance(item, dict) for item in data):
            raise TypeError("apply_schema expects a list of dictionaries or a single dictionary.")

        handler_method = getattr(self, f"apply_{self.schema_name}_schema", None)
        if not handler_method:
            raise ValueError(f"Unsupported schema '{self.schema_name}'")

        transformed_data = handler_method(data=data, **kwargs)
        return transformed_data[0] if is_single_item else transformed_data
    
    def batch_data(self, data: List[Dict[str, Any]], batch_size: int) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Custom function to batch the data.
        This function assumes that each conversation ends after the assistant's content.
        """
        batch = []
        for item in data:
            if item.get('role') == 'assistant':
                batch.append(item)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            else:
                batch.append(item)

        if batch:
            yield batch
    
    def apply_default_schema(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed_data = []
        for item in data:
            transformed_item = {
                "instruction": item.get("instruction", ""),
                "input": item.get("input", ""),
                "output": item.get("output", "")
            }

            # Include conversation_id if present
            if 'conversation_id' in item:
                transformed_item['conversation_id'] = item['conversation_id']

            # Add additional fields specified in filters
            if self.fields_to_include:
                for field in self.fields_to_include:
                    if field in item:
                        transformed_item[field] = item[field]

            transformed_data.append(transformed_item)

        return transformed_data

    def apply_chat_completion_schema(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        chat_completions = []
        for item in data:
            chat_completion = {
                "messages": [
                    {"role": "system", "content": item.get("instruction", "")},  # Always include system message
                    {"role": "user", "content": item.get("input", "")} if item.get("input") else None,
                    {"role": "assistant", "content": item.get("output", "")} if item.get("output") else None
                ]
            }

            # Include conversation_id if present
            if 'conversation_id' in item:
                chat_completion['conversation_id'] = item['conversation_id']

            # Add additional fields specified in filters
            if self.fields_to_include:
                for field in self.fields_to_include:
                    if field in item:
                        chat_completion[field] = item[field]

            # Remove None entries from messages
            chat_completion["messages"] = [msg for msg in chat_completion["messages"] if msg is not None]

            chat_completions.append(chat_completion)

        return chat_completions

