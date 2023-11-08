from typing import List, Dict, Optional, Any, Generator
from inspect import signature

class OutputSchemaHandler:
    def __init__(self, schema_name: Optional[str] = "default", add_keys: Optional[List[str]] = None):
        self.schema_name = schema_name
        self.add_keys = add_keys 

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

    def apply_schema(self, data: Any, batch_size: Optional[int] = None, **kwargs) -> Any:
        is_single_item = isinstance(data, dict)
        if is_single_item:
            data = [data]  # Make it a list to reuse the existing logic

        if not all(isinstance(item, dict) for item in data):
            raise TypeError("apply_schema expects a list of dictionaries or a single dictionary.")

        handler_method = getattr(self, f"apply_{self.schema_name}_schema", None)
        if not handler_method:
            raise ValueError(f"Unsupported schema '{self.schema_name}'")

        params = signature(handler_method).parameters
        use_batching = 'use_batching' in params

        transformed_data = []
        if use_batching and batch_size:
            for batch in self.batch_data(data, batch_size):
                transformed_batch = handler_method(data=batch, **kwargs)
                transformed_data.extend(transformed_batch)
        else:
            transformed_data = handler_method(data=data, **kwargs)

        return transformed_data[0] if is_single_item else transformed_data
    
    def apply_default_schema(self, data: List[Dict[str, Any]], add_keys: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Default schema that can optionally include additional keys in the data.
        """
        if add_keys is None:
            return data

        # If there are additional keys to add, create a new data list that includes them
        transformed_data = []
        for item in data:
            # Start with a shallow copy of the original item
            transformed_item = item.copy()
            # Add any additional keys that are missing
            for add_key in add_keys:
                if add_key not in transformed_item:
                    transformed_item[add_key] = item.get(add_key, "")
            transformed_data.append(transformed_item)

        return transformed_data


    def apply_chat_completion_schema(self, data: List[Dict[str, Any]], add_keys: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        chat_completions = []
        for item in data:
            chat_completion = {
                "messages": [
                    {"role": "system", "content": item.get("instruction", "")},
                    {"role": "user", "content": item.get("input", "")},
                    {"role": "assistant", "content": item.get("output", "")}
                ]
            }
            
            # Include additional fields if provided
            if add_keys:
                for add_key in add_keys:
                    # Add the additional field to the chat_completion dictionary
                    chat_completion[add_key] = item.get(add_key, "")
            
            chat_completions.append(chat_completion)

        return chat_completions
