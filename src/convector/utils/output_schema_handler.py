from typing import List, Dict, Optional, Any, Generator
from inspect import signature

class OutputSchemaHandler:
    def __init__(self, schema_name: Optional[str] = "default"):
        self.schema_name = schema_name

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

    def apply_schema(self, data: Any, batch_size: Optional[int] = None) -> Any:
        is_single_item = isinstance(data, dict)
        if is_single_item:
            data = [data]  # Make it a list to reuse the existing logic

        if not all(isinstance(item, dict) for item in data):
            raise TypeError("apply_schema expects a list of dictionaries or a single dictionary.")

        handler_method = getattr(self, f"apply_{self.schema_name}_schema", None)
        if not handler_method:
            raise ValueError(f"Unsupported schema '{self.schema_name}'")

        params = signature(handler_method).parameters
        use_batching = params.get('use_batching', False)

        transformed_data = []
        if use_batching and batch_size:
            for batch in self.batch_data(data, batch_size):
                transformed_batch = handler_method(data=batch)
                transformed_data.extend(transformed_batch)
        else:
            transformed_data = handler_method(data=data)

        return transformed_data[0] if is_single_item else transformed_data
    
    def apply_default_schema(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Default schema that does nothing and returns the data as-is.
        """
        return data

    def apply_chat_completion_schema(self, data: List[Dict[str, Any]], use_batching: bool = True) -> List[Dict[str, Any]]:
        # Your existing logic to transform data into the chat_completion schema
        return [
            {
                "messages": [
                    {"role": "system", "content": item.get("instruction", "")},
                    {"role": "user", "content": item.get("input", "")},
                    {"role": "assistant", "content": item.get("output", "")}
                ]
            }
            for item in data
        ]
