import re
from typing import List, Dict, Any
import logging

class Condition:
    """
    This class represents a single condition in the filter.
    It holds the field to be filtered, the operator, and the value to compare against.
    """
    def __init__(self, field: str, operator: str = None, value: Any = None):
        self.field = field.split('.')
        self.operator = operator
        self.value = self.cast_value(value)

    @staticmethod
    def cast_value(value: str) -> Any:
        """
        Attempts to cast the value to an int or float.
        If casting fails, the value is returned as a string.
        """
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    @staticmethod
    def get_nested_value(item: Dict, fields: List[str]) -> Any:
        """
        Recursively fetches the value from nested dictionaries based on the given fields.
        """
        for field in fields:
            item = item.get(field, {})
        return item if item != {} else None

    def matches(self, item: Dict) -> bool:
        """
        Checks if the item matches this condition.
        """
        # Special case for filter with only field specified
        if self.operator is None and self.value in [None, '']:
            return self.field_exists_in_item(item, self.field)
        else:
            item_value = self.get_nested_value(item, self.field)
            if item_value is None:
                logging.error(f"Field not found in item: {self.field}")

                return False
            item_value = self.cast_value(str(item_value))
            return self.compare_values(item_value, self.value)

    def field_exists_in_item(self, item: Dict, fields: List[str]) -> bool:
        """
        Checks if the specified fields exist in the item.
        """
        for field in fields:
            if field not in item:
                return False
            item = item[field]
        return True

    def compare_values(self, item_value: Any, condition_value: Any) -> bool:
        """
        Compares the item's value against the condition's value based on the operator.
        """
        if self.operator is None:
            return True
        elif self.operator in ["=", "=="]:
            return item_value == condition_value
        elif self.operator == "!=":
            return item_value != condition_value
        elif self.operator == "<":
            return item_value < condition_value
        elif self.operator == ">":
            return item_value > condition_value
        elif self.operator == "<=>":
            lower, upper = map(self.cast_value, condition_value.split(','))
            return lower <= item_value <= upper
        return False

    @staticmethod
    def parse(spec: str) -> 'Condition':
        """
        Parses a condition specification string into a Condition object.
        """
        if "<=>" in spec:
            field, value = spec.split("<=>")
            return Condition(field, "<=>", value)

        match = re.match(r"([\w\.]+)(!=|==|=|<|>)?(.*)", spec)
        if not match:
            raise ValueError(f"Invalid specification string: {spec}")

        field, operator, value = match.groups()
        return Condition(field, operator, value)

class LabelFilter:
    """
    This class handles the filtering of data based on a list of conditions.
    """
    def __init__(self, specifications: List[str]):
        self.conditions = [Condition.parse(spec) for spec in specifications]

    def filter_data(self, data_batch: List[Dict]) -> List[Dict]:
        """
        Filters a batch of data items based on the defined conditions.
        """
        return [self.reduce_item(item) for item in data_batch if self.matches_all_conditions(item)]

    def matches_all_conditions(self, item: Dict) -> bool:
        """
        Checks if an item satisfies all the conditions.
        """
        return all(condition.matches(item) for condition in self.conditions)

    def reduce_item(self, item: Dict) -> Dict:
        """
        Reduces an item to only the fields that are specified in the conditions.
        """
        reduced_item = {}
        for condition in self.conditions:
            if condition.operator is None:
                field_value = condition.get_nested_value(item, condition.field)
                if field_value is not None:
                    reduced_item[".".join(condition.field)] = field_value
        return reduced_item if reduced_item else item

# Usage Example:
# filter = LabelFilter(["user_id>50", "conversation_id", "source=source1", "metadata.age<50", "metadata.temp <=> 1,3"])
# filtered_data = filter.filter_data(data_batch)
