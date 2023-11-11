import re
from typing import List, Dict, Any
import logging
from convector.core.profile import FilterCondition


class Condition:
    """
    This class represents a single condition in the filter.
    It holds the field to be filtered, the operator, and the value to compare against.
    """
    def __init__(self, field: str, operator: str = None, value: Any = None):
        self.field = field.split('.')
        self.operator = operator
        self.value = self.cast_value(value)
        self.is_inclusion = operator is None and value is None

    @staticmethod
    def cast_value(value: str) -> Any:
        """
        Attempts to cast the value to an int or float.
        If casting fails or value is None, the value is returned as is.
        """
        if value is None:
            return value

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
        if self.is_inclusion:
            # For field inclusion, always return True
            return True

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
    
    @staticmethod
    def convert_to_condition(filter_condition: FilterCondition) -> 'Condition':
        field = filter_condition.field
        operator = filter_condition.operator if filter_condition.operator else None
        value = filter_condition.value if filter_condition.value else None
        return Condition(field, operator, value)

class LabelFilter:
    def __init__(self, filter_conditions: List[Condition]):
        self.conditions = [Condition.convert_to_condition(fc) for fc in filter_conditions]

    def apply_filters(self, data_batch: List[Dict]) -> List[Dict]:
        all_inclusions = all(condition.is_inclusion for condition in self.conditions)

        if all_inclusions:
            included_data = [self.include_fields(item) for item in data_batch]
            return included_data

        filtered_data = []
        for item in data_batch:
            matches = self.matches_all_conditions(item)
            if matches:
                item = self.include_fields(item)
                filtered_data.append(item)
        return filtered_data

    def include_fields(self, item: Dict) -> Dict:
        """
        Ensures specified fields are included in the item.
        """
        inclusion_fields = {field: item.get(field) for condition in self.conditions if condition.is_inclusion for field in condition.field}
        return {**item, **inclusion_fields}  # Merge included fields with existing item

    def matches_all_conditions(self, item: Dict) -> bool:
        filter_conditions = [cond for cond in self.conditions if not cond.is_inclusion]
        if not filter_conditions:
            return True  # If there are no filter conditions, all items match
        return all(condition.matches(item) for condition in filter_conditions)
