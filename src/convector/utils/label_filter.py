# label_filter.py

import re
from typing import List, Dict, Any, Union

class Condition:
    def __init__(self, column: str, operator: str, value: Union[str, float, int]):
        self.column = column
        self.operator = operator
        self.value = self.cast_value(value)

    @staticmethod
    def cast_value(value: str) -> Union[str, float, int]:
        try:
            return float(value) if '.' in value or 'e' in value.lower() else int(value)
        except ValueError:
            return value

    def evaluate(self, item: Dict[str, Any]) -> bool:
        item_value = item.get(self.column)
        if isinstance(item_value, (int, float)) and isinstance(self.value, str):
            item_value = str(item_value)

        if self.operator == '>' and item_value > self.value:
            return True
        elif self.operator == '<' and item_value < self.value:
            return True
        elif self.operator == '=' and item_value == self.value:
            return True
        elif self.operator == '!=' and item_value != self.value:
            return True
        return False

class LabelFilter:
    def __init__(self, conditions: List[str]):
        self.conditions = self.parse_conditions(conditions)

    @staticmethod
    def parse_conditions(conditions: List[str]) -> List[Union[str, Condition]]:
        parsed_conditions = []
        for condition in conditions:
            match = re.match(r"(.*?)\s*([><!=]+)\s*(.*)", condition)
            if match:
                column, operator, value = match.groups()
                parsed_conditions.append(Condition(column, operator, value))
            else:
                parsed_conditions.append(condition)  # Column without condition
        return parsed_conditions

    def apply(self, item: Dict[str, Any]) -> bool:
        for condition in self.conditions:
            if isinstance(condition, Condition) and not condition.evaluate(item):
                return False
        return True