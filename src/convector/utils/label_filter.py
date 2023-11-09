# label_filter.py

import re
from typing import List, Dict, Any, Union

class Condition:
    def __init__(self, field, operator=None, value=None):
        self.field = field.split('.')  # Split the field into parts
        self.operator = operator
        self.value = self.cast_value(value)

    def cast_value(self, value):
        """ Try to cast the value to int or float, or keep as string if it fails """
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    def get_nested_value(self, item, fields):
        """ Recursively get the nested value """
        for field in fields:
            item = item.get(field, {})
        return item if item != {} else None

    def apply(self, item):
        """
        Get the value from the nested JSON
        """
        item_value = self.get_nested_value(item, self.field)
        if item_value is None:
            return False

        # Attempt to cast item value to int or float for comparison
        item_value = self.cast_value(str(item_value))

        # Compare based on the operator
        if self.operator == "=":
            return item_value == self.value
        elif self.operator =="!=":
            return item_value != self.value
        elif self.operator == "<":
            return item_value < self.value
        elif self.operator == ">":
            return item_value > self.value
        else:
            return True

    @staticmethod
    def parse_condition_string(spec):
        """ Parses specification strings into field, operator, and value (if present)"""
        match = re.match(r"([\w\.]+)(!=|[<>=])?(.*)", spec)
        if not match:
            raise ValueError(f"Invalid specification string: {spec}")

        field, operator, value = match.groups()
        return Condition(field, operator, value)

class LabelFilter:
    def __init__(self, specifications):
        """ Parses specifications into Condition objects"""
        self.conditions = [Condition.parse_condition_string(spec) for spec in specifications]

    def filter_data(self, data_batch):
        """ Filters and reduces a batch of data items"""
        filtered_batch = []
        for item in data_batch:
            reduced_item = {}
            for condition in self.conditions:
                # If it's a field to include without condition
                if condition.operator is None:
                    reduced_item[condition.field] = item.get(condition.field)
                # If it's a condition with an operator and value
                elif condition.apply(item):
                    reduced_item[condition.field] = item.get(condition.field)
            
            # Add item to the filtered batch if it meets all conditions
            if all(cond.apply(item) for cond in self.conditions if cond.operator):
                filtered_batch.append(reduced_item)
        
        return filtered_batch