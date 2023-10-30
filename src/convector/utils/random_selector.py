# random_selectors.py

from typing import Any, TextIO
import json
import random

class IRandomSelector:
    def select(self, file: TextIO, *args, **kwargs) -> Any:
        raise NotImplementedError("This method should be implemented by subclass")

class ConversationRandomSelector(IRandomSelector):
    def select(self, file: TextIO, lines: int = None, **kwargs) -> Any:
        all_conversations = []
        current_conversation = []

        for line in file:
            line_data = json.loads(line)
            if line_data.get('role') == 'system' and current_conversation:
                all_conversations.append(current_conversation)
                current_conversation = []

            current_conversation.append(line_data)

        if current_conversation:
            all_conversations.append(current_conversation)

        selected_conversations = random.sample(all_conversations, min(lines, len(all_conversations)))
        return selected_conversations


class LineRandomSelector(IRandomSelector):
    def select(self, file: TextIO, lines: int = None, **kwargs) -> Any:
        selected_lines = []
        total_lines = sum(1 for _ in file)
        file.seek(0)
        if lines > total_lines:
            lines = total_lines
        selected_lines = random.sample(range(total_lines), lines)
        return selected_lines


class ByteRandomSelector(IRandomSelector):
    def select(self, file: TextIO, bytes: int = None, **kwargs) -> Any:
        selected_lines = []
        current_bytes = 0
        current_line = 0

        while current_bytes < bytes:
            line = file.readline()
            if not line:
                break

            line_bytes = len(line.encode('utf-8'))
            if current_bytes + line_bytes > bytes:
                break

            selected_lines.append(current_line)
            current_bytes += line_bytes
            current_line += 1

        file.seek(0)
        return selected_lines

