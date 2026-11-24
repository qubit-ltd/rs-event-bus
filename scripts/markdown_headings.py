"""Small Markdown heading scanner shared by documentation checkers."""

import re


FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")


def heading_titles(text):
    """Yield ATX heading titles outside fenced code blocks."""
    for _line_number, _level, title in heading_lines(text):
        yield title


def heading_lines(text):
    """Yield (zero-based line number, level, title) triples outside fences."""
    for line_number, line in lines_outside_fences(text):
        match = HEADING.match(line)
        if match:
            yield line_number, len(match.group(1)), match.group(2).strip()


def lines_outside_fences(text):
    """Yield (zero-based line number, text) pairs outside fenced code blocks."""
    fence_char = None
    fence_length = 0
    for line_number, line in enumerate(text.splitlines()):
        if fence_char is not None:
            closing = re.match(r"^ {0,3}(`+|~+)[ \t]*$", line)
            if (closing is not None and closing.group(1)[0] == fence_char
                    and len(closing.group(1)) >= fence_length):
                fence_char = None
                fence_length = 0
            continue

        match = FENCE.match(line)
        if match:
            marker = match.group(1)
            fence_char = marker[0]
            fence_length = len(marker)
            continue

        yield line_number, line
