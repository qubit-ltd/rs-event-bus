"""Small Markdown heading scanner shared by documentation checkers."""

import re


FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
QUOTE = re.compile(r"^ {0,3}>[ \t]?")
LIST_ITEM = re.compile(r"^( {0,3})(?:[-+*]|\d+[.)])([ \t]+|$)")


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


def _strip_blockquote_prefixes(line):
    """Return block content and the number of enclosing quote markers."""
    depth = 0
    while True:
        quote = QUOTE.match(line)
        if quote is None:
            return line, depth
        line = line[quote.end():]
        depth += 1


def lines_outside_fences(text):
    """Yield block content outside fenced and indented code blocks."""
    fence_char = None
    fence_length = 0
    fence_quote_depth = 0
    fence_list_indent = None
    list_content_indent = None
    list_quote_depth = 0
    indented_code_indent = None
    paragraph_open = False
    paragraph_quote_depth = 0
    for line_number, line in enumerate(text.splitlines()):
        content, quote_depth = _strip_blockquote_prefixes(line)
        if quote_depth != paragraph_quote_depth:
            paragraph_open = False
            paragraph_quote_depth = quote_depth

        if fence_char is not None:
            indentation = len(content) - len(content.lstrip(" "))
            in_quote = quote_depth >= fence_quote_depth
            in_list = (fence_list_indent is None
                       or not content.strip()
                       or indentation >= fence_list_indent)
            if in_quote and in_list:
                if fence_list_indent is not None and content.strip():
                    content = content[fence_list_indent:]
                closing = re.match(r"^ {0,3}(`+|~+)[ \t]*$", content)
                if (closing is not None and closing.group(1)[0] == fence_char
                        and len(closing.group(1)) >= fence_length):
                    fence_char = None
                    fence_length = 0
                    fence_list_indent = None
                    fence_quote_depth = 0
                continue

            # A container boundary ends an unclosed fence implicitly. Reprocess
            # this line as ordinary Markdown after forgetting that list item.
            fence_char = None
            fence_length = 0
            fence_list_indent = None
            fence_quote_depth = 0
            paragraph_open = False
            if list_content_indent is not None and quote_depth < list_quote_depth:
                list_content_indent = None

        list_item = LIST_ITEM.match(content)
        if list_item is not None:
            marker_end = list_item.end()
            list_content_indent = marker_end
            list_quote_depth = quote_depth
            content = content[marker_end:]
            paragraph_open = False
        elif list_content_indent is not None:
            if quote_depth < list_quote_depth:
                list_content_indent = None
            elif not content.strip():
                content = ""
            else:
                indentation = len(content) - len(content.lstrip(" "))
                if indentation >= list_content_indent:
                    content = content[list_content_indent:]
                else:
                    list_content_indent = None

        if indented_code_indent is not None:
            if not content.strip():
                paragraph_open = False
                continue
            indentation = len(content) - len(content.lstrip(" "))
            if indentation >= indented_code_indent:
                continue
            indented_code_indent = None

        match = FENCE.match(content)
        if match:
            marker = match.group(1)
            if marker[0] == "`" and "`" in match.group(2):
                yield line_number, content
                paragraph_open = True
                continue
            fence_char = marker[0]
            fence_length = len(marker)
            fence_quote_depth = quote_depth
            fence_list_indent = list_content_indent
            paragraph_open = False
            continue

        indentation = len(content) - len(content.lstrip(" "))
        if content.strip() and indentation >= 4:
            if paragraph_open:
                yield line_number, content
                continue
            indented_code_indent = 4
            paragraph_open = False
            continue

        yield line_number, content
        if not content.strip() or HEADING.match(content):
            paragraph_open = False
        else:
            paragraph_open = True
