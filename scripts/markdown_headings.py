"""CommonMark heading and link extraction shared by documentation checkers."""

from markdown_it import MarkdownIt


def parse_markdown(text):
    """Parse once and return CommonMark block tokens with unnormalized URLs."""
    parser = MarkdownIt("commonmark")
    parser.normalizeLink = lambda url: url
    return parser.parse(text)


def _inline_text(token):
    if not token.children:
        return token.content
    return "".join(child.content for child in token.children
                   if child.type in ("text", "code_inline", "html_inline", "image"))


def heading_lines(text):
    """Yield (zero-based start line, level, visible title) for ATX and setext headings."""
    yield from heading_lines_from_tokens(parse_markdown(text))


def heading_lines_from_tokens(tokens):
    """Yield heading records from an already parsed token stream."""
    for index, token in enumerate(tokens):
        if token.type == "heading_open" and token.map is not None:
            inline = tokens[index + 1]
            yield token.map[0], int(token.tag[1:]), _inline_text(inline).strip()


def heading_titles(text):
    """Yield visible titles for CommonMark heading blocks."""
    for _line_number, _level, title in heading_lines(text):
        yield title


def link_lines(text):
    """Yield (zero-based block start line, complete destination) pairs."""
    yield from link_lines_from_tokens(parse_markdown(text))


def resource_lines(text):
    """Yield located link and image destinations with their CommonMark type."""
    yield from resource_lines_from_tokens(parse_markdown(text))


def resource_lines_from_tokens(tokens):
    """Yield (line, kind, destination) records for links and images."""
    for token in tokens:
        if token.type != "inline" or token.map is None or not token.children:
            continue
        for child in token.children:
            if child.type == "link_open":
                kind, attribute = "link", "href"
            elif child.type == "image":
                kind, attribute = "image", "src"
            else:
                continue
            destination = child.attrGet(attribute)
            if destination is not None and destination.strip():
                yield token.map[0], kind, destination


def link_lines_from_tokens(tokens):
    """Yield link records from an already parsed token stream."""
    for line_number, kind, destination in resource_lines_from_tokens(tokens):
        if kind == "link":
            yield line_number, destination
