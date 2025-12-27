import pytest
from kgraph2.models import Page, NodeType
from kgraph2.page_parser import PageParser

def test_page_parser_iterator():
    content = """Line 1
Line 2

== Heading 1 ==
Line 3
Line 4
=== Subheading ===
Line 5
"""
    page = Page(title="Test Page", raw_content=content)
    parser = PageParser(page)
    chunks = list(parser)

    assert len(chunks) == 5
    
    # First block of text
    assert chunks[0].content == "Line 1\nLine 2"
    assert chunks[0].type == NodeType.PARAGRAPH
    assert chunks[0].index == 0

    # First heading
    assert chunks[1].content == "== Heading 1 =="
    assert chunks[1].type == NodeType.HEADING
    assert chunks[1].index == 1

    # Second block of text
    assert chunks[2].content == "Line 3\nLine 4"
    assert chunks[2].type == NodeType.PARAGRAPH
    assert chunks[2].index == 2

    # Second heading (subheading)
    assert chunks[3].content == "=== Subheading ==="
    assert chunks[3].type == NodeType.HEADING
    assert chunks[3].index == 3

    # Third block of text
    assert chunks[4].content == "Line 5"
    assert chunks[4].type == NodeType.PARAGRAPH
    assert chunks[4].index == 4

def test_page_parser_starts_with_heading():
    content = "== Title ==\nSome content"
    page = Page(title="Test", raw_content=content)
    parser = PageParser(page)
    chunks = list(parser)

    assert len(chunks) == 2
    assert chunks[0].content == "== Title =="
    assert chunks[0].type == NodeType.HEADING
    assert chunks[1].content == "Some content"
    assert chunks[1].type == NodeType.PARAGRAPH

def test_page_parser_no_headings():
    content = "Just some\ntext lines."
    page = Page(title="Test", raw_content=content)
    parser = PageParser(page)
    chunks = list(parser)

    assert len(chunks) == 1
    assert chunks[0].content == content
    assert chunks[0].type == NodeType.PARAGRAPH
