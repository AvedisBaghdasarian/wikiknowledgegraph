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
    assert chunks[0].hierarchy_owner == "Test Page"

    # First heading
    assert chunks[1].content == "== Heading 1 =="
    assert chunks[1].type == NodeType.HEADING
    assert chunks[1].index == 1
    assert chunks[1].hierarchy_owner == "Test Page"

    # Second block of text
    assert chunks[2].content == "Line 3\nLine 4"
    assert chunks[2].type == NodeType.PARAGRAPH
    assert chunks[2].index == 2
    assert chunks[2].hierarchy_owner == "Heading 1"

    # Second heading (subheading)
    assert chunks[3].content == "=== Subheading ==="
    assert chunks[3].type == NodeType.HEADING
    assert chunks[3].index == 3
    assert chunks[3].hierarchy_owner == "Heading 1"

    # Third block of text
    assert chunks[4].content == "Line 5"
    assert chunks[4].type == NodeType.PARAGRAPH
    assert chunks[4].index == 4
    assert chunks[4].hierarchy_owner == "Subheading"

def test_page_parser_hierarchy():
    content = """
Intro paragraph.
== Heading 1 ==
H1 paragraph.
=== Subheading 1.1 ===
S1.1 paragraph.
== Heading 2 ==
H2 paragraph.
"""
    page = Page(title="Main Page", raw_content=content)
    parser = PageParser(page)
    chunks = list(parser)

    # Chunks:
    # 0: Intro paragraph (PARAGRAPH) -> owner: 'Main Page'
    # 1: == Heading 1 == (HEADING) -> owner: 'Main Page'
    # 2: H1 paragraph (PARAGRAPH) -> owner: 'Heading 1'
    # 3: === Subheading 1.1 === (HEADING) -> owner: 'Heading 1'
    # 4: S1.1 paragraph (PARAGRAPH) -> owner: 'Subheading 1.1'
    # 5: == Heading 2 == (HEADING) -> owner: 'Main Page'
    # 6: H2 paragraph (PARAGRAPH) -> owner: 'Heading 2'

    assert chunks[0].content == "Intro paragraph."
    assert chunks[0].hierarchy_owner == "Main Page"

    assert chunks[1].content == "== Heading 1 =="
    assert chunks[1].hierarchy_owner == "Main Page"

    assert chunks[2].content == "H1 paragraph."
    assert chunks[2].hierarchy_owner == "Heading 1"

    assert chunks[3].content == "=== Subheading 1.1 ==="
    assert chunks[3].hierarchy_owner == "Heading 1"

    assert chunks[4].content == "S1.1 paragraph."
    assert chunks[4].hierarchy_owner == "Subheading 1.1"

    assert chunks[5].content == "== Heading 2 =="
    assert chunks[5].hierarchy_owner == "Main Page"

    assert chunks[6].content == "H2 paragraph."
    assert chunks[6].hierarchy_owner == "Heading 2"

def test_page_parser_starts_with_heading():
    content = "== Title ==\nSome content"
    page = Page(title="Test", raw_content=content)
    parser = PageParser(page)
    chunks = list(parser)

    assert len(chunks) == 2
    assert chunks[0].content == "== Title =="
    assert chunks[0].type == NodeType.HEADING
    assert chunks[0].hierarchy_owner == "Test"
    assert chunks[1].content == "Some content"
    assert chunks[1].type == NodeType.PARAGRAPH
    assert chunks[1].hierarchy_owner == "Title"

def test_page_parser_no_headings():
    content = "Just some\ntext lines."
    page = Page(title="Test", raw_content=content)
    parser = PageParser(page)
    chunks = list(parser)

    assert len(chunks) == 1
    assert chunks[0].content == content
    assert chunks[0].type == NodeType.PARAGRAPH
    assert chunks[0].hierarchy_owner == "Test"
