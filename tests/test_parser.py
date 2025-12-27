import pytest
from kgraph2.models import Page, Node, Link, NodeType
from kgraph2.page_parser import PageParser as WikiParser

def test_parser_basic_title():
    content = "This is a simple page."
    page = Page(title="Test Page", raw_content=content)
    parser = WikiParser(page)
    
    results = list(parser)
    
    # First item should be the title node
    title_node = results[0]
    assert isinstance(title_node, Node)
    assert title_node.type == NodeType.TITLE
    assert title_node.uid == "TITLE:Test Page"
    assert title_node.properties["name"] == "Test Page"

def test_parser_headings_hierarchy():
    content = """
== Section 1 ==
Para 1
=== Subsection 1.1 ===
Para 1.1
== Section 2 ==
Para 2
"""
    page = Page(title="Hierarchy", raw_content=content.strip())
    parser = WikiParser(page)
    results = list(parser)
    
    # Title
    # Heading 1 (Section 1) -> Title
    # Paragraph 1 -> Heading 1
    # Heading 2 (Subsection 1.1) -> Heading 1
    # Paragraph 1.1 -> Heading 2
    # Heading 3 (Section 2) -> Title
    # Paragraph 2 -> Heading 3

    nodes = [r for r in results if isinstance(r, Node)]
    links = [r for r in results if isinstance(r, Link)]

    # Check headings
    h1 = next(n for n in nodes if n.type == NodeType.HEADING and n.properties["heading"] == "Section 1")
    h1_1 = next(n for n in nodes if n.type == NodeType.HEADING and n.properties["heading"] == "Subsection 1.1")
    h2 = next(n for n in nodes if n.type == NodeType.HEADING and n.properties["heading"] == "Section 2")

    # Check links
    # h1 -> Title
    assert any(l.source_uid == h1.uid and l.target_uid == "TITLE:Hierarchy" for l in links)
    # h1_1 -> h1
    assert any(l.source_uid == h1_1.uid and l.target_uid == h1.uid for l in links)
    # h2 -> Title
    assert any(l.source_uid == h2.uid and l.target_uid == "TITLE:Hierarchy" for l in links)

def test_parser_wiki_links():
    content = "Hello [[World]] and [[Python|Language]]."
    page = Page(title="Links", raw_content=content)
    parser = WikiParser(page)
    results = list(parser)
    
    links = [r for r in results if isinstance(r, Link)]
    
    # Wiki links
    assert any(l.target_uid == "TITLE:World" for l in links)
    # Note: parser regex is re.compile(r'\[\[([^\]|#]+)'), so [[Python|Language]] -> TITLE:Python
    assert any(l.target_uid == "TITLE:Python" for l in links)

def test_parser_chunks_and_overlap():
    # max_paragraph_len=20, overlap=0 (to avoid infinite loop if that's the issue)
    content = "This is a long paragraph that should be split into multiple chunks."
    page = Page(title="Chunks", raw_content=content)
    parser = WikiParser(page, max_paragraph_len=20, overlap=0)
    
    results = list(parser)
    para_nodes = [r for r in results if isinstance(r, Node) and r.type == NodeType.PARAGRAPH]
    
    assert len(para_nodes) > 1
    texts = [n.properties["text"] for n in para_nodes]
    assert "".join(texts).replace(" ", "") == content.replace(" ", "")

def test_parser_290_number_reference():
    content = """{{Infobox number|number=290}}
'''290''' ('''two hundred [and] ninety''') is the [[natural number]] following [[289 (number)|289]] and preceding [[291 (number)|291]].

==In mathematics==
The product of three primes, 290 is a [[sphenic number]].
"""
    page = Page(title="290 (number)", raw_content=content.strip())
    parser = WikiParser(page)
    results = list(parser)
    
    nodes = [r for r in results if isinstance(r, Node)]
    links = [r for r in results if isinstance(r, Link)]
    
    # Should have Title node
    assert any(n.uid == "TITLE:290 (number)" for n in nodes)
    
    # Should have Heading node
    assert any(n.properties.get("heading") == "In mathematics" for n in nodes)
    
    # Should have links to other numbers
    assert any(l.target_uid == "TITLE:natural number" for l in links)
    assert any(l.target_uid == "TITLE:289 (number)" for l in links)
    assert any(l.target_uid == "TITLE:291 (number)" for l in links)
    assert any(l.target_uid == "TITLE:sphenic number" for l in links)

def test_parser_unbalanced_links_splitting():
    # If a chunk ends inside a [[link]], it should expand to include ]]
    content = "This is a [[Very Long Link Name That Might Be Split]] across chunks."
    page = Page(title="Unbalanced", raw_content=content)
    # Force split inside the link
    parser = WikiParser(page, max_paragraph_len=20) 
    
    results = list(parser)
    para_nodes = [r for r in results if isinstance(r, Node) and r.type == NodeType.PARAGRAPH]
    
    # The first chunk should contain the whole link or at least not be unbalanced
    for node in para_nodes:
        text = node.properties["text"]
        assert text.count("[[") == text.count("]]")
