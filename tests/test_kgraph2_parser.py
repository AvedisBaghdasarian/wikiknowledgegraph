from kgraph2.models import Page, Node, Link, NodeType
from kgraph2.page_parser import PageParser

def test_parser_basic():
    content = """Title Node
Some introductory paragraph [[Link1]].

== Heading 1 ==
Paragraph under heading [[Link2]].

=== SubHeading ===
More text."""
