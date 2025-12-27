import mwparserfromhell
from typing import Iterator, List
from .models import Page, Chunk, NodeType
from langchain_text_splitters import RecursiveCharacterTextSplitter

class PageParserInner:
    def __init__(self, page: Page):
        """
        Initialize the PageParserInner with a Page object.
        Extracts blocks (headings and paragraphs) from the page.

        Args:
            page (Page): The page to be parsed into blocks.
        """
        self.page = page

    def __iter__(self) -> Iterator[Chunk]:
        """
        Iterator that yields coarse blocks (Chunks) from the page content.

        Yields:
            Chunk: A chunk representing either a heading or a block of text.
        """
        wikicode = mwparserfromhell.parse(self.page.raw_content)

        current_block: List[str] = []
        chunk_index = 0
        
        # Track heading hierarchy: list of heading titles.
        # The owner is the last element in this list.
        # By default, the hierarchy starts with the page title (representing level 1).
        hierarchy_stack = [self.page.title]

        for node in wikicode.nodes:
            # MediaWiki heading
            if isinstance(node, mwparserfromhell.nodes.Heading):
                # Flush pending paragraph
                if current_block:
                    yield Chunk(
                        content="".join(current_block).strip(),
                        index=chunk_index,
                        type=NodeType.PARAGRAPH,
                        hierarchy_owner=hierarchy_stack[-1]
                    )
                    chunk_index += 1
                    current_block = []

                # Determine heading level and title
                # MediaWiki headings usually start at level 2 (== Heading ==).
                # We want to maintain a stack where the level corresponds to the stack length.
                heading_level = node.level
                heading_title = node.title.strip()

                # Update hierarchy stack: pop until its length < current heading level
                while len(hierarchy_stack) >= heading_level:
                    hierarchy_stack.pop()
                
                # The hierarchy owner for the HEADING chunk itself is its parent (the current last element)
                yield Chunk(
                    content=str(node).strip(),
                    index=chunk_index,
                    type=NodeType.HEADING,
                    hierarchy_owner=hierarchy_stack[-1]
                )
                chunk_index += 1

                # Append this new heading to the stack to become the new owner for subsequent content
                hierarchy_stack.append(heading_title)

            else:
                # Accumulate text-like nodes
                text = str(node)
                if text.strip() or current_block:
                    current_block.append(text)

        # Flush remaining paragraph
        if current_block:
            yield Chunk(
                content="".join(current_block).strip(),
                index=chunk_index,
                type=NodeType.PARAGRAPH,
                hierarchy_owner=hierarchy_stack[-1]
            )

class PageParser:
    def __init__(self, page: Page):
        """
        Initialize the PageParser with a Page object.
        Uses PageParserInner to get blocks and then LangChain to split them into smaller chunks.

        Args:
            page (Page): The page to be parsed into blocks for vector embedding.
        """
        self.page = page
        self.inner = PageParserInner(page)
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=100,
            length_function=len,
            is_separator_regex=False,
        )

    def __iter__(self) -> Iterator[Chunk]:
        """
        Iterator that yields smaller chunks from the page content.

        Yields:
            Chunk: A chunk representing a piece of text from the page.
        """
        chunk_index = 0
        for block in self.inner:
            # For headings, we probably don't want to split them if they are short,
            # but for consistency and since LangChain handles short text fine:
            
            if block.type == NodeType.HEADING:
                # Usually headings are short, just yield them
                yield Chunk(
                    content=block.content,
                    index=chunk_index,
                    type=NodeType.HEADING,
                    hierarchy_owner=block.hierarchy_owner,
                    metadata=block.metadata
                )
                chunk_index += 1
            else:
                # Split paragraphs using LangChain
                sub_chunks = self.text_splitter.split_text(block.content)
                for sub_content in sub_chunks:
                    yield Chunk(
                        content=sub_content,
                        index=chunk_index,
                        type=NodeType.PARAGRAPH,
                        hierarchy_owner=block.hierarchy_owner,
                        metadata=block.metadata
                    )
                    chunk_index += 1
