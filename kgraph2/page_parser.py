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

        for node in wikicode.nodes:
            # MediaWiki heading
            if isinstance(node, mwparserfromhell.nodes.Heading):
                # Flush pending paragraph
                if current_block:
                    yield Chunk(
                        content="".join(current_block).strip(),
                        index=chunk_index,
                        type=NodeType.PARAGRAPH
                    )
                    chunk_index += 1
                    current_block = []

                # Emit heading chunk (preserve original markup)
                yield Chunk(
                    content=str(node).strip(),
                    index=chunk_index,
                    type=NodeType.HEADING
                )
                chunk_index += 1

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
                type=NodeType.PARAGRAPH
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
                        metadata=block.metadata
                    )
                    chunk_index += 1
