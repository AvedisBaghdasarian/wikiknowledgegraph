from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Iterator
from enum import Enum
import mwxml
import re


class NodeType(Enum):
    TITLE = "Title"
    HEADING = "Heading"
    PARAGRAPH = "Paragraph"


@dataclass(frozen=True)
class Node:
    uid: str  # Unique identifier for the node
    type: NodeType
    properties: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class Link:
    source_uid: str
    target_uid: str
    # property-free as requested

@dataclass
class Chunk:
    content: str
    index: int
    type: NodeType
    hierarchy_owner: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_links(self) -> List[str]:
        """
        Extracts Wikipedia links (double brackets) from the content.
        Returns a unique list of linked page titles.
        """
        # Find all [[target]] or [[target|text]] patterns
        matches = re.findall(r'\[\[([^\]|]+)(?:\|[^\]]*)?\]\]', self.content)
        # Use a set to ensure uniqueness, then back to a list
        return list(set(matches))

@dataclass
class Page:
    title: str
    raw_content: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class XMLMultiPageDoc:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def __iter__(self) -> Iterator[Page]:
        with open(self.file_path, "rb") as f:
            dump = mwxml.Dump.from_file(f)

            for page in dump:
                # skip empty pages
                if page.id is None:
                    continue

                revision = self._latest_revision(page)
                if revision is None:
                    continue

                yield Page(
                    title=page.title,
                    raw_content=revision.text or "",
                    metadata={
                        "page_id": page.id,
                        "revision_id": revision.id,
                        "timestamp": revision.timestamp,
                        "redirect": page.redirect
                    },
                )

    @staticmethod
    def _latest_revision(page: mwxml.Page) -> mwxml.Revision | None:
        latest = None
        for rev in page:
            if rev.text is None:
                continue
            latest = rev
        return latest