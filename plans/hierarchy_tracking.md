# Plan: Heading Hierarchy Tracking

Add hierarchy tracking to Wikipedia page parsing to maintain context for chunks.

## User Review Required

> [!IMPORTANT]
> The hierarchy will be a list of strings: `['Page Title', 'Heading 1', 'Subheading 1.1']`.
> For a heading chunk itself, its hierarchy will point to its parent (the hierarchy it belongs to).

## Proposed Changes

### 1. Models
- Update `Chunk` in [`kgraph2/models.py`](kgraph2/models.py):
    - Add `hierarchy: List[str] = field(default_factory=list)`

### 2. Page Parser
- Update `PageParserInner` in [`kgraph2/page_parser.py`](kgraph2/page_parser.py):
    - Track `hierarchy_stack` (list of `(level, title)` tuples).
    - Initial stack: `[(0, page_title)]`.
    - On Heading (level `L`):
        - Pop from stack while `stack[-1].level >= L`.
        - Hierarchy for this heading chunk = `[item.title for item in stack]`.
        - Push `(L, heading_title)` to stack.
    - On Paragraph:
        - Hierarchy = `[item.title for item in stack]`.

- Update `PageParser` in [`kgraph2/page_parser.py`](kgraph2/page_parser.py):
    - Copy `hierarchy` from `block` to all generated chunks.

### 3. Testing
- Add `test_hierarchy_tracking` to [`tests/test_page_parser.py`](tests/test_page_parser.py).

## Verification Plan

### Automated Tests
- Run `pytest tests/test_page_parser.py` to ensure hierarchy is correctly assigned.
- Verify:
    - Root paragraphs have `['Page Title']`.
    - Level 2 headings have `['Page Title']`.
    - Paragraphs under Level 2 have `['Page Title', 'Heading']`.
    - Level 3 headings have `['Page Title', 'Heading']`.
