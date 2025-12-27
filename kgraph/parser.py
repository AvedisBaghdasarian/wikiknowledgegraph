"""Article text parser: converts article text files into nodes/edges payloads.

This module focuses only on text parsing and produces plain Python structures
that are then written to Neo4j by a separate client. The goal is clarity and
testability.
"""
from typing import List, Tuple, Dict, Iterator
import re
import logging
from .config import DEFAULT_CONFIG


logger = logging.getLogger(__name__)


def split_paragraphs(text: str, max_len: int = DEFAULT_CONFIG.max_paragraph_len, overlap: int = DEFAULT_CONFIG.para_overlap) -> Iterator[str]:
    text = text.strip()
    if not text:
        return
    start = 0
    n = len(text)
    while start < n:
        end = min(start + max_len, n)
        if end < n:
            # try to end on whitespace to avoid breaking words
            while end > start and not text[end - 1].isspace():
                end -= 1
            if end == start:
                # can't find whitespace, force break
                end = min(start + max_len, n)
        # ensure we close any unbalanced wiki links
        chunk = text[start:end]
        if chunk.count('[[') > chunk.count(']]'):
            closing = text.find(']]', end)
            if closing != -1:
                end = closing + 2
            else:
                end = n
        yield text[start:end].strip()
        if end >= n:
            break
        start = max(end - overlap, end)


def eval_hierarchy(line: str, title: str, hierarchy_array: List[Tuple[str,int,int]], headings_data: List[Dict], lineno: int) -> bool:
    m = re.match(r'^(=+)([^=]+)=+$', line)
    if not m:
        return False
    level = len(m.group(1))
    heading_text = m.group(2).strip()
    while hierarchy_array and hierarchy_array[-1][2] >= level:
        hierarchy_array.pop()
    parent = hierarchy_array[-1][0] if hierarchy_array else title
    headings_data.append({'title': title, 'heading': heading_text, 'line': lineno, 'parent': parent})
    hierarchy_array.append((heading_text, lineno, level))
    return True


def get_next_glob(lines: List[str], start_index: int, title: str, hierarchy_array: List[Tuple[str,int,int]], headings_data: List[Dict], max_size: int = DEFAULT_CONFIG.max_paragraph_len) -> Tuple[str, int, bool]:
    n = len(lines)
    if start_index >= n:
        return '', start_index, False
    raw = lines[start_index].rstrip('\n')
    if eval_hierarchy(raw, title, hierarchy_array, headings_data, start_index):
        return '', start_index + 1, True
    if not raw.strip():
        return '', start_index + 1, False
    is_block = raw.strip().startswith(('{|', '|', '!'))
    buf = [raw]
    idx = start_index + 1
    cur_len = len(raw)

    def links_unbalanced(s: str) -> bool:
        return s.count('[[') > s.count(']]')

    while idx < n:
        nxt = lines[idx].rstrip('\n')
        if re.match(r'^(=+)([^=]+)=+$', nxt):
            break
        if not nxt.strip():
            if is_block and cur_len < max_size:
                buf.append(nxt)
                idx += 1
            break
        if cur_len + len(nxt) > max_size:
            joined = '\n'.join(buf + [nxt])
            if links_unbalanced(joined):
                buf.append(nxt)
                cur_len += len(nxt)
                idx += 1
                continue
            break
        buf.append(nxt)
        cur_len += len(nxt)
        idx += 1

    glob_text = '\n'.join(buf).strip()
    return glob_text, idx, False


def process_paragraph_line(line: str, lineno: int, title: str, hierarchy_array: List[Tuple[str,int,int]], paragraphs: List[Dict], hierarchy_edges: List[Dict], link_edges: List[Dict]):
    parent_heading = hierarchy_array[-1][0] if hierarchy_array else title
    for chunk_idx, chunk in enumerate(split_paragraphs(line)):
        node_id = f"{title}:{lineno}:{chunk_idx}"
        paragraphs.append({'id': node_id, 'text': chunk, 'title': title, 'line': lineno, 'chunk_idx': chunk_idx, 'parent_heading': parent_heading})
        hierarchy_edges.append({'child_id': node_id, 'parent': parent_heading, 'title': title})
        for link in re.findall(r'\[\[([^\]|#]+)', chunk):
            link_title = link.split('|')[0].strip()
            link_edges.append({'from_id': node_id, 'to_title': link_title})


def parse_article(filepath: str, max_paragraph_len: int = DEFAULT_CONFIG.max_paragraph_len) -> Dict:
    title = filepath.rsplit('/', 1)[-1].rsplit('.', 1)[0]
    logger.info(f"Parsing article: {title} from {filepath}")
    with open(filepath, 'r', encoding='utf-8') as fh:
        lines = fh.readlines()
    
    logger.debug(f"Loaded {len(lines)} lines for {title}")
    hierarchy: List[Tuple[str,int,int]] = []
    paragraphs: List[Dict] = []
    headings_data: List[Dict] = []
    hierarchy_edges: List[Dict] = []
    link_edges: List[Dict] = []

    i = 0
    while i < len(lines):
        glob_text, next_i, is_heading = get_next_glob(lines, i, title, hierarchy, headings_data, max_size=max_paragraph_len)
        if is_heading:
            i = next_i
            continue
        if not glob_text:
            i = next_i
            continue
        process_paragraph_line(glob_text, i, title, hierarchy, paragraphs, hierarchy_edges, link_edges)
        i = next_i

    logger.info(f"Finished parsing {title}: {len(headings_data)} headings, {len(paragraphs)} paragraphs")
    return {
        'title': title,
        'headings': headings_data,
        'paragraphs': paragraphs,
        'hierarchy': hierarchy_edges,
        'links': link_edges,
    }
