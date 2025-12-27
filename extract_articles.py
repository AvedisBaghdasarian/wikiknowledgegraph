import xml.etree.ElementTree as ET
import os
import logging

# Path to the Wikipedia XML dump
XML_PATH = "enwiki-20250501-pages-articles-multistream11.xml-p5399367p6899366"
OUTPUT_DIR = "articles_output"
MAX_ARTICLES = 50000

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(message)s')

def sanitize_filename(title):
    # Replace problematic characters for filenames
    return "_".join(title.split()).replace('/', '_')

def main():
    logging.info(f"Parsing XML file: {XML_PATH}")
    context = ET.iterparse(XML_PATH, events=("start", "end"))
    in_page = False
    article_count = 0
    title = None
    text = None
    for event, elem in context:
        tag = elem.tag
        if tag[0] == '{':
            tag = tag.split('}', 1)[1]  # Remove namespace
        if event == "start" and tag == "page":
            in_page = True
            title = None
            text = None
        elif event == "end" and in_page:
            if tag == "title":
                title = elem.text or "untitled"
            elif tag == "text":
                text = elem.text or ""
            elif tag == "page":
                # Write article when </page> is reached
                if title is not None and text is not None:
                    filename = sanitize_filename(title)[:100]  # Limit filename length
                    filepath = os.path.join(OUTPUT_DIR, f"{filename}.txt")
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(text)
                    article_count += 1
                    logging.info(f"Saved article {article_count}: {title} -> {filepath}")
                    if article_count >= MAX_ARTICLES:
                        break
                in_page = False
                elem.clear()  # Free memory
    logging.info(f"Extraction complete. {article_count} articles saved to '{OUTPUT_DIR}'.")

if __name__ == "__main__":
    main()
