import pytest
import os
import tempfile
from kgraph2.models import XMLMultiPageDoc, Chunk, NodeType

XML_CONTENT = """<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.11/ http://www.mediawiki.org/xml/export-0.11.xsd" version="0.11" xml:lang="en">
  <siteinfo>
    <sitename>Wikipedia</sitename>
    <dbname>enwiki</dbname>
    <base>https://en.wikipedia.org/wiki/Main_Page</base>
    <generator>MediaWiki 1.44.0-wmf.25</generator>
    <case>first-letter</case>
    <namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
      <namespace key="-1" case="first-letter">Special</namespace>
      <namespace key="0" case="first-letter" />
    </namespaces>
  </siteinfo>
  <page>
    <title>House Tornado (album)</title>
    <ns>0</ns>
    <id>5399372</id>
    <redirect title="House Tornado" />
    <revision>
      <id>56586451</id>
      <timestamp>2006-06-03T00:06:34Z</timestamp>
      <contributor>
        <username>Worden</username>
        <id>205118</id>
      </contributor>
      <comment>moved [[House Tornado (album)]] to [[House Tornado]]: no other House Tornados exist</comment>
      <origin>56586451</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes="27" sha1="7wv0tmkb6o21hld1u6wveqptzh2w88d" xml:space="preserve">#REDIRECT [[House Tornado]]</text>
      <sha1>7wv0tmkb6o21hld1u6wveqptzh2w88d</sha1>
    </revision>
  </page>
  <page>
    <title>State of Change</title>
    <ns>0</ns>
    <id>5399373</id>
    <revision>
      <id>1178291230</id>
      <parentid>1177307237</parentid>
      <timestamp>2023-10-02T19:06:25Z</timestamp>
      <contributor>
        <username>Bigwhofan</username>
        <id>12635625</id>
      </contributor>
      <comment>/* External links */</comment>
      <origin>1178291230</origin>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text bytes="2619" sha1="gwrw4aaf5eltrs8zqj1yhm4f4ja3g6c" xml:space="preserve">{{Short description|1994 novel by Christopher Bulis}}
{{Use dmy dates|date=April 2022}}
{{Infobox book
|name = State of Change
|image = State of Change.jpg
|caption = Cover Art
|author = [[Christopher Bulis]]
|series = ''[[Doctor Who]]'' book:<br />[[Virgin Missing Adventures]]
|release_number = 5
|subject = Featuring:<br />[[Sixth Doctor]]<br />[[Peri Brown|Peri]]
|set_in = Period between<br />''[[Revelation of the Daleks]]'' and<br />''[[The Trial of a Time Lord]]''
|release_date = December 1994
|publisher = [[Virgin Books]]
| isbn =  0-426-20431-X
|preceded_by = [[The Crystal Bucephalus]]
|followed_by = [[The Romance of Crime]]
}}
'''''State of Change''''' is an original novel written by [[Christopher Bulis]] and based on the long-running British [[science fiction on television|science fiction television]] series ''[[Doctor Who]]''. The novel features the [[Sixth Doctor]] and [[Peri Brown|Peri]], although the dimensional instability of the realm they are currently visiting causes the Doctor to briefly regress through his first five incarnations; the Sixth Doctor also spends a great deal of time allowing the personality of the [[Third Doctor]] to take control of his body when he is forced to fight.

==Plot==
10 BC. The Doctor and Peri land in ancient Rome, specifically in the tomb of [[Cleopatra]]. But something is very wrong: The tomb walls depict steam-driven galleys and other disturbing anachronisms. The time travellers discover that Rome has advanced far beyond its natural means, and they must recruit the aid of [[Caesarion|Ptolemy Caesar]] to prevent his half-siblings, [[Alexander Helios]] and [[Cleopatra Selene II]], from waging a potentially world-ending war with each other. But the anomalies don't just end with Rome, as The Doctor and Peri experience changes of their own...
</text>
    </revision>
  </page>
</mediawiki>
"""

def test_chunk_get_links():
    content = "This is a [[link]] to [[Something|Else]] and another [[link]]."
    chunk = Chunk(
        content=content,
        index=0,
        type=NodeType.PARAGRAPH,
        hierarchy_owner="Test Page"
    )
    links = chunk.get_links()
    assert len(links) == 2
    assert "link" in links
    assert "Something" in links

def test_xml_multi_page_doc():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
        f.write(XML_CONTENT)
        temp_path = f.name

    try:
        doc = XMLMultiPageDoc(temp_path)
        pages = list(doc)

        assert len(pages) == 2
        
        # Check first page
        assert pages[0].title == "House Tornado (album)"
        assert str(pages[0].metadata['page_id']) == "5399372"
        assert "#REDIRECT [[House Tornado]]" in pages[0].raw_content

        # Check second page
        assert pages[1].title == "State of Change"
        assert str(pages[1].metadata['page_id']) == "5399373"
        assert "State of Change" in pages[1].raw_content
        assert "[[Christopher Bulis]]" in pages[1].raw_content

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
