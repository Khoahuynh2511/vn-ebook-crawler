import html
import re
import uuid
import zipfile
from pathlib import Path
from typing import List, Tuple

STYLE_CSS = """body {
  font-family: Georgia, "Times New Roman", serif;
  line-height: 1.65;
  text-align: justify;
  margin: 1.2em;
  color: #1a1a1a;
}
h1, h2, h3 {
  text-align: left;
  line-height: 1.3;
  margin-top: 1.2em;
  margin-bottom: 0.6em;
}
p {
  margin: 0.6em 0;
  text-indent: 1.2em;
}
"""


def _sanitize_chapter_html(body_html: str) -> str:
    """Convert raw chapter HTML or plain text into clean XHTML paragraphs."""
    text = body_html.strip()
    if not text:
        return "<p>(Nội dung trống)</p>"

    # Strip script/style tags if any slipped through
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", text, flags=re.I | re.S)

    # If the text already contains <p> or <br>, normalize into clean <p> blocks
    if re.search(r"<(p|div|br)\b", text, re.I):
        # Replace <br> variants with newline markers, strip remaining tags
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
        text = re.sub(r"</(?:p|div|h[1-6]|li)>", "\n", text, flags=re.I)
        text = re.sub(r"<[^>]+>", "", text)
        text = html.unescape(text)
    else:
        text = html.unescape(text)

    paragraphs = [
        f"<p>{html.escape(line.strip())}</p>"
        for line in re.split(r"\r?\n+", text)
        if line.strip()
    ]
    return "\n".join(paragraphs) if paragraphs else "<p>(Nội dung trống)</p>"


def build_epub_from_chapters(
    title: str,
    author: str,
    description: str,
    chapters: List[Tuple[str, str]],
    output_path: Path,
) -> Path:
    """Build a standards-compliant EPUB3/EPUB2 archive using Python's stdlib zipfile."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(".epub.part")

    book_id = f"urn:uuid:{uuid.uuid4()}"
    safe_title = html.escape(title or "Sách Tiếng Việt")
    safe_author = html.escape(author or "Khuyết danh")
    safe_desc = html.escape(description or "")

    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""

    manifest_items = [
        '<item id="css" href="style.css" media-type="text/css"/>',
        '<item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>',
        '<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>',
    ]
    spine_items = []
    nav_links = []
    ncx_points = []
    chapter_files: List[Tuple[str, str]] = []

    for idx, (chap_title, chap_body) in enumerate(chapters, start=1):
        chap_id = f"chap{idx}"
        chap_file = f"chapter_{idx}.xhtml"
        clean_chap_title = html.escape(chap_title.strip() or f"Chương {idx}")
        clean_body = _sanitize_chapter_html(chap_body)

        xhtml = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="vi" lang="vi">
<head>
  <meta charset="utf-8"/>
  <title>{clean_chap_title}</title>
  <link rel="stylesheet" type="text/css" href="style.css"/>
</head>
<body>
  <h2>{clean_chap_title}</h2>
  {clean_body}
</body>
</html>
"""
        chapter_files.append((f"OEBPS/{chap_file}", xhtml))
        manifest_items.append(
            f'<item id="{chap_id}" href="{chap_file}" media-type="application/xhtml+xml"/>'
        )
        spine_items.append(f'<itemref idref="{chap_id}"/>')
        nav_links.append(f'<li><a href="{chap_file}">{clean_chap_title}</a></li>')
        ncx_points.append(
            f"""    <navPoint id="navPoint-{idx}" playOrder="{idx}">
      <navLabel><text>{clean_chap_title}</text></navLabel>
      <content src="{chap_file}"/>
    </navPoint>"""
        )

    nav_xhtml = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops" xml:lang="vi" lang="vi">
<head>
  <meta charset="utf-8"/>
  <title>Mục lục - {safe_title}</title>
  <link rel="stylesheet" type="text/css" href="style.css"/>
</head>
<body>
  <nav epub:type="toc" id="toc">
    <h1>Mục lục</h1>
    <ol>
      {"".join(nav_links)}
    </ol>
  </nav>
</body>
</html>
"""

    toc_ncx = f"""<?xml version="1.0" encoding="UTF-8"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head>
    <meta name="dtb:uid" content="{book_id}"/>
    <meta name="dtb:depth" content="1"/>
    <meta name="dtb:totalPageCount" content="0"/>
    <meta name="dtb:maxPageNumber" content="0"/>
  </head>
  <docTitle><text>{safe_title}</text></docTitle>
  <navMap>
{chr(10).join(ncx_points)}
  </navMap>
</ncx>
"""

    content_opf = f"""<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="BookId" xml:lang="vi">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:identifier id="BookId">{book_id}</dc:identifier>
    <dc:title>{safe_title}</dc:title>
    <dc:creator>{safe_author}</dc:creator>
    <dc:language>vi</dc:language>
    <dc:description>{safe_desc}</dc:description>
  </metadata>
  <manifest>
    {chr(10).join("    " + item for item in manifest_items)}
  </manifest>
  <spine toc="ncx">
    {chr(10).join("    " + item for item in spine_items)}
  </spine>
</package>
"""

    with zipfile.ZipFile(tmp_path, "w") as zf:
        # EPUB specification requires 'mimetype' to be first and uncompressed
        zf.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        zf.writestr("META-INF/container.xml", container_xml, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("OEBPS/style.css", STYLE_CSS, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("OEBPS/nav.xhtml", nav_xhtml, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("OEBPS/toc.ncx", toc_ncx, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr("OEBPS/content.opf", content_opf, compress_type=zipfile.ZIP_DEFLATED)
        for arcname, content in chapter_files:
            zf.writestr(arcname, content, compress_type=zipfile.ZIP_DEFLATED)

    if output_path.exists():
        output_path.unlink()
    tmp_path.rename(output_path)
    return output_path
