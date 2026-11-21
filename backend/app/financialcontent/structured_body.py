"""Retain readable structure as inert JSON, separately from ticker/body evidence."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from lxml import html

MAX_ARCHIVE_BYTES=1024*1024
DROP={'script','style','iframe','object','embed','svg','math','form','input','button','noscript','template','img'}
BLOCK={'p','div','section','article','blockquote','h1','h2','h3','h4','h5','h6','ul','ol','table','hr'}

def tidy(value):return ' '.join(value.split())
def text_of(node):return tidy(' '.join(node.itertext()))

def document_text(blocks):
    parts=[]
    for block in blocks:
        if block['type']=='table':
            if block['caption']:parts.append(block['caption'])
            parts.extend(cell['text'] for row in block['rows'] for cell in row)
        elif block['type']=='list':parts.extend(block['items'])
        else:parts.append(block['text'])
    return tidy(' '.join(parts))

def build_reader_document(content,entry):
    if len(content)>MAX_ARCHIVE_BYTES:raise ValueError('Body exceeds bound')
    tree=html.fromstring(content.decode('utf-8'))
    bodies=tree.xpath('//*[@itemprop="articleBody"]')
    if len(bodies)!=1:raise ValueError('Ambiguous body')
    body=bodies[0]
    for node in list(body.iterdescendants()):
        if isinstance(node.tag,str) and node.tag.lower() in DROP:
            node.drop_tree()
    if len(list(body.iter()))>30000:raise ValueError('Excessive body structure')
    blocks=[]

    def emit_text(text,kind='paragraph',**extra):
        value=tidy(text)
        if value:blocks.append({'type':kind,'text':value,**extra})

    def walk(node,depth=0):
        if depth>40:raise ValueError('Excessive nesting')
        tag=node.tag.lower() if isinstance(node.tag,str) else ''
        if tag=='table':
            if node.xpath('.//table'):raise ValueError('Nested tables need separate review')
            rows=[]
            for row in node.xpath('.//tr'):
                cells=[]
                for cell in row.xpath('./td|./th'):
                    def span(name):
                        raw=cell.get(name,'1')
                        if not raw.isdigit() or not 1<=int(raw)<=40:raise ValueError('Invalid table span')
                        return int(raw)
                    cells.append({'text':text_of(cell),'header':cell.tag.lower()=='th',
                                  'colspan':span('colspan'),'rowspan':span('rowspan')})
                if len(cells)>80:raise ValueError('Excessive table width')
                if cells:rows.append(cells)
            if len(rows)>2000:raise ValueError('Excessive table length')
            if rows:
                captions=node.xpath('./caption')
                blocks.append({'type':'table','caption':text_of(captions[0]) if captions else '', 'rows':rows})
            return
        if tag in {'ul','ol'}:
            items=[text_of(x) for x in node.xpath('./li')]
            if items:blocks.append({'type':'list','ordered':tag=='ol','items':items})
            return
        if tag in {'h1','h2','h3','h4','h5','h6'}:
            emit_text(text_of(node),'heading',level=min(4,max(2,int(tag[1]))));return
        # Inline elements stay in their original paragraph; block wrappers recurse.
        pending=[node.text or '']
        for child in node:
            child_tag=child.tag.lower() if isinstance(child.tag,str) else ''
            if child_tag in BLOCK:
                emit_text(' '.join(pending));pending=[];walk(child,depth+1)
            elif child_tag=='br':pending.append('\n')
            else:pending.append(text_of(child) if isinstance(child.tag,str) else '')
            pending.append(child.tail or '')
        emit_text(' '.join(pending))
    walk(body)
    if not blocks or len(blocks)>2000:raise ValueError('Invalid body blocks')
    if document_text(blocks)!=tidy(entry['summary']):raise ValueError('Structured text differs from validated body')
    doc={'version':1,'wire_key':entry['id'],'title':entry['title'],'source_url':entry['link'],
         'original_url':entry['source']['href'],'published_at':entry['published'],'blocks':blocks,
         'source_html_sha256':hashlib.sha256(content).hexdigest(),
         'text_sha256':hashlib.sha256(entry['summary'].encode()).hexdigest()}
    if len(json.dumps(doc,ensure_ascii=False).encode())>MAX_ARCHIVE_BYTES:raise ValueError('Archive exceeds bound')
    return doc


def write_reader_archive(directory,article_id,entry):
    """Called only after the existing original-ID/ticker/dedup commit succeeds."""
    doc=entry.get('_reader_document')
    if doc is None:return 'no_structured_body'
    if not re.fullmatch(r'bw:\d+:en',entry.get('id','')) or doc['wire_key']!=entry['id'] or doc['title']!=entry['title'] or doc['source_url']!=entry['link']:
        raise ValueError('Reader identity mismatch')
    return save_reader_document(directory, article_id, doc)


def save_reader_document(directory, article_id, document):
    """Publish the complete document once, including when multiple sources race."""
    if type(article_id) is not int or article_id <= 0:
        raise ValueError('Invalid article ID')
    data = json.dumps({**document, 'article_id': article_id}, ensure_ascii=False).encode('utf-8')
    if len(data) > MAX_ARCHIVE_BYTES:
        raise ValueError('Reader archive exceeds bound')
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f'{article_id}.json'
    stream = tempfile.NamedTemporaryFile(dir=directory, prefix='.reader-', delete=False)
    try:
        with stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(stream.name, destination)
        except FileExistsError:
            with destination.open('rb') as saved:
                raw = saved.read(MAX_ARCHIVE_BYTES + 1)
            if len(raw) > MAX_ARCHIVE_BYTES:
                raise ValueError('Existing reader archive exceeds bound')
            existing = json.loads(raw.decode('utf-8'))
            if (not isinstance(existing, dict) or type(existing.get('article_id')) is not int
                    or existing['article_id'] != article_id or existing.get('wire_key') != document['wire_key']):
                raise ValueError('Existing reader identity conflict')
            return 'already_archived'
        return 'archived'
    finally:
        Path(stream.name).unlink()
