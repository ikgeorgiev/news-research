from concurrent.futures import ThreadPoolExecutor
import hashlib,json
from pathlib import Path
import pytest

FIXTURES = Path(__file__).parent / 'fixtures/financialcontent'

from app.financialcontent.financialcontent_adapter import parse_article
from app.financialcontent.structured_body import build_reader_document,write_reader_archive,document_text

CASES=[
 (FIXTURES/'blackrock.html',
  'https://markets.chroniclejournal.com/thepilotnews/article/bizwire-2026-10-30-certain-blackrock-closed-end-funds-announce-estimated-sources-of-distributions',
  'Certain BlackRock Closed-End Funds Announce Estimated Sources of Distributions',7,1634),
 (FIXTURES/'bny.html',
  'https://markets.chroniclejournal.com/thepilotnews/article/bizwire-2026-10-29-bny-mellon-municipal-bond-closed-end-funds-declare-distributions',
  'BNY Mellon Municipal Bond Closed-End Funds Declare Distributions',1,1617),
]

def parsed(case=CASES[0]):
    file,url,title,_,_=case
    entry=parse_article(file.read_bytes(),article_url=url,expected_title=title)
    entry['_reader_document']=build_reader_document(file.read_bytes(),entry)
    return entry

@pytest.mark.parametrize('case',CASES)
def test_real_saved_bodies_preserve_every_text_token_and_all_tables(case):
    entry=parsed(case);doc=entry['_reader_document']
    assert document_text(doc['blocks'])==entry['summary']
    assert sum(x['type']=='table' for x in doc['blocks'])==case[3]
    assert doc['source_html_sha256']==hashlib.sha256(case[0].read_bytes()).hexdigest()
    assert len(json.dumps(doc).encode())<1024*1024

def test_real_bny_table_keeps_funds_separate_with_correct_values():
    doc=parsed(CASES[1])['_reader_document'];table=next(x for x in doc['blocks'] if x['type']=='table')
    assert [[c['text'] for c in row][1:] for row in table['rows'][1:]]==[['DSM','$0.030','--'],['LEO','$0.030','--']]

def test_reader_strips_active_elements_and_never_retains_html_attributes():
    content=b'<div itemprop="articleBody"><p onclick="evil()">Safe &lt;script&gt; text</p><script>evil()</script><img src="https://tracker.invalid/pixel"><iframe src="https://tracker.invalid"></iframe></div>'
    entry={**parsed(CASES[1]),'summary':'Safe <script> text'}
    doc=build_reader_document(content,entry)
    assert doc['blocks']==[{'type':'paragraph','text':'Safe <script> text'}]
    assert 'tracker.invalid' not in json.dumps(doc) and 'onclick' not in json.dumps(doc)

@pytest.mark.parametrize('body',[
 '<div itemprop="articleBody"><table><tr><td colspan="999999">x</td></tr></table></div>',
 '<div itemprop="articleBody"><table><tr><td><table><tr><td>x</td></tr></table></td></tr></table></div>',
 '<div itemprop="articleBody"><p>x</p></div><div itemprop="articleBody"><p>x</p></div>',
])
def test_ambiguous_or_unbounded_structure_stays_out_of_reader(body):
    with pytest.raises(ValueError):build_reader_document(body.encode(),{**parsed(CASES[1]),'summary':'x'})

def test_text_loss_is_rejected_instead_of_silently_rewriting_evidence():
    with pytest.raises(ValueError,match='differs'):
        build_reader_document(b'<div itemprop="articleBody">Different content</div>',parsed(CASES[1]))

def test_archive_has_one_complete_first_write_under_concurrency(tmp_path):
    entry=parsed();before=json.dumps(entry,sort_keys=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results=list(pool.map(lambda _:write_reader_archive(tmp_path,1634,entry),range(8)))
    assert results.count('archived')==1 and results.count('already_archived')==7
    saved=json.loads((tmp_path/'1634.json').read_text(encoding='utf-8'))
    assert saved['article_id']==1634 and saved['wire_key']==entry['id']
    assert len(list(tmp_path.iterdir()))==1
    assert json.dumps(entry,sort_keys=True)==before

def test_existing_reader_cannot_be_overwritten_by_different_original_release(tmp_path):
    entry=parsed();write_reader_archive(tmp_path,1634,entry)
    before=(tmp_path/'1634.json').read_bytes()
    other=parsed(CASES[1])
    with pytest.raises(ValueError,match='identity conflict'):write_reader_archive(tmp_path,1634,other)
    assert (tmp_path/'1634.json').read_bytes()==before

def test_old_flattened_entries_are_not_reconstructed_or_replayed(tmp_path):
    entry=parsed();entry.pop('_reader_document')
    assert write_reader_archive(tmp_path,1634,entry)=='no_structured_body'
    assert list(tmp_path.iterdir())==[]


def test_oversized_existing_archive_is_preserved_without_decoding(tmp_path):
    destination = tmp_path / '1.json'
    destination.write_bytes(b'x' * (1024 * 1024 + 1))
    with pytest.raises(ValueError, match='exceeds'):
        write_reader_archive(tmp_path, 1, parsed(CASES[1]))
    assert list(tmp_path.iterdir()) == [destination]
    assert destination.stat().st_size == 1024 * 1024 + 1


def test_boolean_existing_article_id_cannot_claim_the_first_archive(tmp_path):
    entry = parsed(CASES[1])
    destination = tmp_path / '1.json'
    destination.write_text(json.dumps({'article_id': True, 'wire_key': entry['id']}))
    with pytest.raises(ValueError, match='identity conflict'):
        write_reader_archive(tmp_path, 1, entry)
    assert list(tmp_path.iterdir()) == [destination]


def test_fsync_failure_leaves_no_archive_or_temporary_file(tmp_path, monkeypatch):
    def fail_fsync(fd):
        raise OSError('disk flush failed')

    monkeypatch.setattr('app.financialcontent.structured_body.os.fsync', fail_fsync)
    with pytest.raises(OSError, match='disk flush failed'):
        write_reader_archive(tmp_path, 1, parsed(CASES[1]))
    assert list(tmp_path.iterdir()) == []
