"""Offline GNW body review using the same bounded parser as the collector."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
from app.financialcontent.gnw_body import MAX_BODY, MAX_MANIFEST, aware_time, read_bounded, parse_body

def review_capture(manifest_path):
    manifest_path = Path(manifest_path).resolve()
    raw_manifest = read_bounded(manifest_path, MAX_MANIFEST)
    m = json.loads(raw_manifest)
    if not isinstance(m, dict) or m.get('version') != 1 or m.get('route') != 'financialcontent-gnw-body':
        raise ValueError('Version 1 FC GNW body manifest required')
    if type(m.get('http_status')) is not int or m['http_status'] != 200:
        raise ValueError('Only a retained HTTP 200 body can be parsed; never retry refusals')
    if m.get('classification') not in {'baseline', 'new', 'historical', 'unknown_history'}:
        raise ValueError('Explicit observation classification required')
    if m.get('receipt_kind') not in {'network_response', 'retained_response', 'cached'}:
        raise ValueError('Explicit receipt kind required')
    received = aware_time(m.get('received_at'))
    if m['receipt_kind'] == 'network_response':
        started = aware_time(m.get('request_started_at'))
        if started > received or not isinstance(m.get('clock_id'), str) or not 0 < len(m['clock_id']) <= 120:
            raise ValueError('Ordered request/receipt and clock identity required')
    filename = m.get('payload_file')
    if not isinstance(filename, str) or not filename:
        raise ValueError('Relative payload filename required')
    rel = Path(filename)
    payload = (manifest_path.parent / rel).resolve()
    if rel.is_absolute() or not payload.is_relative_to(manifest_path.parent):
        raise ValueError('Payload must stay inside the capture directory')
    content = read_bounded(payload, MAX_BODY)
    if hashlib.sha256(content).hexdigest() != m.get('payload_sha256'):
        raise ValueError('Payload digest mismatch')
    result = parse_body(content, article_url=m.get('request_url'), expected_title=m.get('expected_title'))
    result['observation'] = {k: m.get(k) for k in ['classification', 'receipt_kind', 'received_at',
        'request_started_at', 'clock_id', 'http_status', 'payload_sha256']}
    result['manifest_sha256'] = hashlib.sha256(raw_manifest).hexdigest()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    result = review_capture(args.manifest)
    encoded = json.dumps(result, ensure_ascii=False, indent=2) + '\n'
    # Never overwrite a prior review or mutate the saved body.
    with args.output.open('x', encoding='utf-8') as stream:
        stream.write(encoded)
    print(json.dumps({'status': result['status'], 'output': str(args.output.resolve()),
        'body_characters': len(result['body_text']), 'blocks': len(result['reader']['blocks']),
        'original_link_candidates': len(result['original_link_candidates']),
        'production_articles_written': 0, 'network_requests': 0}))


if __name__ == '__main__':
    main()
