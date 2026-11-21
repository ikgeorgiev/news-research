"""Usage: python scripts/compare_news_observations.py reviewed-input.json output.json"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.source_research.observations import compare, read_document


def main():
    parser=argparse.ArgumentParser(description='Offline, bounded source-arrival comparison; no network or database access.')
    parser.add_argument('input',type=Path)
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    result=compare(read_document(args.input))
    with args.output.open('x',encoding='utf8') as output:
        json.dump(result,output,indent=2,ensure_ascii=False)
        output.write('\n')
    print(f"Compared {result['accepted_first_receipts']} receipts; excluded {len(result['excluded'])} rows. Saved {args.output}")


if __name__=='__main__':
    main()
