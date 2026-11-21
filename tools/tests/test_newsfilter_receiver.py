"""Disposable loopback integration; no source requests or production writes."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest

import httpx
sys.path.insert(0,str(Path(__file__).resolve().parent))
from test_newsfilter_live import event


class NewsfilterReceiverTests(unittest.TestCase):
    def test_pair_accept_deduplicate_refusal_stop(self):
        with tempfile.TemporaryDirectory() as temp:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
            command=[sys.executable,str(Path(__file__).resolve().parents[1]/'newsfilter_live.py'),'--store',temp,
                     'serve','--minutes','1','--compatibility-check','--port',str(port)]
            child=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
            pairing=None
            try:
                ready=json.loads(child.stdout.readline());self.assertEqual(ready['status'],'ready')
                pairing=json.loads((Path(temp)/'pairing.json').read_text());base=f'http://127.0.0.1:{port}'
                headers={'Origin':'chrome-extension://'+'a'*32,'X-CEF-Bridge':'1'}
                with httpx.Client(timeout=3,trust_env=False) as client:
                    p=client.post(base+'/pair',json={'code':ready['connection_code'],'client_id':'a'*32},headers=headers)
                    self.assertEqual(p.status_code,200);self.assertEqual(p.json()['url'],base+'/newsfilter')
                    auth={'Authorization':'Bearer '+p.json()['token'],'Origin':headers['Origin']}
                    e=event();r=client.post(base+'/newsfilter',json=e,headers=auth)
                    self.assertEqual(r.status_code,200);self.assertEqual(r.json()['classifications'],{'compatibility_history':1})
                    self.assertEqual(client.post(base+'/newsfilter',json=e,headers=auth).json()['status'],'duplicate_capture')
                    self.assertEqual(client.post(base+'/godel',json=e,headers=auth).status_code,410)
                    self.assertEqual(client.post(base+'/newsfilter',json=e,headers={**auth,'Origin':'https://newsfilter.io'}).status_code,403)
                    self.assertEqual(client.post(base+'/newsfilter',json={**e,'password':'never-save'},headers=auth).status_code,400)
                    e=event('denied');e['status']=403;e.pop('payload')
                    self.assertEqual(client.post(base+'/newsfilter',json=e,headers=auth).json()['status'],'native_error')
                    self.assertEqual(client.post(base+'/newsfilter',json=event('late'),headers=auth).json()['status'],'held')
            finally:
                if pairing:
                    httpx.post(base+'/stop',json={},headers={'Authorization':'Bearer '+pairing['token']},timeout=3,trust_env=False)
                child.wait(timeout=5);child.stdout.close();child.stderr.close()
            self.assertEqual(child.returncode,0)


if __name__=='__main__':unittest.main()
