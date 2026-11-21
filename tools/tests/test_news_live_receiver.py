"""Loopback-only integration checks; owns and concludes its disposable process."""
import importlib.util
from contextlib import closing
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import httpx

SPEC=importlib.util.spec_from_file_location('fixtures',Path(__file__).with_name('test_news_live.py'))
F=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(F)


class ReceiverTests(unittest.TestCase):
    def test_pairing_origin_payload_and_native_capture(self):
        with tempfile.TemporaryDirectory() as temp:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
            command=[sys.executable,str(Path(__file__).resolve().parents[1]/'news_live.py'),'--store',temp,'godel-serve','--compatibility-check','--minutes','1','--port',str(port)]
            child=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
            pairing=None
            try:
                ready=json.loads(child.stdout.readline())
                self.assertEqual(ready['status'],'ready')
                pairing=json.loads((Path(temp)/'pairing.json').read_text())
                with httpx.Client(timeout=3,trust_env=False) as client:
                    url=pairing['url'];auth={'Authorization':'Bearer '+pairing['token']}
                    self.assertEqual(client.get(url.replace('/godel','/health')).status_code,200)
                    pair_url=url.replace('/godel','/pair')
                    client_id='a'*32
                    setup={'code':ready['connection_code'],'client_id':client_id}
                    header={'X-CEF-Bridge':'1','Origin':'chrome-extension://'+client_id}
                    self.assertEqual(client.post(pair_url,json=setup).status_code,403)
                    self.assertEqual(client.post(pair_url,json=setup,headers={**header,'Origin':'https://evil.invalid'}).status_code,403)
                    self.assertEqual(client.post(pair_url,json={**setup,'client_id':'b'*32},headers=header).status_code,403)
                    self.assertEqual(client.post(pair_url,json={**setup,'code':'BAD-CODE'},headers=header).status_code,401)
                    paired=client.post(pair_url,json=setup,headers=header)
                    self.assertEqual(paired.status_code,200)
                    self.assertEqual(paired.json()['token'],pairing['token'])
                    self.assertEqual(client.post(pair_url,json=setup,headers=header).status_code,200)
                    self.assertEqual(client.post(pair_url,json={**setup,'client_id':'b'*32},headers={**header,'Origin':'chrome-extension://'+'b'*32}).status_code,409)
                    self.assertEqual(client.post(url,json=F.godel()).status_code,401)
                    self.assertEqual(client.post(url,json=F.godel(),headers={**auth,'Origin':'https://evil.invalid'}).status_code,403)
                    self.assertEqual(client.post(url,content=b'bad',headers=auth).status_code,413)
                    event=F.godel()
                    response=client.post(url,json=event,headers={**auth,'Origin':'chrome-extension://'+'a'*32})
                    self.assertEqual(response.status_code,200)
                    self.assertEqual(response.json()['classifications'],{'compatibility_history':1})
                    self.assertEqual(client.post(url,json=event,headers=auth).json()['status'],'duplicate_capture')
                    status_url=url.replace('/godel','/status')
                    diagnostic={'id':'test-reconnect-1','at':F.live.stamp() if hasattr(F,'live') else '2026-11-09T12:00:00Z','kind':'capture_recovered','capture_id':None,'gap_ms':61000}
                    status_body={'active':True,'dropped':1,'diagnostics':[diagnostic]}
                    for _ in range(2):self.assertEqual(client.post(status_url,json=status_body,headers=auth).json()['status'],'ok')
                    with closing(sqlite3.connect((Path(temp)/'live.sqlite').as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
                        self.assertEqual(db.execute('SELECT count(*) FROM bridge_diagnostics').fetchone()[0],1)
                    for bad in ([{**diagnostic,'token':'never-store'}],[diagnostic]*7,[{**diagnostic,'gap_ms':True}],[{**diagnostic,'capture_id':'invalid'}]):
                        self.assertEqual(client.post(status_url,json={**status_body,'diagnostics':bad},headers=auth).status_code,400)
                    rejected=client.post(url,json=['never-store'],headers=auth)
                    self.assertEqual(rejected.status_code,400)
                    self.assertEqual(rejected.json()['error'],'invalid_capture')
                    self.assertEqual(rejected.json()['diagnostic']['reason'],'expected_capture_object')
                    archives=list((Path(temp)/'rejections').glob('*.json'));self.assertEqual(len(archives),5)
                    self.assertTrue(all('never-store' not in p.read_text() for p in archives))
                    with closing(sqlite3.connect((Path(temp)/'live.sqlite').as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
                        self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0],1)
                        self.assertEqual(db.execute('SELECT count(*) FROM bridge_diagnostics').fetchone()[0],6)
                    self.assertEqual(client.post(url,content=b'x'*(1024*1024+1),headers={**auth,'Content-Type':'application/json'}).status_code,413)
                    self.assertEqual(client.post(url.replace('/godel','/stop'),json={'reason':'unknown_private_text'},headers=auth).status_code,400)
                    self.assertEqual(client.get(url.replace('/godel','/health')).status_code,200)
                    for _ in range(4):
                        self.assertEqual(client.post(pair_url,json={**setup,'code':'BAD-CODE'},headers=header).status_code,401)
                    self.assertEqual(client.post(pair_url,json=setup,headers=header).status_code,429)
            finally:
                if pairing:
                    httpx.post(pairing['url'].replace('/godel','/stop'),json={},
                        headers={'Authorization':'Bearer '+pairing['token']},timeout=5,trust_env=False)
                child.wait(timeout=5)
                child.stdout.close();child.stderr.close()


if __name__=='__main__':unittest.main()
