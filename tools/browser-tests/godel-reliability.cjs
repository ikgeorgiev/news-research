/* Local-only fault proxy and real Chromium lifecycle acceptance tests.
 * No production endpoints, credentials, forced source requests or fake clock. */
const http=require('node:http'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path');
const {spawnSync}=require('node:child_process');
exports.proxy=async function(faults) {
  const server=http.createServer((req,res)=>{
    const chunks=[];let size=0;
    req.on('data',c=>{size+=c.length;if(size>1024*1024+1)req.destroy();else chunks.push(c);});
    req.on('end',()=>{
      const kind=req.url==='/status'?'status':req.url==='/godel'?'capture':null;
      if(kind)faults[kind+'Requests'].push({at:Date.now(),fault:!!faults[kind]});
      if(kind&&faults[kind]>0) {
        faults[kind]--;res.writeHead(503,{'Content-Type':'application/json','Connection':'close'});
        return res.end('{"error":"synthetic_local_outage"}');
      }
      const upstream=http.request({hostname:'127.0.0.1',port:18766,path:req.url,method:req.method,headers:req.headers,timeout:5500},r=>{
        const parts=[];r.on('data',c=>parts.push(c));r.on('end',()=>{
          let body=Buffer.concat(parts);
          if(req.url==='/pair'&&r.statusCode===200){const p=JSON.parse(body);p.url='http://127.0.0.1:8766/godel';body=Buffer.from(JSON.stringify(p));}
          res.writeHead(r.statusCode,{...r.headers,'content-length':body.length});res.end(body);
        });
      });
      upstream.on('error',()=>{res.writeHead(502);res.end('{}');});
      upstream.on('timeout',()=>upstream.destroy());upstream.end(Buffer.concat(chunks));
    });
  });
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(8766,'127.0.0.1',resolve);});
  return server;
};
exports.run=async function(e) {
  const {context,news,extensionId,report,faults,check,snapshot,saved,stopped,receiverStopped,wait,STORE,PY,RUN}=e;
  let popup=e.popup;
  const state=()=>popup.evaluate(()=>chrome.storage.session.get(['pairing','captureState','dropped']));
  async function until(predicate,timeout=80000) {
    const end=Date.now()+timeout;
    while(Date.now()<end){if(await predicate())return;await wait(250);}
    throw Error('Timed out waiting for real stored transition');
  }
  const active=()=>until(async()=>{const {pairing}=await state();return pairing?.active&&!pairing.recovering;});
  const recovering=()=>until(async()=>(await state()).pairing?.recovering);
  const original=(await state()).pairing;
  await check('native worker delivers a baseline without extra source requests',async()=>{
    await news.locator('#emit').click();await saved(1);assert.equal(snapshot().observations,1);assert.equal(report.fixture_requests.length,1);
  });
  await check('one real local 503 recovers on the next real minute alarm without a new code',async()=>{
    faults.status=1;await recovering();assert.equal((await state()).pairing.end,original.end);
    try {await news.waitForFunction(()=>!window.CefGodelObserver.status().active,null,{timeout:5000});}
    catch(error) {
      report.pause_failure={observer:await news.evaluate(()=>window.CefGodelObserver.status()),state:await state(),diagnostics:await popup.evaluate(()=>chrome.storage.local.get('diagnostics')),requests:faults.statusRequests};
      delete report.pause_failure.state.pairing.token;
      throw error;
    }
    await active();const p=(await state()).pairing;
    assert.notEqual(p.captureId,original.captureId);assert.equal(p.token,original.token);assert.equal(p.end,original.end);
    await news.locator('#emit').click();await saved(2);assert.equal(report.fixture_requests.length,1);
  });
  await check('failed capture POST is not replayed and recovery retains its loss counter',async()=>{
    const before=snapshot().observations,posted=faults.captureRequests.length;
    faults.capture=1;await news.locator('#emit').click();await recovering();
    assert.equal(snapshot().observations,before);assert.equal((await state()).dropped,1);
    await active();assert.equal(faults.captureRequests.length,posted+1,'No replay of an uncertain POST');
    await news.locator('#emit').click();await saved(3);assert.equal(snapshot().observations,before+1);
  });
  await check('ordinary native page reload resumes automatically with a new capture generation',async()=>{
    const old=(await state()).pairing.captureId;
    await news.reload();await news.locator('#result').filter({hasText:'response'}).waitFor();await active();
    assert.notEqual((await state()).pairing.captureId,old);assert.equal((await state()).pairing.end,original.end);
    await news.locator('#emit').click();await saved(4);assert.equal(report.fixture_requests.length,2);
  });
  await check('background tab and real service-worker termination recover through Chrome alarms',async()=>{
    const cdp=await context.newCDPSession(news);let version;
    cdp.on('ServiceWorker.workerVersionUpdated',({versions})=>{version=versions.find(v=>v.scriptURL===`chrome-extension://${extensionId}/background.js`&&v.runningStatus==='running')||version;});
    await cdp.send('ServiceWorker.enable');
    for(let n=0;!version&&n<30;n++)await wait(100);
    assert.ok(version,'Chrome must expose the actual extension worker version');
    const blank=await context.newPage();await blank.goto('about:blank');await blank.bringToFront();
    await popup.close();const checked=faults.statusRequests.length;
    await cdp.send('ServiceWorker.stopWorker',{versionId:version.versionId});await cdp.detach();
    report.stopped_extension_worker_version=version.versionId;
    // No extension evaluate or popup until a real alarm wakes the stopped worker.
    for(let n=0;faults.statusRequests.length===checked&&n<80;n++)await wait(1000);
    assert.ok(faults.statusRequests.length>checked,'A real scheduled alarm must revive the worker');
    popup=await context.newPage();await popup.goto(`chrome-extension://${extensionId}/popup.html`);
    assert.equal((await state()).pairing.token,original.token);assert.equal((await state()).pairing.end,original.end);
    assert.equal((await state()).pairing.active,true);await news.locator('#emit').click();
    await popup.waitForFunction(()=>document.querySelector('#frames').textContent==='5');
    assert.equal(report.fixture_requests.length,2);await blank.close();
  });
  await check('six-minute loopback gap does not kill the bounded daily receiver or fabricate news',async()=>{
    const before=snapshot().observations;
    faults.status=6;await recovering();
    const started=Date.now();
    while(faults.status>0&&Date.now()-started<390000)await wait(1000);
    assert.equal(faults.status,0);assert.ok(Date.now()-started>290000,'Exercise the old five-minute lease boundary');
    assert.equal(snapshot().observations,before);await active();
    assert.equal((await state()).pairing.end,original.end);assert.equal(report.fixture_requests.length,2);
    await news.locator('#emit').click();await popup.waitForFunction(()=>document.querySelector('#frames').textContent==='6');
    report.real_gap_exercised_ms=Date.now()-started;
  });
  await check('diagnostics survive worker restart and contain no pairing token or private fixture data',async()=>{
    const journal=await popup.evaluate(async()=>(await chrome.storage.local.get('diagnostics')).diagnostics);
    assert.ok(journal.some(x=>x.kind==='worker_started'));
    assert.ok(journal.some(x=>x.kind==='capture_recovered'&&x.gap_ms>0));
    assert.ok(journal.some(x=>x.kind==='local_http_temporary'&&x.http_status===503));
    const serialized=JSON.stringify(journal);assert.ok(!serialized.includes(original.token));assert.ok(!serialized.includes('TEST-NEVER-ARCHIVE'));
    report.diagnostics=journal;fs.writeFileSync(path.join(RUN,'diagnostics.json'),JSON.stringify(journal,null,2));
    const result=spawnSync(PY,['-c',"import sqlite3,sys,json;from pathlib import Path;c=sqlite3.connect(Path(sys.argv[1]).resolve().as_uri()+'?mode=ro',uri=True,timeout=5);c.execute('pragma query_only=on');print(json.dumps([json.loads(x[0]) for x in c.execute('select data from bridge_diagnostics')]))",path.join(STORE,'live.sqlite')],{encoding:'utf8',windowsHide:true,timeout:5000});
    assert.equal(result.status,0);report.persisted_diagnostics=JSON.parse(result.stdout);assert.ok(report.persisted_diagnostics.length>0);
  });
  await check('Stop during recovery ends capture and receiver; late alarms cannot reconnect',async()=>{
    faults.capture=1;await news.locator('#emit').click();await recovering();
    await popup.locator('#stop').click();await receiverStopped();
    assert.equal((await state()).pairing,undefined);assert.equal((await state()).captureState.reason,'user_stopped');
    assert.equal(await popup.evaluate(()=>chrome.alarms.get('cef-bridge-status')),undefined);
    assert.equal(await news.evaluate(()=>window.CefGodelObserver.status().active),false);
    report.final_status_requests=faults.statusRequests;report.final_capture_requests=faults.captureRequests;
  });
};
