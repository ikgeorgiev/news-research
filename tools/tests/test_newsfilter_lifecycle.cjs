const {test}=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {randomUUID}=require('node:crypto');
const script=fs.readFileSync(path.join(__dirname,'../newsfilter_bridge/background.js'),'utf8');
const origin='https://newsfilter.io',endpoint='https://api.newsfilter.io/actions',extensionId='a'.repeat(32);
const popup={id:extensionId,url:'chrome-extension://'+extensionId+'/popup.html'};
const tick=()=>new Promise(r=>setTimeout(r,20));
function env(response=async()=>({ok:true,status:200,json:async()=>({status:'ok',articles:50})})) {
  const storage={},scripts=[],requests=[],listeners={};
  const c={TextEncoder,Date,AbortSignal,URL,crypto:{randomUUID},chrome:{
    runtime:{id:extensionId,getURL:p=>'chrome-extension://'+extensionId+'/'+p,onMessage:{addListener:f=>listeners.message=f}},
    storage:{session:{get:async()=>structuredClone(storage),set:async x=>Object.assign(storage,structuredClone(x)),remove:async k=>delete storage[k]}},
    tabs:{get:async id=>({id,url:origin+'/search'}),onRemoved:{addListener:f=>listeners.remove=f}},
    webNavigation:{onCommitted:{addListener:f=>listeners.commit=f}},
    scripting:{executeScript:async o=>{scripts.push(o);return o.args?[{result:{capture_id:o.args[0]?.captureId}}]:[];}}
  },fetch:async(url,o)=>{requests.push({url,options:o});return response(url,o);}};
  vm.createContext(c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-session-policy.js'),'utf8'),c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-monitor.js'),'utf8'),c);vm.runInContext(script,c);
  const send=(message,sender=popup)=>new Promise(resolve=>listeners.message(message,sender,resolve));
  const connect=()=>send({type:'start_capture',tabId:7,pairing:{url:'http://127.0.0.1:8767/newsfilter',token:'x'.repeat(43),expires_at:new Date(Date.now()+60000).toISOString()}});
  const capture=(capture_id=storage.pairing?.captureId,tabId=7,documentId=storage.pairing?.documentId)=>send({type:'capture',payload:{capture_id,endpoint,receipt_kind:'passive_browser_copy',scope:{queryString:'symbols:OXLC'}}},{origin,frameId:0,documentId,tab:{id:tabId}});
  const ready=(documentId='newdoc',tabId=7)=>send({type:'document_ready'},{origin,frameId:0,documentId,url:origin+'/search?query=OXLC',tab:{id:tabId}});
  return {storage,scripts,requests,listeners,send,connect,capture,ready,c};
}
test('only popup starts; invalid setup preserves an active pairing',async()=>{
  const e=env();assert.equal(e.listeners.message({type:'start_capture'},{origin},()=>{}),undefined);
  assert.equal((await e.connect()).status,'started');const prior=structuredClone(e.storage.pairing);
  assert.equal((await e.send({type:'start_capture',tabId:7,pairing:{}})).status,'error');assert.deepEqual(e.storage.pairing,prior);assert.equal(e.requests.length,0);
});
test('user navigation reattaches to the exact document without extending deadline',async()=>{
  const e=env();await e.connect();await e.capture();const p=structuredClone(e.storage.pairing);
  e.listeners.commit({frameId:0,tabId:7,documentId:'newdoc',url:origin+'/search?query=GOF'});await tick();await e.ready();
  assert.notEqual(e.storage.pairing.captureId,p.captureId);assert.equal(e.storage.pairing.end,p.end);assert.equal(e.storage.frames,1);
  assert.equal(e.storage.pairing.startedAt,p.startedAt);
  assert.equal(e.scripts.at(-1).target.documentIds[0],'newdoc');assert.equal(e.requests.length,1);
  assert.equal((await e.capture(p.captureId)).status,'invalid');assert.equal((await e.capture(undefined,7,'olddoc')).status,'invalid');
});
test('document-ready cannot activate an unpaired tab or resume after Stop',async()=>{
  const e=env();assert.equal((await e.ready()).status,'inactive');assert.equal(e.requests.length,0);
  await e.connect();const original=e.storage.pairing.captureId;
  assert.equal((await e.ready('otherdoc',8)).status,'inactive');assert.equal(e.storage.pairing.captureId,original);
  await e.send({type:'stop_capture'});const count=e.requests.length;
  assert.equal((await e.ready('late')).status,'inactive');assert.equal(e.requests.length,count);assert.equal(e.storage.pairing,undefined);
});
test('duplicate document handshake and late onCommitted preserve received state',async()=>{
  const e=env();await e.connect();await e.ready();await e.capture();const p=structuredClone(e.storage.pairing),count=e.scripts.length;
  assert.equal((await e.ready()).status,'already_attached');
  e.listeners.commit({frameId:0,tabId:7,documentId:'newdoc',url:origin+'/search'});await tick();
  assert.equal(e.storage.pairing.captureId,p.captureId);assert.equal(e.scripts.length,count);assert.equal(e.storage.captureState.status,'receiving');
});
test('no capture from other tabs, subframes or unpaired browsing',async()=>{
  const e=env();e.listeners.commit({frameId:0,tabId:7,url:origin+'/search'});await tick();assert.equal(e.scripts.length,0);
  await e.connect();assert.equal((await e.capture(undefined,8)).status,'inactive');
  assert.equal(e.listeners.message({type:'capture'},{origin,frameId:1,tab:{id:7}},()=>{}),undefined);assert.equal(e.requests.length,0);
});
test('Stop concludes paired receiver and navigation never restarts it',async()=>{
  const e=env(async()=>({ok:true,status:200,json:async()=>({status:'stopping'})}));await e.connect();await e.send({type:'stop_capture'});
  assert.equal(e.storage.pairing,undefined);assert.equal(e.storage.captureState.receiverStopped,true);assert.equal(e.requests[0].url,'http://127.0.0.1:8767/stop');
  const count=e.scripts.length;e.listeners.commit({frameId:0,tabId:7,url:origin+'/latest/news'});await tick();assert.equal(e.scripts.length,count);
});
test('leaving news pages closes only this capture; login is never observed',async()=>{
  const e=env();await e.connect();e.listeners.commit({frameId:0,tabId:7,url:origin+'/login'});await tick();
  assert.equal(e.storage.pairing,undefined);assert.equal(e.storage.captureState.reason,'left_news_page');
});
test('refusal or local error stops without retry',async()=>{
  for(const mode of ['refusal','disconnect']){const e=env(async()=>{if(mode==='disconnect')throw new Error('down');return {ok:true,status:200,json:async()=>({status:'native_error'})};});
    await e.connect();await e.capture();assert.equal(e.storage.pairing,undefined);assert.equal(e.requests.length,1);}
});
test('late receipt after Stop cannot reactivate',async()=>{
  let finish;const e=env(url=>url.endsWith('/stop')?Promise.resolve({ok:true,status:200,json:async()=>({status:'stopping'})}):new Promise(r=>finish=r));
  await e.connect();const pending=e.capture();await tick();await e.send({type:'stop_capture'});finish({ok:true,status:200,json:async()=>({status:'ok',articles:50})});
  assert.equal((await pending).status,'superseded');assert.equal(e.storage.pairing,undefined);assert.equal(e.storage.frames,0);
});
test('expiry blocks capture and fresh navigation',async()=>{
  const e=env();await e.connect();e.storage.pairing.end=0;assert.equal((await e.capture()).status,'inactive');assert.equal(e.storage.pairing,undefined);assert.equal(e.requests.length,0);
});
test('closing paired tab concludes receiver, unrelated tab does nothing',async()=>{
  const e=env();await e.connect();e.listeners.remove(8);await tick();assert.equal(e.storage.pairing.active,true);
  e.listeners.remove(7);await tick();assert.equal(e.storage.pairing,undefined);assert.equal(e.requests[0].url,'http://127.0.0.1:8767/stop');
});
