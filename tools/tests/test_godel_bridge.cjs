const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {randomUUID} = require('node:crypto');
const script = fs.readFileSync(path.join(__dirname,'../godel_bridge/observer.js'),'utf8');
const endpoint = 'https://terminal-api.godelterminal.com/api/news/items';
const scope = {symbols:['GOF'],sources:['test'],languages:['en'],pageable:{afterCursor:null,beforeCursor:null},dateTimeRange:{startTime:'0',endTime:'1'}};
const sample = {content:[{id:'local',title:'Example distribution',securityIds:['FALSE'],originalUrl:null}],pageSize:20,nextCursor:'history',totalItems:30};
const tick = () => new Promise(r => setTimeout(r,30));
function env(response = () => new Response(JSON.stringify(sample))) {
  let calls = 0;
  class XHR extends EventTarget {
    open(method,url) {this.method=method;this.url=url;}
    send() {calls++;}
  }
  const original = function() {calls++;return Promise.resolve(response());};
  const c={fetch:original,XMLHttpRequest:XHR,URL,Request,Response,TextDecoder,TextEncoder,Date,crypto:{randomUUID},setTimeout,clearTimeout,location:{href:'https://app.godelterminal.com/'}};
  c.window=c;
  vm.createContext(c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-session-policy.js'),'utf8'),c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-monitor.js'),'utf8'),c);vm.runInContext(script,c);
  return {c,original,calls:()=>calls};
}
test('install/start sends no request and stop restores fetch and XHR',()=>{
  const e=env(),open=e.c.XMLHttpRequest.prototype.open;
  e.c.CefGodelObserver.start({seconds:2});assert.equal(e.calls(),0);
  e.c.CefGodelObserver.stop();assert.equal(e.c.fetch,e.original);assert.equal(e.c.XMLHttpRequest.prototype.open,open);
});
test('captures native response without modifying its return',async()=>{
  const e=env();e.c.CefGodelObserver.start({seconds:2});
  const result=await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify(scope)});
  assert.equal((await result.json()).content[0].id,'local');await tick();
  const [event]=e.c.CefGodelObserver.drain();e.c.CefGodelObserver.stop();
  assert.equal(e.calls(),1);assert.equal(event.payload.content.length,1);
  assert.equal(event.payload.nextCursor,'history');assert.equal(event.scope.symbols[0],'GOF');
  assert.match(event.capture_id,/^[a-f0-9-]{36}$/);assert.equal(event.payload.content[0].originalUrl,null);
});
test('other endpoints and GET requests are not captured',async()=>{
  const e=env();e.c.CefGodelObserver.start({seconds:2});
  await e.c.fetch('https://terminal-api.godelterminal.com/api/other',{method:'POST'});
  await e.c.fetch(endpoint,{method:'GET'});await tick();
  assert.equal(e.c.CefGodelObserver.drain().length,0);e.c.CefGodelObserver.stop();
});

test('broad native fetch and XHR preserve omitted symbols and selected filters',async()=>{
  for(const transport of ['fetch','xhr']) {
    const e=env();e.c.CefGodelObserver.start({seconds:2});
    const broad={...scope,privateAccount:'DO-NOT-COPY'};delete broad.symbols;
    if(transport==='fetch') await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify(broad)});
    else {const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(broad));
      x.status=200;x.responseText=JSON.stringify(sample);x.dispatchEvent(new Event('load'));}
    await tick();const [event]=e.c.CefGodelObserver.drain();e.c.CefGodelObserver.stop();
    assert.equal(Object.hasOwn(event.scope,'symbols'),false);
    assert.equal(event.scope.sources[0],'test');assert.equal(event.scope.languages[0],'en');
    assert.equal(event.request_scope_diagnostic.fields.symbols,'missing');
    assert.equal(event.request_scope_diagnostic.status,'selected');
    assert.equal(JSON.stringify(event).includes('DO-NOT-COPY'),false);assert.equal(e.calls(),1);
  }
});

test('null or malformed symbols are not mistaken for an omitted field',async()=>{
  for(const symbols of [null,'GOF',[42]]) {
    const e=env();e.c.CefGodelObserver.start({seconds:2});
    await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify({...scope,symbols})});await tick();
    const [event]=e.c.CefGodelObserver.drain();e.c.CefGodelObserver.stop();
    assert.equal(event.scope,null);assert.equal(event.request_scope_diagnostic.status,'unavailable');
  }
});
test('fetch Request filter body survives page consumption without copying private fields',async()=>{
  const e=env();e.c.CefGodelObserver.start({seconds:2});
  const req=new Request(endpoint,{method:'POST',headers:{Authorization:'DO-NOT-COPY'},body:JSON.stringify({...scope,privateAccount:'DO-NOT-COPY'})});
  const response=await e.c.fetch(req);
  await req.text();await response.json();await tick();
  const [event]=e.c.CefGodelObserver.drain();e.c.CefGodelObserver.stop();
  assert.equal(event.scope.symbols[0],'GOF');assert.equal(event.request_scope_diagnostic.body_kind,'Request');
  assert.equal(event.request_scope_diagnostic.status,'selected');assert.equal(e.calls(),1);
  assert.equal(JSON.stringify(event).includes('DO-NOT-COPY'),false);
});
test('explicit fetch init body takes priority over the Request body',async()=>{
  const e=env();e.c.CefGodelObserver.start({seconds:2});
  const req=new Request(endpoint,{method:'POST',body:JSON.stringify({...scope,symbols:['GOOG']})});
  await e.c.fetch(req,{body:JSON.stringify(scope)});await tick();
  const [event]=e.c.CefGodelObserver.drain();e.c.CefGodelObserver.stop();
  assert.equal(event.scope.symbols[0],'GOF');assert.equal(event.request_scope_diagnostic.body_kind,'string');
});
test('unavailable or oversized request scope remains explicitly unverified',async()=>{
  for(const body of ['{}',JSON.stringify({...scope,padding:'x'.repeat(65536)})]){
    const e=env();e.c.CefGodelObserver.start({seconds:2});
    await e.c.fetch(new Request(endpoint,{method:'POST',body}));await tick();
    const [event]=e.c.CefGodelObserver.drain();e.c.CefGodelObserver.stop();
    assert.equal(event.scope,null);assert.notEqual(event.request_scope_diagnostic.status,'selected');assert.equal(e.calls(),1);
  }
});
test('oversized native response is rejected without breaking terminal',async()=>{
  const e=env(()=>new Response('x'.repeat(1024*1024+1)));e.c.CefGodelObserver.start({seconds:2});
  const response=await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify(scope)});await tick();
  assert.equal(response.status,200);assert.match(e.c.CefGodelObserver.drain()[0].error,/1 MiB/);e.c.CefGodelObserver.stop();
});
test('schema drift is an error, not a zero-item success',async()=>{
  const e=env(()=>new Response('{}'));e.c.CefGodelObserver.start({seconds:2});
  await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify(scope)});await tick();
  assert.match(e.c.CefGodelObserver.drain()[0].error,/envelope/);e.c.CefGodelObserver.stop();
});
test('native refusal stops capture without retrying',async()=>{
  const e=env(()=>new Response('denied',{status:403}));e.c.CefGodelObserver.start({seconds:2});
  await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify(scope)});await tick();
  assert.equal(e.c.CefGodelObserver.drain()[0].status,403);assert.equal(e.c.CefGodelObserver.status().active,false);assert.equal(e.calls(),1);
});
test('a fresh capture has a fresh baseline identity',async()=>{
  const e=env();const ids=[];
  for(let n=0;n<2;n++) {e.c.CefGodelObserver.start({seconds:2});await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify(scope)});await tick();ids.push(e.c.CefGodelObserver.drain()[0].capture_id);e.c.CefGodelObserver.stop();}
  assert.notEqual(ids[0],ids[1]);
});
test('XHR copies only selected public news fields',()=>{
  const e=env();e.c.CefGodelObserver.start({seconds:2});
  const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
  x.status=200;x.responseText=JSON.stringify({...sample,privateAccount:'do-not-copy'});x.dispatchEvent(new Event('load'));
  const event=e.c.CefGodelObserver.drain()[0];e.c.CefGodelObserver.stop();
  assert.equal(event.transport,'xhr');assert.equal(event.payload.privateAccount,undefined);assert.equal(event.payload.content[0].title,'Example distribution');
});
test('background relays only to loopback with a short-lived token',async()=>{
  let listener;const calls=[];const storage={pairing:{token:'test-token',end:Date.now()+60000,tabId:7,active:true,captureId:'test-generation'}};
  const c={chrome:{runtime:{onMessage:{addListener:f=>listener=f}},storage:{session:{get:async()=>storage,set:async x=>Object.assign(storage,x),remove:async k=>delete storage[k]}},
    scripting:{executeScript:async()=>[]},tabs:{onRemoved:{addListener:()=>{}},onUpdated:{addListener:()=>{}}}},
    TextEncoder,Date,AbortSignal,fetch:async(url,options)=>{calls.push({url,options});return {status:200,ok:true,json:async()=>({status:'ok',articles:20})};}};
  vm.createContext(c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-session-policy.js'),'utf8'),c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-monitor.js'),'utf8'),c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../godel_bridge/background.js'),'utf8'),c);
  const message={type:'capture',payload:{endpoint,receipt_kind:'passive_browser_copy',capture_id:'test-generation'}};
  assert.equal(listener(message,{origin:'https://evil.invalid',tab:{id:7}},()=>{}),undefined);
  const result=await new Promise(resolve=>listener(message,{origin:'https://app.godelterminal.com',tab:{id:7}},resolve));
  assert.equal(result.status,'ok');assert.equal(calls.length,1);assert.equal(calls[0].url,'http://127.0.0.1:8766/godel');
  assert.equal(calls[0].options.credentials,'omit');assert.equal(calls[0].options.redirect,'error');
  assert.equal(storage.frames,1);assert.equal(storage.observations,20);
  storage.pairing.end=0;
  const expired=await new Promise(resolve=>listener(message,{origin:'https://app.godelterminal.com',tab:{id:7}},resolve));
  assert.equal(expired.status,'inactive');assert.equal(calls.length,1);
});
