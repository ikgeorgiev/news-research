const {test}=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {randomUUID}=require('node:crypto');
const script=fs.readFileSync(path.join(__dirname,'../newsfilter_bridge/observer.js'),'utf8');
const endpoint='https://api.newsfilter.io/actions';
const scope={type:'filterArticles',queryString:'symbols:OXLC',from:0,size:50};
const sample={from:0,size:50,total:{value:1,relation:'eq'},articles:[{id:'one',title:'Fund distribution',sourceUrl:'https://newsfilter.io/articles/local',symbols:['OXLC'],source:{id:'globenewswire',name:'GlobeNewswire'}}]};
const tick=()=>new Promise(r=>setTimeout(r,20));
function env(response=()=>new Response(JSON.stringify(sample))) {
  let calls=0;
  class XHR extends EventTarget {
    open(){}send(){calls++;}
    getAllResponseHeaders(){return '';}
    getResponseHeader(){throw new Error('Do not probe potentially CORS-hidden headers');}
  }
  const original=()=>{calls++;return Promise.resolve(response());};
  const c={fetch:original,XMLHttpRequest:XHR,URL,Response,TextDecoder,TextEncoder,Date,crypto:{randomUUID},setTimeout,clearTimeout,location:{href:'https://newsfilter.io/search'}};
  c.window=c;vm.createContext(c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-session-policy.js'),'utf8'),c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-monitor.js'),'utf8'),c);vm.runInContext(script,c);
  return {c,original,calls:()=>calls,o:c.CefNewsfilterObserver};
}
test('start and stop make no Newsfilter request and restore originals',()=>{
  const e=env(),open=e.c.XMLHttpRequest.prototype.open;e.o.start({seconds:2});e.o.stop();
  assert.equal(e.calls(),0);assert.equal(e.c.fetch,e.original);assert.equal(e.c.XMLHttpRequest.prototype.open,open);
});
test('native request is unchanged; credentials and descriptions are never copied',async()=>{
  const e=env(()=>new Response(JSON.stringify({...sample,token:'secret',articles:[{...sample.articles[0],description:'private',source:{id:'gnw',name:'GNW',token:'secret'}}]})));
  e.o.start({seconds:2});const result=await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify({...scope,token:'secret'})});
  assert.equal((await result.json()).token,'secret');await tick();const event=e.o.drain()[0];e.o.stop();
  assert.equal(e.calls(),1);assert.equal(JSON.stringify(event).includes('secret'),false);assert.equal(event.payload.articles[0].description,undefined);
});
test('login, pagination, other hosts and credential-bearing URL are ignored',async()=>{
  const e=env();e.o.start({seconds:2});
  for(const [url,body] of [[endpoint,{type:'login',email:'private'}],[endpoint,{...scope,from:50}],['https://api.newsfilter.io/public/actions',scope],[endpoint+'?token=secret',scope]])
    await e.c.fetch(url,{method:'POST',body:JSON.stringify(body)});
  await tick();assert.equal(e.o.drain().length,0);e.o.stop();assert.equal(e.calls(),4);
});
test('XHR preserves selected source URLs and timestamps',()=>{
  const e=env();e.o.start({seconds:2});const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
  x.status=200;x.responseText=JSON.stringify(sample);x.dispatchEvent(new Event('load'));
  const event=e.o.drain()[0];e.o.stop();assert.equal(event.payload.articles.length,1);assert.equal(event.scope.queryString,'symbols:OXLC');
  assert.equal(event.payload.articles[0].sourceUrl,'https://newsfilter.io/articles/local');
});

test('XHR copies only exposed allowed headers and preserves HTTP-date colons',()=>{
  const e=env();e.o.start({seconds:2});const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
  x.status=200;x.responseText=JSON.stringify(sample);
  x.getAllResponseHeaders=()=> 'Date: Sun, 08 Nov 2026 13:00:00 GMT\r\nAge: 42\r\nCache-Control: max-age=600\r\nRetry-After: 90\r\nX-Private-Fixture: never-copy\r\n';
  x.dispatchEvent(new Event('load'));const event=e.o.drain()[0];e.o.stop();
  assert.deepEqual({...event.headers},{date:'Sun, 08 Nov 2026 13:00:00 GMT',age:'42','cache-control':'max-age=600','retry-after':'90'});
  assert.equal(event.payload.articles.length,1);assert.equal(JSON.stringify(event).includes('never-copy'),false);
});

test('CORS-hidden optional XHR headers stay absent without failing capture',()=>{
  for(const visible of ['',null,'cache-control: max-age=600\r\n']) {
    const e=env();e.o.start({seconds:2});const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
    x.status=200;x.responseText=JSON.stringify(sample);x.getAllResponseHeaders=()=>visible;
    x.dispatchEvent(new Event('load'));const event=e.o.drain()[0];e.o.stop();
    assert.equal(event.error,undefined);assert.equal(event.payload.articles.length,1);
    assert.equal(event.headers.date,undefined);assert.equal(event.headers.age,undefined);assert.equal(event.headers['retry-after'],undefined);
  }
});

test('XHR 429 stops with exposed Retry-After retained or hidden Retry-After absent',()=>{
  for(const visible of ['', 'retry-after: 90\r\n']) {
    const e=env();e.o.start({seconds:2});const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
    x.status=429;x.getAllResponseHeaders=()=>visible;x.dispatchEvent(new Event('load'));const event=e.o.drain()[0];
    assert.equal(event.status,429);assert.equal(event.headers['retry-after'],visible?'90':undefined);
    assert.equal(e.o.status().active,false);assert.equal(e.calls(),1);
  }
});
test('403 and 429 stop without any retry',async()=>{
  for(const status of [403,429]){const e=env(()=>new Response('blocked',{status}));e.o.start({seconds:2});await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify(scope)});await tick();
    assert.equal(e.o.drain()[0].status,status);assert.equal(e.o.status().active,false);assert.equal(e.calls(),1);}
});
test('oversize and schema errors stop with a fixed non-sensitive error',async()=>{
  for(const body of ['x'.repeat(1024*1024+1),'{}']){const e=env(()=>new Response(body));e.o.start({seconds:2});await e.c.fetch(endpoint,{method:'POST',body:JSON.stringify(scope)});await tick();
    assert.equal(e.o.drain()[0].error,'news_response_copy_or_schema_error');assert.equal(e.o.status().active,false);}
});
test('fresh generation discards a late old XHR response',()=>{
  const e=env();const first=e.o.start({seconds:2});const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
  const second=e.o.start({seconds:2});x.status=200;x.responseText=JSON.stringify(sample);x.dispatchEvent(new Event('load'));
  assert.notEqual(first.capture_id,second.capture_id);assert.equal(e.o.drain().length,0);e.o.stop();
});
test('document-start preparation never reads a body before an active lease',()=>{
  const e=env();e.o.prepare();const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
  x.status=200;Object.defineProperty(x,'responseText',{get(){throw new Error('Must not read unpaired response');}});
  x.dispatchEvent(new Event('load'));assert.equal(e.o.drain().length,0);assert.equal(e.o.status().active,false);e.o.stop();assert.equal(e.calls(),1);
});
test('early XHR request survives the paired-tab handshake, with no source replay',()=>{
  const e=env();const startedAt=Date.now()-1000;e.o.prepare();const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
  const id=randomUUID();e.o.start({seconds:2,startedAt,captureId:id});x.status=200;x.responseText=JSON.stringify(sample);x.dispatchEvent(new Event('load'));
  const event=e.o.drain()[0];assert.equal(event.capture_id,id);assert.equal(event.payload.articles.length,1);assert.equal(e.calls(),1);e.o.stop();
});
test('manual Start excludes a request that began before consent',()=>{
  const e=env();e.o.prepare();const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
  e.o.start({seconds:2,startedAt:Date.now()+1});x.status=200;x.responseText=JSON.stringify(sample);x.dispatchEvent(new Event('load'));
  assert.equal(e.o.drain().length,0);e.o.stop();
});

test('completed XHR waits for the existing session lease without reading its body early',()=>{
  const e=env();let reads=0;e.o.prepare();const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
  x.status=200;Object.defineProperty(x,'responseText',{get(){reads++;return JSON.stringify(sample);}});
  x.dispatchEvent(new Event('load'));assert.equal(reads,0);assert.equal(e.o.drain().length,0);
  e.o.start({seconds:2,startedAt:Date.now()-1000});const event=e.o.drain()[0];
  assert.equal(reads,1);assert.equal(event.payload.articles[0].id,'one');assert.equal(e.calls(),1);
  assert.ok(event.browser_response_completed_at<=event.browser_body_observed_at);e.o.stop();
});

test('completed pre-consent XHR and callbacks discarded by Stop are never read',()=>{
  for(const stop of [false,true]){
    const e=env();let reads=0;e.o.prepare();const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
    x.status=200;Object.defineProperty(x,'responseText',{get(){reads++;return JSON.stringify(sample);}});x.dispatchEvent(new Event('load'));
    if(stop)e.o.stop();e.o.start({seconds:2,startedAt:stop?0:Date.now()+1});
    assert.equal(reads,0);assert.equal(e.o.drain().length,0);e.o.stop();
  }
});

test('reused XHR cannot attribute the new body to an older request',()=>{
  const e=env();e.o.prepare();const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));
  x.status=200;x.responseText=JSON.stringify(sample);x.dispatchEvent(new Event('load'));
  x.open('POST',endpoint);x.send(JSON.stringify({...scope,queryString:'symbols:GOF'}));
  x.responseText=JSON.stringify({...sample,articles:[{...sample.articles[0],id:'two'}]});x.dispatchEvent(new Event('load'));
  e.o.start({seconds:2,startedAt:Date.now()-1000});const events=e.o.drain();
  assert.equal(events.length,1);assert.equal(events[0].scope.queryString,'symbols:GOF');assert.equal(events[0].payload.articles[0].id,'two');e.o.stop();
});

test('pending handshake references are bounded; a deferred refusal stops later copies',()=>{
  const e=env();e.o.prepare();let reads=0;
  for(let i=0;i<25;i++){
    const x=new e.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));x.status=200;
    Object.defineProperty(x,'responseText',{get(){reads++;return JSON.stringify(sample);}});x.dispatchEvent(new Event('load'));
  }
  assert.equal(reads,0);e.o.start({seconds:2,startedAt:Date.now()-1000});assert.equal(reads,20);assert.equal(e.o.drain().length,20);e.o.stop();
  const f=env();f.o.prepare();
  for(const status of [403,200]){const x=new f.c.XMLHttpRequest();x.open('POST',endpoint);x.send(JSON.stringify(scope));x.status=status;x.responseText=JSON.stringify(sample);x.dispatchEvent(new Event('load'));}
  f.o.start({seconds:2,startedAt:Date.now()-1000});assert.equal(f.o.status().active,false);assert.equal(f.o.drain().length,1);
});
