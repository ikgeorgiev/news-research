const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const script=fs.readFileSync(path.join(__dirname,'../godel_bridge/popup.js'),'utf8');
const tick=()=>new Promise(r=>setTimeout(r,10));
function env(initial={}) {
  const els={},storage=initial,requests=[],messages=[];let changed;
  const c={Date,AbortSignal,setInterval:()=>{},document:{getElementById:id=>els[id] ||= {value:'',textContent:'',disabled:false,addEventListener:(event,fn)=>els[id][event]=fn}},
    chrome:{runtime:{id:'a'.repeat(32),sendMessage:async m=>{messages.push(m);return {status:m.type==='stop_capture'?'stopped':'started'};}},
      storage:{session:{get:async()=>storage},onChanged:{addListener:fn=>changed=fn}},tabs:{query:async()=>[{id:7,url:'https://app.godelterminal.com/'}]}},
    fetch:async(url,options)=>{requests.push({url,options});return {ok:true,json:async()=>({url:'http://127.0.0.1:8766/godel',token:'x'.repeat(43),expires_at:new Date(Date.now()+60000).toISOString()})};}};
  vm.createContext(c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-session-policy.js'),'utf8'),c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-monitor.js'),'utf8'),c);vm.runInContext(script,c);
  return {els,c,storage,requests,messages,changed:()=>changed({},'session')};
}
test('plain eight-character code pairs on loopback without file or clipboard',async()=>{
  const e=env();await tick();e.els.code.value='abcd-efgh';await e.els.start.click();
  assert.equal(e.requests.length,1);assert.equal(e.requests[0].url,'http://127.0.0.1:8766/pair');
  assert.equal(JSON.parse(e.requests[0].options.body).code,'ABCDEFGH');
  assert.equal(e.messages[0].type,'start_capture');assert.equal(e.els.code.value,'');
});
test('rendered status updates from storage and shows actual terminal filter',async()=>{
  const e=env({pairing:{active:true,end:Date.now()+60000},captureState:{status:'waiting'}});await tick();
  assert.match(e.els.status.textContent,/natural news/);assert.equal(e.els.start.disabled,true);
  Object.assign(e.storage,{captureState:{status:'receiving'},frames:2,lastResult:{articles:20,symbols:['GOOG'],sources:24,at:new Date().toISOString()}});
  e.changed();await tick();assert.equal(e.els.frames.textContent,'2');assert.equal(e.els.headlines.textContent,'20');
  assert.match(e.els.filter.textContent,/GOOG/);assert.match(e.els.filter.textContent,/24 sources/);
});
test('bad code stays a visible error and causes no request',async()=>{
  const e=env();await tick();e.els.code.value='short';await e.els.start.click();
  assert.match(e.els.message.textContent,/eight characters/);assert.equal(e.requests.length,0);assert.equal(e.messages.length,0);
});

test('omitted ticker filter is shown as unknown rather than all symbols',async()=>{
  const e=env({lastResult:{symbols:null,symbolsOmitted:true,sources:24,assessment:{filter_scope:{mode:'symbols_omitted_unverified',eligible_count:0,universe_count:367}}}});await tick();
  assert.match(e.els.filter.textContent,/Ticker filter omitted/);
  assert.match(e.els.filter.textContent,/scope unverified/);
  assert.doesNotMatch(e.els.filter.textContent,/All symbols/);
  assert.match(e.els.coverage.textContent,/coverage is unknown/);
  assert.match(e.els.coverage.textContent,/Other news panels/);
  assert.doesNotMatch(e.els.coverage.textContent,/0\/367/);
});
test('resume uses saved connection only after explicit click',async()=>{
  const e=env({pairing:{active:false,end:Date.now()+60000,token:'x'.repeat(43)},captureState:{status:'stopped',reason:'tab_reloaded'}});await tick();
  assert.match(e.els.start.textContent,/Resume/);assert.equal(e.messages.length,0);await e.els.start.click();
  assert.equal(e.messages[0].type,'start_capture');assert.equal(e.requests.length,0);
});
test('unconfirmed receiver stop is not presented as verified shutdown',async()=>{
  const e=env({captureState:{status:'stopped',reason:'user_stopped',receiverStopped:false}});await tick();
  assert.match(e.els.status.textContent,/unconfirmed/);
});
test('daily recovery explains automatic reconnection and keeps Stop available',async()=>{
  const e=env({pairing:{active:false,recovering:true,end:Date.now()+60000,mode:'until_close'},captureState:{status:'recovering',reason:'local_transport_error'}});await tick();
  assert.equal(e.els['session-label'].textContent,'Reconnecting locally');
  assert.match(e.els.status.textContent,/No new code needed/);
  assert.equal(e.els.start.disabled,true);assert.equal(e.els.stop.disabled,false);
});

test('diagnostic projection preserves failure codes without exporting private stored data',async()=>{
  const e=env({frames:1,dropped:0,pairing:{token:'DO-NOT-EXPORT',end:0},
    captureState:{status:'stopped',reason:'receiver_error',at:'2026-11-09T14:53:21.987Z'},
    lastResult:{http_status:400,error:'invalid_capture',transport:'native_worker',at:'2026-11-09T14:53:21.900Z',body:'DO-NOT-EXPORT',symbols:['PRIVATE']}});
  await tick();
  const text=e.els.diagnostics.value,data=JSON.parse(text);
  assert.equal(data.last_reply.http_status,400);assert.equal(data.last_reply.error,'invalid_capture');
  assert.equal(data.capture.reason,'receiver_error');assert.equal(data.updates_saved,1);
  assert.doesNotMatch(text,/DO-NOT-EXPORT|PRIVATE|token/);
  const result=vm.runInContext('diagnosticSummary({lastResult:{error:"private account@example.com"}}, {diagnostics:[{kind:"private message",at:"private",token:"secret"}],sourceHold:{token:"secret"}})',e.c);
  assert.equal(result.last_reply.error,null);assert.equal(result.recent_events[0].kind,null);assert.equal(result.source_hold_present,true);
  assert.doesNotMatch(JSON.stringify(result),/secret|account@example/);
  assert.equal(e.requests.length,0);assert.equal(e.messages.length,0);
});

test('review-only delayed copies do not claim fresh receiving status or stop the session',async()=>{
  const e=env({pairing:{active:true,end:Date.now()+60000},captureState:{status:'receiving'},lastResult:{review_only:true,articles:0,at:new Date().toISOString()}});
  await tick();assert.equal(e.els['session-label'].textContent,'Delayed copy · monitoring');
  assert.match(e.els.status.textContent,/excluded from speed/);assert.equal(e.els.stop.disabled,false);assert.equal(e.messages.length,0);
});
