const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const script = fs.readFileSync(path.join(__dirname,'../godel_bridge/background.js'),'utf8');
const origin='https://app.godelterminal.com';
const endpoint='https://terminal-api.godelterminal.com/api/news/items';
const extensionId='a'.repeat(32);
const popup={id:extensionId,url:'chrome-extension://'+extensionId+'/popup.html'};
const tick=()=>new Promise(r=>setTimeout(r,20));
function env(reply=async()=>({ok:true,status:200,json:async()=>({status:'ok',articles:20})}),daily=false) {
  const storage={},local={},scripts=[],requests=[],listeners={};let generation=0,alarm;
  const clock={now:Date.parse('2026-11-09T12:00:00Z')};
  class TestDate extends Date {constructor(...a){super(...(a.length?a:[clock.now]));}static now(){return clock.now;}}
  const c={TextEncoder,Date:daily?TestDate:Date,AbortSignal,chrome:{
    runtime:{id:extensionId,getURL:p=>'chrome-extension://'+extensionId+'/'+p,onMessage:{addListener:f=>listeners.message=f}},
    storage:{local:{get:async()=>structuredClone(local),set:async x=>Object.assign(local,structuredClone(x))},session:{get:async()=>structuredClone(storage),set:async x=>Object.assign(storage,structuredClone(x)),remove:async k=>delete storage[k]}},
    ...(daily?{alarms:{get:async()=>alarm,create:async()=>{alarm={name:'cef-bridge-status'};},clear:async()=>{alarm=undefined;},onAlarm:{addListener:f=>listeners.alarm=f}}}:{}),
    tabs:{get:async id=>({id,url:origin+'/'}),onRemoved:{addListener:f=>listeners.remove=f},onUpdated:{addListener:f=>listeners.update=f}},
    scripting:{executeScript:async options=>{scripts.push(options);return options.args?[{result:{capture_id:++generation===1?'new-generation':'generation-'+generation}}]:[];}}
  },fetch:async(url,options)=>{requests.push({url,options});return reply(url,options);}};
  vm.createContext(c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-session-policy.js'),'utf8'),c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-monitor.js'),'utf8'),c);vm.runInContext(script,c);
  const send=(message,sender=popup)=>new Promise(resolve=>listeners.message(message,sender,resolve));
  const connect=()=>send({type:'start_capture',tabId:7,pairing:{url:'http://127.0.0.1:8766/godel',token:'x'.repeat(43),expires_at:daily?'2026-11-09T22:00:00Z':new Date(Date.now()+60000).toISOString(),...(daily?{mode:'until_close'}:{})}});
  const capture=(capture_id='new-generation',tabId=7)=>send({type:'capture',payload:{capture_id,endpoint,receipt_kind:'passive_browser_copy',scope:{symbols:['GOF'],sources:['wire']}}},{origin,tab:{id:tabId}});
  return {storage,local,scripts,requests,listeners,send,connect,capture,c,clock,check:()=>vm.runInContext('monitor.check()',c)};
}
test('only extension popup can start; rejected setup preserves an active session',async()=>{
  const e=env();
  assert.equal(e.listeners.message({type:'start_capture'}, {origin,tab:{id:7}},()=>{}),undefined);
  assert.equal((await e.connect()).status,'started');
  const prior=structuredClone(e.storage.pairing);
  assert.equal((await e.send({type:'start_capture',tabId:7,pairing:{}})).status,'error');
  assert.deepEqual(e.storage.pairing,prior);
  assert.equal(e.requests.length,0);
});
test('Stop targets paired tab regardless of current tab and closes owned receiver',async()=>{
  const e=env(async()=>({ok:true,status:200,json:async()=>({status:'stopping'})}));await e.connect();
  const result=await e.send({type:'stop_capture'});
  assert.equal(result.status,'stopped');assert.equal(e.storage.pairing,undefined);
  assert.equal(e.scripts.at(-1).target.tabId,7);
  assert.equal(e.requests.length,1);assert.equal(e.requests[0].url,'http://127.0.0.1:8766/stop');
});
test('capture from stale generation or another tab never reaches receiver',async()=>{
  const e=env();await e.connect();
  assert.equal((await e.capture('old-generation')).status,'invalid');
  assert.equal((await e.capture('new-generation',8)).status,'inactive');assert.equal(e.requests.length,0);
});
test('reload pauses and requires explicit resume with a new baseline',async()=>{
  const e=env();await e.connect();e.listeners.update(7,{status:'loading'});await tick();
  assert.equal(e.storage.pairing.active,false);assert.equal(e.storage.captureState.reason,'tab_reloaded');
  assert.equal((await e.capture()).status,'inactive');assert.equal(e.requests.length,0);
  assert.equal((await e.connect()).status,'started');assert.equal(e.storage.pairing.active,true);
});
test('local receiver failure stops observer without retrying',async()=>{
  const e=env(async()=>{throw new Error('connection refused');});await e.connect();
  assert.equal((await e.capture()).status,'local_receiver_unavailable');
  assert.equal(e.storage.pairing,undefined);assert.equal(e.storage.captureState.reason,'local_receiver_unavailable');
  assert.equal((await e.capture()).status,'inactive');assert.equal(e.requests.length,1);
});
test('a native refusal stops the capture and retains visible failure',async()=>{
  const e=env(async()=>({ok:true,status:200,json:async()=>({status:'native_error'})}));await e.connect();
  await e.capture();assert.equal(e.storage.captureState.reason,'native_error');assert.equal(e.storage.pairing,undefined);
  assert.equal(e.storage.frames,0);assert.equal(e.storage.lastResult.status,'native_error');
});
test('late response after Stop cannot reactivate capture or overwrite state',async()=>{
  let finish;
  const e=env((url)=>url.endsWith('/stop')?Promise.resolve({ok:true,status:200,json:async()=>({status:'stopping'})}):new Promise(r=>finish=r));
  await e.connect();const pending=e.capture();await tick();await e.send({type:'stop_capture'});
  finish({ok:true,status:200,json:async()=>({status:'ok',articles:20})});
  assert.equal((await pending).status,'superseded');assert.equal(e.storage.captureState.reason,'user_stopped');assert.equal(e.storage.frames,0);
});
test('closing unrelated tab leaves capture running; closing paired tab stops',async()=>{
  const e=env();await e.connect();e.listeners.remove(8);await tick();assert.equal(e.storage.pairing.active,true);
  e.listeners.remove(7);await tick();assert.equal(e.storage.pairing,undefined);assert.equal(e.storage.captureState.reason,'tab_closed');
});

test('daily transport loss pauses, keeps its token/deadline, recovers with a fresh generation and never replays',async()=>{
  let broken=true;
  const e=env(async()=>{if(broken)throw Error('offline');return {ok:true,status:200,json:async()=>({status:'ok'})};},true);
  await e.connect();const before=structuredClone(e.storage.pairing);
  await e.capture();assert.equal(e.storage.pairing.recovering,true);assert.equal(e.storage.pairing.active,false);
  assert.equal(e.storage.dropped,1);await e.check();assert.equal(e.storage.pairing.recovering,true);
  broken=false;await e.check();assert.equal(e.storage.pairing.active,true);
  assert.equal(e.storage.pairing.token,before.token);assert.equal(e.storage.pairing.end,before.end);
  assert.notEqual(e.storage.pairing.captureId,before.captureId);
  assert.equal(e.requests.filter(x=>x.url.endsWith('/godel')).length,1);
  assert.ok(e.local.diagnostics.some(x=>x.kind==='capture_recovered'));
  assert.ok(!JSON.stringify(e.local).includes(before.token));
});
for(const status of [401,403,404,410,429])test(`local status ${status} is terminal, never automatic recovery`,async()=>{
  const e=env(async()=>({ok:false,status,json:async()=>({error:'terminal'})}),true);
  await e.connect();await e.check();assert.equal(e.storage.pairing,undefined);
  assert.equal(e.storage.captureState.reason,'local_http_terminal');
});
test('malformed status JSON is terminal rather than a successful empty feed',async()=>{
  const e=env(async()=>({ok:true,status:200,json:async()=>{throw Error('bad json');}}),true);
  await e.connect();await e.check();assert.equal(e.storage.pairing,undefined);assert.equal(e.storage.captureState.reason,'receiver_status_invalid');
});
test('native refusal lost during a local outage remains a durable hold',async()=>{
  const e=env(async()=>{throw Error('offline');},true);await e.connect();
  await e.send({type:'capture',payload:{capture_id:'new-generation',endpoint,receipt_kind:'passive_browser_copy',status:403}},{origin,tab:{id:7}});
  assert.equal(e.storage.pairing,undefined);assert.equal(e.local.sourceHold.status,403);
  assert.equal((await e.connect()).error,'saved_source_hold');
});
test('deadline during recovery removes pairing and cannot create another generation',async()=>{
  let broken=true;
  const e=env(async()=>{if(broken)throw Error('offline');return {ok:true,status:200,json:async()=>({status:'ok'})};},true);
  await e.connect();await e.capture();const injections=e.scripts.filter(x=>x.args).length;
  e.clock.now=Date.parse('2026-11-09T22:00:00Z');broken=false;await e.check();
  assert.equal(e.storage.pairing,undefined);assert.equal(e.storage.captureState.reason,'expired_connection');
  assert.equal(e.scripts.filter(x=>x.args).length,injections);
});
test('a saved source hold appearing during recovery cannot be bypassed by a healthy local status',async()=>{
  const e=env(async()=>({ok:true,status:200,json:async()=>({status:'ok'})}),true);
  await e.connect();await vm.runInContext("(async()=>pauseCapture('local_transport_error', (await chrome.storage.session.get('pairing')).pairing))()",e.c);
  e.local.sourceHold={status:403};const injections=e.scripts.filter(x=>x.args).length;
  await e.check();assert.equal(e.storage.pairing,undefined);assert.equal(e.storage.captureState.reason,'held');
  assert.equal(e.scripts.filter(x=>x.args).length,injections);
});
test('Stop wins while recovery is attaching its observer',async()=>{
  const e=env(async url=>({ok:true,status:200,json:async()=>({status:url.endsWith('/stop')?'stopping':'ok'})}),true);
  await e.connect();await vm.runInContext("(async()=>pauseCapture('local_transport_error', (await chrome.storage.session.get('pairing')).pairing))()",e.c);
  const execute=e.c.chrome.scripting.executeScript;let finish,began;
  const attaching=new Promise(r=>began=r);
  e.c.chrome.scripting.executeScript=options=>options.args?new Promise(r=>{finish=()=>r([{result:{capture_id:'late-generation'}}]);began();}):execute(options);
  const checking=e.check();await attaching;await e.send({type:'stop_capture'});finish();await checking;
  assert.equal(e.storage.pairing,undefined);assert.equal(e.storage.captureState.reason,'user_stopped');
});
