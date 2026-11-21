const {test}=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const policy=fs.readFileSync(path.join(__dirname,'../bridge-session-policy.js'),'utf8');
const monitor=fs.readFileSync(path.join(__dirname,'../bridge-monitor.js'),'utf8');
function policyAt(iso){const c={Date,Set};vm.createContext(c);vm.runInContext(policy,c);return {p:c.CefBridgeSession,now:Date.parse(iso)};}
test('all-day lease survives twenty minutes but cannot cross midnight Kyiv',()=>{
  const {p,now}=policyAt('2026-11-09T08:00:00Z');
  const lease={mode:'until_close',expires_at:'2026-11-09T22:00:00Z'};
  assert.equal(p.end(lease,now),Date.parse(lease.expires_at));
  assert.equal(p.observerEnd(lease,now+21*60*1000),Date.parse(lease.expires_at));
  assert.throws(()=>p.end({...lease,expires_at:'2026-11-10T22:00:00Z'},now));
  assert.throws(()=>p.end(lease,Date.parse('2026-11-09T22:00:00Z')));
});
test('weekday dates and short legacy sessions retain their boundaries',()=>{
  const {p,now}=policyAt('2026-11-09T08:00:00Z');
  assert.equal(p.end({expires_at:'2026-11-09T22:00:00Z'},now),now+1200000);
  for(const day of ['2026-11-08','2026-11-14','2026-11-16'])assert.throws(()=>p.end({mode:'until_close',expires_at:day+'T22:00:00Z'},Date.parse(day+'T12:00:00Z')));
  assert.throws(()=>p.end({mode:'until_close',expires_at:'2026-11-09T22:00:00Z'},Date.parse('2026-11-09T07:59:59Z')));
});
test('freshness ages even while the popup remains open; connected is not live',()=>{
  const {p,now}=policyAt('2026-11-09T08:00:00Z');
  const state={pairing:{active:true,startedAt:now,end:now+3600000}};
  assert.equal(p.freshness(state,now),'waiting');assert.equal(p.freshness(state,now+180001),'no_updates');
  state.lastResult={at:new Date(now).toISOString()};assert.equal(p.freshness(state,now+60000),'recent_copy');assert.equal(p.freshness(state,now+180001),'stale');
});
test('shared policy and monitor shipped identically in both extensions',()=>{
  for(const dir of ['godel_bridge','newsfilter_bridge']) {
    assert.equal(fs.readFileSync(path.join(__dirname,'..',dir,'session-policy.js'),'utf8'),policy);
    assert.equal(fs.readFileSync(path.join(__dirname,'..',dir,'monitor.js'),'utf8'),monitor);
    const m=JSON.parse(fs.readFileSync(path.join(__dirname,'..',dir,'manifest.json')));
    assert.match(m.version,/^0\.[3-9]\.\d+$/);assert.ok(m.permissions.includes('alarms'));
    assert.equal(m.host_permissions.length,2);
  }
});
test('status alarm only calls loopback and never starts or refreshes a source',async()=>{
  const calls=[],badges=[],stops=[],storage={pairing:{active:true,token:'token',end:Date.now()+600000,captureId:'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'}};
  const c={Date,AbortSignal,chrome:{storage:{session:{get:async()=>storage,set:async x=>Object.assign(storage,x)}},action:{setBadgeText:async v=>badges.push(v.text),setBadgeBackgroundColor:async()=>{}},alarms:{onAlarm:{addListener:()=>{}},get:async()=>({}),create:async()=>{},clear:async()=>{}}},fetch:async(url,opts)=>{calls.push({url,opts});return {ok:true,json:async()=>({native_status:'stale'})};}};
  vm.createContext(c);vm.runInContext(monitor,c);
  const m=c.CefBridgeMonitor.install('http://127.0.0.1:8767/newsfilter',async reason=>stops.push(reason));
  await m.check();assert.equal(calls.length,1);assert.equal(calls[0].url,'http://127.0.0.1:8767/status');assert.equal(badges.at(-1),'GAP');assert.deepEqual(stops,[]);
  storage.pairing.end=0;await m.check();assert.deepEqual(stops,['expired_connection']);assert.equal(calls.length,1);
});
for(const outcome of ['failure','success'])test(`late status ${outcome} cannot change a newer connection`,async()=>{
  const stops=[],badges=[],clears=[];
  const storage={pairing:{active:true,token:'old-token',end:Date.now()+600000,captureId:'old-capture'}};
  let release,requested;
  const began=new Promise(resolve=>{requested=resolve;});
  const pending=new Promise((resolve,reject)=>{release=()=>outcome==='failure'?reject(new Error('old receiver closed')):resolve({ok:true,json:async()=>({native_status:'fresh'})});});
  const c={Date,AbortSignal,chrome:{storage:{session:{get:async()=>({...storage}),set:async x=>Object.assign(storage,x)}},action:{setBadgeText:async v=>badges.push(v.text),setBadgeBackgroundColor:async()=>{}},alarms:{onAlarm:{addListener:()=>{}},get:async()=>({}),create:async()=>{},clear:async name=>clears.push(name)}},fetch:()=>{requested();return pending;}};
  vm.createContext(c);vm.runInContext(monitor,c);
  const m=c.CefBridgeMonitor.install('http://127.0.0.1:8766/godel',async reason=>stops.push(reason));
  const checking=m.check();await began;
  storage.pairing={active:true,token:'new-token',end:Date.now()+600000,captureId:'new-capture'};
  release();await checking;
  assert.deepEqual(stops,[]);assert.deepEqual(badges,[]);assert.deepEqual(clears,[]);
  assert.equal(storage.pairing.token,'new-token');assert.equal(storage.receiverStatus,undefined);
});
