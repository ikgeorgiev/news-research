const {test}=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const script=fs.readFileSync(path.join(__dirname,'../newsfilter_bridge/popup.js'),'utf8');
const tick=()=>new Promise(r=>setTimeout(r,10));
function env(storage) {
  const els={};let changed;
  const c={Date,AbortSignal,setInterval:()=>{},document:{getElementById:id=>els[id]||={value:'',textContent:'',disabled:false,addEventListener:()=>{}}},
    chrome:{storage:{session:{get:async()=>storage},onChanged:{addListener:f=>changed=f}}}};
  vm.createContext(c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-session-policy.js'),'utf8'),c);vm.runInContext(fs.readFileSync(path.join(__dirname,'../bridge-monitor.js'),'utf8'),c);vm.runInContext(script,c);
  return {els,storage,changed:()=>changed({},'session')};
}
test('navigation waiting keeps prior successful delivery visible without claiming a new response',async()=>{
  const e=env({pairing:{active:true,end:Date.now()+60000},captureState:{status:'waiting'},frames:1,lastResult:{articles:50,query:'symbols:OXLC',at:new Date().toISOString()}});
  await tick();assert.equal(e.els['session-label'].textContent,'Connected · updates saved');assert.match(e.els.status.textContent,/Listening for the next response/);
  assert.equal(e.els.frames.textContent,'1');assert.equal(e.els.headlines.textContent,'50');
  e.storage.captureState.status='receiving';e.changed();await tick();assert.equal(e.els['session-label'].textContent,'Receiving news');
});
test('empty pairing and verified Stop have distinct truthful messages',async()=>{
  const e=env({pairing:{active:true,end:Date.now()+60000},captureState:{status:'waiting'},frames:0});await tick();
  assert.equal(e.els['session-label'].textContent,'Connected · waiting');assert.equal(e.els.headlines.textContent,'—');
  delete e.storage.pairing;e.storage.captureState={reason:'user_stopped',receiverStopped:true};e.changed();await tick();
  assert.match(e.els.status.textContent,/receiver have stopped/);assert.equal(e.els.stop.disabled,true);
});
test('leaving supported pages explains the stop instead of showing the initial welcome',async()=>{
  const e=env({captureState:{status:'stopped',reason:'left_news_page',receiverStopped:true},frames:0});await tick();
  assert.match(e.els.status.textContent,/paired tab left the supported news pages/);
  e.storage.captureState.reason='navigation_attach_failed';e.changed();await tick();
  assert.match(e.els.status.textContent,/could not attach to the new page/);
});
