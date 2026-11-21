if (typeof importScripts === 'function') importScripts('session-policy.js','monitor.js');
const ENDPOINT = 'https://terminal-api.godelterminal.com/api/news/items';
const LOCAL = 'http://127.0.0.1:8766/godel';
const ORIGIN = 'https://app.godelterminal.com';
let inFlight = false, starting = false, resuming = false, operation = 0;
let journal = Promise.resolve(), eventSequence = 0;
const monitor = CefBridgeMonitor.install(LOCAL,stopCapture,{pause:pauseCapture,resume:resumeCapture,record,ack});

function same(a,b) {return !!a && !!b && a.token===b.token && a.captureId===b.captureId;}
function record(kind,p,details={}) {
  // Selected diagnostics only: no token, URL, response body or account data.
  const event={id:Date.now().toString(36)+'-'+(++eventSequence)+'-'+Math.random().toString(36).slice(2,10),
    at:new Date().toISOString(),kind,capture_id:p?.captureId||null};
  if(Number.isInteger(details.http_status))event.http_status=details.http_status;
  if(Number.isFinite(details.gap_ms))event.gap_ms=Math.max(0,Math.round(details.gap_ms));
  journal=journal.catch(()=>{}).then(async()=>{
    const area=chrome.storage.local||chrome.storage.session;
    const {diagnostics=[]}=await area.get('diagnostics');
    await area.set({diagnostics:[...diagnostics,event].slice(-128)});
    const {diagnosticPending=[]}=await chrome.storage.session.get('diagnosticPending');
    await chrome.storage.session.set({diagnosticPending:[...diagnosticPending,event].slice(-6)});
  });
  return journal;
}
function ack(events) {
  const ids=new Set(events.map(e=>e.id));
  journal=journal.catch(()=>{}).then(async()=>{
    const {diagnosticPending=[]}=await chrome.storage.session.get('diagnosticPending');
    await chrome.storage.session.set({diagnosticPending:diagnosticPending.filter(e=>!ids.has(e.id))});
  });return journal;
}
async function stopObserver(p) {
  try {await chrome.scripting.executeScript({target:{tabId:p.tabId},world:'MAIN',func:()=>window.CefGodelObserver?.stop()});}
  catch (error) {console.warn('Godel observer stop failed:',String(error?.message));await record('observer_stop_failed',p);}
}
async function attachObserver(p) {
  await chrome.scripting.executeScript({target:{tabId:p.tabId},files:['relay.js']});
  await chrome.scripting.executeScript({target:{tabId:p.tabId},world:'MAIN',files:['session-policy.js','stream-observer.js','worker-observer.js','observer.js']});
  const result=await chrome.scripting.executeScript({target:{tabId:p.tabId},world:'MAIN',func:lease=>
    window.CefGodelObserver.start({mode:lease.mode,expires_at:new Date(lease.end).toISOString(),seconds:Math.max(1,Math.floor((lease.end-Date.now())/1000)),onEvent:payload=>{
      window.postMessage({type:'CEF_GODEL_NATIVE_COPY',payload},'https://app.godelterminal.com');
    }}),args:[{end:p.end,mode:p.mode}]});
  const captureId=result?.[0]?.result?.capture_id;
  if(!captureId)throw new Error('observer_not_started');
  return captureId;
}
async function pauseCapture(reason,p,details={}) {
  const seen=operation;
  const {pairing}=await chrome.storage.session.get('pairing');
  if(!same(pairing,p))return;
  if(pairing.mode!=='until_close')return stopCapture('local_receiver_unavailable');
  await record(reason,p,details);
  if(seen!==operation)return;
  if(pairing.recovering)return;
  const op=++operation, gapStartedAt=Date.now();
  await chrome.storage.session.set({pairing:{...pairing,active:false,recovering:true,gapStartedAt},
    captureState:{status:'recovering',reason,at:new Date().toISOString()}});
  await stopObserver(pairing);
  if(op===operation)await monitor.arm();
}
async function resumeCapture(p) {
  if(starting||resuming)return;
  resuming=true;
  const op=++operation;
  try {
    const {pairing}=await chrome.storage.session.get('pairing');
    if(!same(pairing,p)||!pairing.recovering)return;
    const {sourceHold}=await (chrome.storage.local||chrome.storage.session).get('sourceHold');
    if(sourceHold)return await stopCapture('held',true);
    if(Date.now()>=p.end)return await stopCapture('expired_connection',true);
    const tab=await chrome.tabs.get(p.tabId);
    if(!tab.url?.startsWith(ORIGIN+'/'))return await stopCapture('left_news_page',true);
    if(tab.status==='loading')return;
    const captureId=await attachObserver(p);
    const current=await chrome.storage.session.get('pairing');
    if(op!==operation||!same(current.pairing,p)||Date.now()>=p.end) {await stopObserver(p);return;}
    await chrome.storage.session.set({pairing:{...p,active:true,recovering:false,captureId},receiverStatus:null,
      captureState:{status:'waiting',reason:'recovered',at:new Date().toISOString()}});
    await record('capture_recovered',{...p,captureId},{gap_ms:Date.now()-(p.gapStartedAt||Date.now())});
    await monitor.arm();
  } catch {
    if(op===operation) {await record('observer_recovery_failed',p);await stopCapture('observer_not_started');}
  } finally {resuming=false;}
}

async function stopCapture(reason, closeReceiver = false, retainConnection = false) {
  const op=++operation;
  const {pairing} = await chrome.storage.session.get('pairing');
  if(op!==operation)return;
  let receiverStopped = false;
  await chrome.storage.session.remove('pairing');
  await monitor.clear();
  if (pairing) {
    await record(reason,pairing);
    await stopObserver(pairing);
    if (closeReceiver) {
      try {
        const response = await fetch(LOCAL.replace('/godel','/stop'),{method:'POST',redirect:'error',credentials:'omit',
          headers:{'Content-Type':'application/json','Authorization':'Bearer '+pairing.token},body:JSON.stringify({reason:['user_stopped','tab_closed','left_news_page','expired_connection'].includes(reason)?reason:'receiver_error'}),
          signal:AbortSignal.timeout(5000)});
        receiverStopped = response.ok && (await response.json()).status === 'stopping';
      } catch { /* A stopped or expired receiver needs no retry. */ }
    }
    if(op!==operation)return;
    if (retainConnection && pairing.end > Date.now()) {
      await chrome.storage.session.set({pairing:{...pairing,active:false}});
      await monitor.arm();
    } else await chrome.storage.session.remove('pairing');
  }
  if(op!==operation)return;
  await chrome.storage.session.set({captureState:{status:'stopped',reason,receiverStopped,at:new Date().toISOString()}});
}

async function startCapture(p, tabId) {
  if (starting||resuming) return {status:'error',error:'start_in_progress'};
  starting = true;
  let changed = false, op;
  try {
    const {sourceHold}=await (chrome.storage.local||chrome.storage.session).get('sourceHold');
    if(sourceHold)throw new Error('saved_source_hold');
    const tab = await chrome.tabs.get(tabId);
    if (!tab.url?.startsWith(ORIGIN+'/')) throw new Error('open_terminal');
    if (p?.url !== LOCAL || !/^[A-Za-z0-9_-]{40,80}$/.test(p?.token || '')) throw new Error('invalid_connection');
    const end = CefBridgeSession.end(p);
    const mode = p.mode || 'short', startedAt = Date.now();
    if (!Number.isFinite(end) || end <= Date.now()) throw new Error('expired_connection');
    changed = true;
    await stopCapture('replaced');
    op=operation;
    await chrome.storage.session.set({pairing:{token:p.token,end,tabId,active:true,mode,startedAt},frames:0,observations:0,dropped:0,
      lastResult:null,receiverStatus:null,captureState:{status:'starting'}});
    const captureId=await attachObserver({end,mode,tabId});
    if(op!==operation) {await stopObserver({tabId});return {status:'error',error:'superseded'};}
    await chrome.storage.session.set({pairing:{token:p.token,end,tabId,active:true,captureId,mode,startedAt},captureState:{status:'waiting'}});
    await monitor.arm();
    await record('capture_started',{captureId});
    return {status:'started'};
  } catch (error) {
    if (changed && op===operation) await stopCapture('start_failed');
    return {status:'error',error:error.message};
  } finally { starting = false; }
}

chrome.runtime.onMessage.addListener((message,sender,reply) => {
  if (['start_capture','stop_capture'].includes(message?.type)) {
    if (sender.id !== chrome.runtime.id || sender.url !== chrome.runtime.getURL('popup.html')) return;
    const work = message.type === 'start_capture' ? startCapture(message.pairing,message.tabId) :
      stopCapture('user_stopped',true).then(() => ({status:'stopped'}));
    work.then(reply).catch(() => reply({status:'error',error:'bridge_error'}));
    return true;
  }
  if (message?.type !== 'capture' || sender.origin !== ORIGIN) return;
  (async () => {
    const {pairing} = await chrome.storage.session.get('pairing');
    if (!pairing?.active || pairing.tabId !== sender.tab?.id) return reply({status:'inactive'});
    if (Date.now() >= pairing.end) {
      await stopCapture('expired_connection');
      return reply({status:'inactive'});
    }
    const p = message.payload;
    const worker=p?.version===3&&p?.endpoint===ORIGIN+'/'&&p?.receipt_kind==='passive_browser_worker_batch';
    const stream=worker||(p?.version===2&&p?.endpoint==='wss://terminal-api.godelterminal.com/events'&&p?.receipt_kind==='passive_browser_stream_batch');
    if ((!stream&&(p?.endpoint !== ENDPOINT || p?.receipt_kind !== 'passive_browser_copy')) || p.capture_id !== pairing.captureId) return reply({status:'invalid'});
    if (inFlight) {
      const {dropped=0} = await chrome.storage.session.get('dropped');
      await chrome.storage.session.set({dropped:dropped+1});
      return reply({status:'busy'});
    }
    const body = JSON.stringify(p);
    if (new TextEncoder().encode(body).length > 1024*1024) return reply({status:'oversized'});
    const sourceFailure=!stream && (!!p.error || [401,403,404,429].includes(p.status));
    if(sourceFailure)await (chrome.storage.local||chrome.storage.session).set({sourceHold:{at:new Date().toISOString(),status:p.status||null,reason:'native_refusal_or_copy_failure'}});
    inFlight = true;
    let failure='local_transport_error',httpStatus;
    try {
      const response = await fetch(LOCAL,{method:'POST',redirect:'error',credentials:'omit',cache:'no-store',
        headers:{'Content-Type':'application/json','Authorization':'Bearer '+pairing.token},
        body,signal:AbortSignal.timeout(5000)});
      httpStatus=response.status;
      if([500,502,503,504].includes(response.status))throw new Error('local_temporary');
      failure='receiver_response_invalid';
      const result = await response.json();
      const current = await chrome.storage.session.get(['pairing','frames','observations']);
      if (current.pairing?.captureId !== pairing.captureId) return reply({status:'superseded'});
      const accepted = response.ok && result.status === 'ok';
      const filters=stream?p.records?.at(-1)?.scope:p.scope;
      await chrome.storage.session.set({lastResult:{at:new Date().toISOString(),http_status:response.status,...result,
        transport:worker?'native_worker':stream?'native_websocket':'native_http',streamDropped:stream?p.dropped_frames:0,
        symbols:filters?.symbols ?? null,symbolsOmitted:!!filters&&!Object.hasOwn(filters,'symbols'),sources:filters?.sources?.length || 0},
        frames:(current.frames || 0)+(accepted?1:0),observations:(current.observations || 0)+(accepted?(result.articles || 0):0),
        captureState:{status:accepted?'receiving':result.status || 'receiver_error'}});
      if (!response.ok || ['held','native_error'].includes(result.status)) await stopCapture(result.status || 'receiver_error',true);
      reply(result);
    } catch {
      const current = await chrome.storage.session.get('pairing');
      if (current.pairing?.captureId === pairing.captureId) {
        const {dropped=0}=await chrome.storage.session.get('dropped');
        await chrome.storage.session.set({dropped:dropped+1});
        // Never replay an uncertain POST; the next generation starts a new baseline.
        if(sourceFailure) {await record('native_failure_delivery_lost',pairing,{http_status:p.status});await stopCapture('native_error');}
        else if(failure==='local_transport_error' && pairing.mode==='until_close')await pauseCapture('capture_transport_error',pairing,{http_status:httpStatus});
        else {await record(failure,pairing,{http_status:httpStatus});await stopCapture(pairing.mode==='until_close'?failure:'local_receiver_unavailable');}
      }
      reply({status:'local_receiver_unavailable'});
    } finally { inFlight = false; }
  })().catch(() => reply({status:'bridge_error'}));
  return true;
});

chrome.tabs.onRemoved.addListener(tabId => {
  chrome.storage.session.get('pairing').then(({pairing}) => {
    if (pairing?.tabId === tabId) return stopCapture('tab_closed',true);
  }).catch(() => {});
});
chrome.tabs.onUpdated.addListener((tabId,change) => {
  if (!['loading','complete'].includes(change.status)) return;
  chrome.storage.session.get('pairing').then(({pairing}) => {
    if(pairing?.tabId!==tabId)return;
    if(change.status==='loading' && pairing.active)return pairing.mode==='until_close'?pauseCapture('tab_reloaded',pairing):stopCapture('tab_reloaded',false,true);
    if(change.status==='complete' && pairing.recovering)return monitor.check();
  }).catch(() => {});
});
