if (typeof importScripts === 'function') importScripts('session-policy.js','monitor.js');
const ENDPOINT='https://api.newsfilter.io/actions';
const LOCAL='http://127.0.0.1:8767/newsfilter', ORIGIN='https://newsfilter.io';
let revision=0, inFlight=false;
const monitor=CefBridgeMonitor.install(LOCAL,stopCapture);
const newsPage=url=>{try {const u=new URL(url);return u.origin===ORIGIN && (u.pathname==='/search'||u.pathname.startsWith('/latest/')||u.pathname.startsWith('/latest-news/'));}catch{return false;}};

async function stopCapture(reason,closeReceiver=false) {
  const op=++revision;
  const {pairing}=await chrome.storage.session.get('pairing');
  if(op!==revision) return op;
  await chrome.storage.session.remove('pairing');
  await monitor.clear();
  await chrome.storage.session.set({captureState:{status:'stopped',reason,receiverStopped:false}});
  if(!pairing) return op;
  try {await chrome.scripting.executeScript({target:{tabId:pairing.tabId},world:'MAIN',func:()=>window.CefNewsfilterObserver?.stop()});}catch{}
  if(closeReceiver) {
    try {
      const response=await fetch(LOCAL.replace('/newsfilter','/stop'),{method:'POST',redirect:'error',credentials:'omit',
        headers:{'Content-Type':'application/json','Authorization':'Bearer '+pairing.token},body:JSON.stringify({reason}),signal:AbortSignal.timeout(5000)});
      const confirmed=response.ok&&(await response.json()).status==='stopping';
      const state=await chrome.storage.session.get('pairing');
      if(!state.pairing) await chrome.storage.session.set({captureState:{status:'stopped',reason,receiverStopped:confirmed}});
    }catch{}
  }
  return op;
}

async function attach(pairing,op,reset=false,documentId,early=false) {
  const target=documentId?{tabId:pairing.tabId,documentIds:[documentId]}:{tabId:pairing.tabId};
  if(op!==revision) return {status:'superseded'};
  const captureId=crypto.randomUUID();
  await chrome.storage.session.set({pairing:{...pairing,active:true,attaching:true,captureId,documentId},captureState:{status:'waiting'},
    ...(reset?{frames:0,observations:0,dropped:0,lastResult:null,receiverStatus:null}:{})});
  for(const args of early?[]:[{files:['relay.js']},{world:'MAIN',files:['session-policy.js','observer.js']}]) {
    if(op!==revision) return {status:'superseded'};
    await chrome.scripting.executeScript({target,injectImmediately:true,...args});
  }
  if(op!==revision) return {status:'superseded'};
  const result=await chrome.scripting.executeScript({target,world:'MAIN',injectImmediately:true,
    func:lease=>window.CefNewsfilterObserver.start({mode:lease.mode,expires_at:new Date(lease.end).toISOString(),captureId:lease.captureId,startedAt:lease.startedAt,seconds:Math.max(1,Math.floor((lease.end-Date.now())/1000)),onEvent:payload=>{
      window.postMessage({type:'CEF_NEWSFILTER_NATIVE_COPY',payload},'https://newsfilter.io');
    }}),args:[{end:pairing.end,mode:pairing.mode,startedAt:pairing.startedAt,captureId}]});
  if(op!==revision) {
    try {await chrome.scripting.executeScript({target,world:'MAIN',func:id=>{
      if(window.CefNewsfilterObserver?.status().capture_id===id) window.CefNewsfilterObserver.stop();
    },args:[captureId]});}catch{}
    return {status:'superseded'};
  }
  if(result?.[0]?.result?.capture_id!==captureId) throw new Error('observer_not_started');
  await chrome.storage.session.set({pairing:{...pairing,active:true,attaching:false,captureId,documentId}});
  await monitor.arm();
  return {status:'started'};
}

async function startCapture(p,tabId) {
  const before=revision;
  const tab=await chrome.tabs.get(tabId);
  if(before!==revision) return {status:'superseded'};
  if(!newsPage(tab.url)) return {status:'error',error:'open_terminal'};
  if(p?.url!==LOCAL||!/^[A-Za-z0-9_-]{40,80}$/.test(p?.token||'')) return {status:'error',error:'invalid_connection'};
  let end;
  try {end=CefBridgeSession.end(p);} catch(error) {return {status:'error',error:error.message};}
  if(!Number.isFinite(end)||end<=Date.now()) return {status:'error',error:'expired_connection'};
  const stopped=await stopCapture('replaced');
  if(stopped!==revision) return {status:'superseded'};
  const op=++revision;
  try {return await attach({token:p.token,end,tabId,startedAt:Date.now(),mode:p.mode||'short'},op,true);}
  catch {if(op===revision) await stopCapture('start_failed');return {status:'error',error:'observer_not_started'};}
}

chrome.runtime.onMessage.addListener((message,sender,reply)=>{
  if(message?.type==='document_ready') {
    if(sender.origin!==ORIGIN||sender.frameId!==0||!sender.documentId||!newsPage(sender.url)) return;
    (async()=>{
      const before=revision;
      const {pairing}=await chrome.storage.session.get('pairing');
      if(before!==revision) return {status:'superseded'};
      if(!pairing?.active||pairing.tabId!==sender.tab?.id||Date.now()>=pairing.end) {
        try {await chrome.scripting.executeScript({target:{tabId:sender.tab.id,documentIds:[sender.documentId]},world:'MAIN',
          func:()=>window.CefNewsfilterObserver?.stop()});}catch{}
        return {status:'inactive'};
      }
      if(pairing.attaching||pairing.documentId===sender.documentId) return {status:'already_attached'};
      const op=++revision;
      try {return await attach(pairing,op,false,sender.documentId,true);}
      catch {if(op===revision) await stopCapture('navigation_attach_failed');return {status:'error'};}
    })().then(reply).catch(()=>reply({status:'error'}));return true;
  }
  if(['start_capture','stop_capture'].includes(message?.type)) {
    if(sender.id!==chrome.runtime.id||sender.url!==chrome.runtime.getURL('popup.html')) return;
    (message.type==='start_capture'?startCapture(message.pairing,message.tabId):stopCapture('user_stopped',true).then(()=>({status:'stopped'})))
      .then(reply).catch(()=>reply({status:'error',error:'bridge_error'}));return true;
  }
  if(message?.type!=='capture'||sender.origin!==ORIGIN||sender.frameId!==0) return;
  (async()=>{
    const {pairing}=await chrome.storage.session.get('pairing');
    if(!pairing?.active||pairing.tabId!==sender.tab?.id) return reply({status:'inactive'});
    if(pairing.documentId&&pairing.documentId!==sender.documentId) return reply({status:'invalid'});
    if(Date.now()>=pairing.end) {await stopCapture('expired_connection');return reply({status:'inactive'});}
    const p=message.payload;
    if(p?.endpoint!==ENDPOINT||p?.receipt_kind!=='passive_browser_copy'||p.capture_id!==pairing.captureId) return reply({status:'invalid'});
    if(inFlight) {
      const {dropped=0}=await chrome.storage.session.get('dropped');await chrome.storage.session.set({dropped:dropped+1});return reply({status:'busy'});
    }
    const body=JSON.stringify(p);
    if(new TextEncoder().encode(body).length>1024*1024) {await stopCapture('oversized_capture');return reply({status:'oversized'});}
    inFlight=true;
    try {
      const response=await fetch(LOCAL,{method:'POST',redirect:'error',credentials:'omit',cache:'no-store',
        headers:{'Content-Type':'application/json','Authorization':'Bearer '+pairing.token},body,signal:AbortSignal.timeout(5000)});
      const result=await response.json(), current=await chrome.storage.session.get(['pairing','frames','observations']);
      if(current.pairing?.captureId!==pairing.captureId) return reply({status:'superseded'});
      const accepted=response.ok&&result.status==='ok';
      await chrome.storage.session.set({lastResult:{at:new Date().toISOString(),http_status:response.status,...result,query:p.scope?.queryString},
        frames:(current.frames||0)+(accepted?1:0),observations:(current.observations||0)+(accepted?(result.articles||0):0),
        captureState:{status:accepted?'receiving':'receiver_error'}});
      if(!response.ok||['held','native_error'].includes(result.status)) await stopCapture(result.status||'receiver_error');
      reply(result);
    }catch {
      const current=await chrome.storage.session.get('pairing');
      if(current.pairing?.captureId===pairing.captureId) await stopCapture('local_receiver_unavailable');
      reply({status:'local_receiver_unavailable'});
    }finally{inFlight=false;}
  })().catch(()=>reply({status:'bridge_error'}));return true;
});

chrome.tabs.onRemoved.addListener(tabId=>{
  chrome.storage.session.get('pairing').then(({pairing})=>{if(pairing?.tabId===tabId) return stopCapture('tab_closed',true);}).catch(()=>{});
});
// document_start scripts handle allowed navigation before the site's first XHR.
// Navigation events only conclude a session that leaves news pages or expires.
chrome.webNavigation.onCommitted.addListener(details=>{
  if(details.frameId!==0) return;
  (async()=>{
    const before=revision;
    const {pairing}=await chrome.storage.session.get('pairing');
    if(before!==revision) return;
    if(!pairing?.active||pairing.tabId!==details.tabId) return;
    if(!newsPage(details.url)) return stopCapture('left_news_page',true);
    if(Date.now()>=pairing.end) return stopCapture('expired_connection');
  })().catch(()=>{});
});
