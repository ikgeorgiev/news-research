/* A local-receiver check, not a source poll. No capture starts here. */
(() => {
  const NAME='cef-bridge-status';
  let running=false;
  async function badge(text,color) {
    if (!chrome.action?.setBadgeText) return;
    // A UI failure must never tear down an otherwise healthy capture.
    try {
      await chrome.action.setBadgeText({text});
      if (color) await chrome.action.setBadgeBackgroundColor({color});
    } catch { /* Status remains available in storage and the popup. */ }
  }
  function install(local,stopCapture,recovery) {
    async function check() {
      if(running) return;
      running=true;
      let checkedPairing, failure='local_transport_error', httpStatus;
      try {
        const state=await chrome.storage.session.get(['pairing','dropped','lastResult','diagnosticPending']);
        const p=state.pairing;
        checkedPairing=p;
        if(!p) {await clear();return;}
        if(Date.now()>=p.end) {await stopCapture('expired_connection',true);await clear();return;}
        const response=await fetch(local.replace(/\/(godel|newsfilter)$/,'/status'),{
          method:'POST',redirect:'error',credentials:'omit',cache:'no-store',
          headers:{'Content-Type':'application/json',Authorization:'Bearer '+p.token},
          body:JSON.stringify({active:!!p.active,capture_id:p.captureId,dropped:state.dropped||0,
            ...(recovery?{diagnostics:(state.diagnosticPending||[]).slice(-6)}:{})}),signal:AbortSignal.timeout(5000)});
        httpStatus=response.status;
        if(!response.ok) {
          failure=[500,502,503,504].includes(response.status)?'local_http_temporary':'local_http_terminal';
          throw new Error(failure);
        }
        failure='receiver_status_invalid';
        const result=await response.json();
        const current=await chrome.storage.session.get('pairing');
        if(current.pairing?.captureId!==p.captureId || current.pairing?.token!==p.token) return;
        if(recovery && result.status!=='ok') {
          await recovery.record('receiver_status_terminal',p,{http_status:httpStatus});
          await stopCapture(result.status==='held'?'held':'receiver_status_invalid',true);return;
        }
        await chrome.storage.session.set({receiverStatus:result});
        if(recovery) {
          await recovery.ack(state.diagnosticPending||[]);
          if(p.recovering) {await recovery.resume(p);return;}
        }
        await badge(!p.active?'PAUSE':result.native_status==='fresh'?'LIVE':'GAP',p.active&&result.native_status==='fresh'?'#1e6e4d':'#a05c12');
      } catch {
        const {pairing}=await chrome.storage.session.get('pairing');
        if(checkedPairing && pairing?.token===checkedPairing.token && pairing?.captureId===checkedPairing.captureId) {
          if(recovery && ['local_transport_error','local_http_temporary'].includes(failure) && pairing.mode==='until_close') {
            await recovery.pause(failure,pairing,{http_status:httpStatus});
            await badge('RETRY','#a05c12');
          } else {
            if(recovery) await recovery.record(failure,pairing,{http_status:httpStatus});
            await stopCapture(recovery?failure:'local_receiver_unavailable');
            await clear();
          }
        }
      } finally {running=false;}
    }
    async function arm() {
      if(chrome.alarms && !await chrome.alarms.get(NAME)) await chrome.alarms.create(NAME,{periodInMinutes:1});
      await badge('WAIT','#946313');
    }
    async function clear() {
      if(chrome.alarms) await chrome.alarms.clear(NAME);
      await badge('');
    }
    chrome.alarms?.onAlarm.addListener(alarm=>{if(alarm.name===NAME)void (async()=>{
      if(recovery && Date.now()-alarm.scheduledTime>90000) await recovery.record('alarm_delayed',null,{gap_ms:Date.now()-alarm.scheduledTime});
      await check();
    })().catch(()=>{});});
    if(chrome.alarms) void chrome.storage.session.get('pairing').then(async({pairing})=>{
      if(pairing) {
        if(!await chrome.alarms.get(NAME))await arm();
        if(recovery) {await recovery.record('worker_started',pairing);await check();}
      }
    }).catch(()=>{});
    return {arm,clear,check};
  }
  globalThis.CefBridgeMonitor={install};
})();
