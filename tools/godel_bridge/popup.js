const $ = id => document.getElementById(id);
const loadedVersion=chrome.runtime?.getManifest?.()?.version;
if(loadedVersion) $('bridge-version').textContent=loadedVersion;
const errors = {
  saved_source_hold:'A saved native refusal or copy failure needs review before capture can restart.',
  outside_weekday_window:'Daily sessions run November9–13, 10:00–24:00 Kyiv only.',
  pairing_expired:'The ten-minute connection code expired. Start a new receiver for a fresh code.',
  open_terminal:'Open the Godel terminal tab first, then reopen this extension.',
  invalid_connection:'Enter the code from a running local receiver.',
  expired_connection:'This connection expired. Start a new local receiver for a fresh code.',
  connection_code:'That code does not match. Check the eight characters and try again.',
  pairing_locked:'Too many incorrect codes. Start a new local receiver.',
  already_paired:'This receiver is already paired with another extension.',
  local_receiver_unavailable:'The local receiver is not running. Start it to get a new code.',
  observer_not_started:'The terminal capture could not start. Reload Godel and try again.',
  start_in_progress:'The connection is still starting.',
  bridge_error:'The extension could not complete this action. Reopen it and try again.'
};
let state = {}, busy = false;
function render() {
  const p = state.pairing, valid = !!p && p.end > Date.now(), active = valid && p.active;
  const stale = active && ['stale','no_updates'].includes(CefBridgeSession.freshness(state));
  const delayed = active && state.lastResult?.review_only;
  const received = active && !stale && !delayed && state.captureState?.status === 'receiving';
  const recovering = valid && p.recovering, resumable = valid && !p.active && !recovering;
  $('session-label').textContent = recovering?'Reconnecting locally':delayed?'Delayed copy · monitoring':stale?'No recent updates':received?'Receiving news':active?'Connected · waiting':resumable?'Capture paused':'Ready to connect';
  $('lamp').className = 'lamp'+(received?' live':active?'':' off');
  $('remaining').textContent = valid?Math.max(1,Math.ceil((p.end-Date.now())/60000))+' MIN LEFT':'MANUAL SESSION';
  const reason = state.captureState?.reason;
  $('status').textContent = recovering
    ? 'Recovering this daily session automatically. No new code needed. The gap is recorded; no Godel requests are added.'
    : active
    ? (received?'New terminal responses are being saved locally.':'Waiting for the terminal’s next natural news update.')
    : resumable?'Godel reloaded. Click Resume capture to start a fresh baseline.'
    : reason==='user_stopped'?(state.captureState.receiverStopped?'Capture and the local receiver have stopped.':'Capture stopped. Receiver shutdown is unconfirmed; its time limit still applies.')
    : reason==='tab_closed'?'The terminal tab closed. Open Godel and use a new connection code.'
    : reason==='local_receiver_unavailable'?errors.local_receiver_unavailable
    : ['held','native_error','receiver_error','receiver_response_invalid','receiver_status_invalid','local_http_terminal','observer_not_started'].includes(reason)?'Capture stopped after a source or receiver error. The saved evidence is ready for review.'
    : p || reason==='expired_connection'?'The session ended. Use a new code for another capture.'
    :'Open Godel, enter the connection code, then start.';
  if(stale) $('status').textContent='No native news response for over three minutes. This interval is unobserved; a connected tab does not prove a live feed.';
  if(delayed) $('status').textContent='A delayed browser copy was saved for review. Monitoring continues; this copy is excluded from speed measurements.';
  const assessment=state.lastResult?.assessment, scope=assessment?.filter_scope;
  const unknownScope=scope && ['symbols_omitted_unverified','unverified_expression'].includes(scope.mode);
  $('coverage').textContent=unknownScope
    ? 'Ticker coverage is unknown for this response. Other news panels keep their own filters.'
    : scope ? scope.eligible_count+'/'+scope.universe_count+' CEF symbols eligible under this response’s filter. This is not verified release coverage.' : 'CEF filter coverage awaits the first response.';
  if(assessment?.continuity?.full_page) $('coverage').textContent+=' Full result page; overflow remains possible.';
  if(state.lastResult?.transport==='native_websocket') $('coverage').textContent+=' Native live stream; each message keeps its own arrival time.';
  if(state.lastResult?.transport==='native_worker') $('coverage').textContent+=' Native worker news; timestamps measure the copy delivered to the page.';
  $('session-note').textContent=valid ? (p.mode==='until_close'?'Daily session · stops at midnight Kyiv.':'Short check · original deadline retained.') : '';
  $('frames').textContent = String(state.frames || 0);
  $('headlines').textContent = String(state.lastResult?.articles ?? '—');
  const symbols = state.lastResult?.symbols;
  $('filter').textContent = state.lastResult?.symbolsOmitted ? 'Ticker filter omitted · scope unverified · '+(state.lastResult.sources || 0)+' sources' : Array.isArray(symbols)?(symbols.length?symbols.join(', '):'All symbols')+' · '+(state.lastResult.sources || 0)+' sources':'Appears after the first update';
  $('received').textContent = state.lastResult?.at?'Last update '+new Date(state.lastResult.at).toLocaleTimeString()+(state.dropped?' · '+state.dropped+' skipped while busy':'')+(state.lastResult.streamDropped?' · '+state.lastResult.streamDropped+' stream frames dropped':''):active?'Awaiting first update':'Waiting for a connection';
  $('start').disabled = busy || !!active || !!recovering;
  $('start').textContent = busy?'Connecting…':resumable?'Resume capture ↗':'Connect & start ↗';
  $('stop').disabled = busy || !valid;
  $('code').disabled = busy || !!active || !!recovering;
}
function diagnosticSummary(saved, local) {
  // Explicit projection: never export pairing, filter strings, URLs, bodies or
  // arbitrary stored error text. Unknown codes remain unknown.
  const code = x => typeof x==='string' && /^[a-z][a-z0-9_]{0,63}$/.test(x) ? x : null;
  const at = x => typeof x==='string' && /^\d{4}-\d\d-\d\dT[\d:.]+(?:Z|\+00:00)$/.test(x) ? x : null;
  const number = x => Number.isFinite(x) && x>=0 && x<=1e12 ? x : null;
  const result=saved.lastResult||{}, capture=saved.captureState||{};
  return {version:loadedVersion||null,updates_saved:number(saved.frames),dropped:number(saved.dropped),
    capture:{status:code(capture.status),reason:code(capture.reason),at:at(capture.at),receiver_stopped:capture.receiverStopped===true},
    last_reply:{at:at(result.at),http_status:number(result.http_status),status:code(result.status),error:code(result.error),
      transport:code(result.transport),articles:number(result.articles),validation_reason:code(result.diagnostic?.reason),record_index:number(result.diagnostic?.record_index)},
    source_hold_present:!!local.sourceHold,
    recent_events:(Array.isArray(local.diagnostics)?local.diagnostics:[]).slice(-12).map(x=>({at:at(x?.at),kind:code(x?.kind),http_status:number(x?.http_status),gap_ms:number(x?.gap_ms)}))};
}
async function reloadState() {
  state=await chrome.storage.session.get(['pairing','captureState','lastResult','frames','dropped','receiverStatus']);render();
  const local=await (chrome.storage.local||chrome.storage.session).get(['diagnostics','sourceHold']);
  $('diagnostics').value=JSON.stringify(diagnosticSummary(state,local),null,2);
}
async function activeTab() {
  const [tab] = await chrome.tabs.query({active:true,currentWindow:true});
  if (!tab?.url?.startsWith('https://app.godelterminal.com/')) throw new Error('open_terminal');
  return tab;
}
$('start').addEventListener('click',async () => {
  busy=true;$('message').textContent='';render();
  try {
    const tab=await activeTab();
    const code=$('code').value.toUpperCase().replace(/[-\s]/g,'');
    let pairing;
    if ($('pairing').value.trim()) pairing=JSON.parse($('pairing').value);
    else if (code) {
      if (!/^[A-Z2-9]{8}$/.test(code)) throw new Error('connection_code');
      let response;
      try {
        response=await fetch('http://127.0.0.1:8766/pair',{method:'POST',credentials:'omit',redirect:'error',cache:'no-store',
          headers:{'Content-Type':'application/json','X-CEF-Bridge':'1'},body:JSON.stringify({code,client_id:chrome.runtime.id}),
          signal:AbortSignal.timeout(5000)});
      } catch { throw new Error('local_receiver_unavailable'); }
      pairing=await response.json();
      if (!response.ok) throw new Error(pairing.error || 'invalid_connection');
    } else if (state.pairing && state.pairing.end>Date.now()) {
      pairing={url:'http://127.0.0.1:8766/godel',token:state.pairing.token,expires_at:new Date(state.pairing.end).toISOString(),mode:state.pairing.mode||'short'};
    } else throw new Error('invalid_connection');
    const result=await chrome.runtime.sendMessage({type:'start_capture',pairing,tabId:tab.id});
    if (result?.status!=='started') throw new Error(result?.error || 'bridge_error');
    $('code').value='';$('pairing').value='';
  } catch(e) { $('message').textContent=errors[e.message] || 'Could not connect. Check the code and local receiver.'; }
  finally {busy=false;await reloadState();}
});
$('stop').addEventListener('click',async () => {
  busy=true;$('message').textContent='';render();
  try { await chrome.runtime.sendMessage({type:'stop_capture'}); }
  catch { $('message').textContent=errors.bridge_error; }
  finally {busy=false;await reloadState();}
});
chrome.storage.onChanged.addListener((changes,area) => {if(area==='session'||area==='local') void reloadState();});
void reloadState();
setInterval(render,1000);
