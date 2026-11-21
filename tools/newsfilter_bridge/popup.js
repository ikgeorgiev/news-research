const $ = id => document.getElementById(id);
const loadedVersion=chrome.runtime?.getManifest?.()?.version;
if(loadedVersion) $('bridge-version').textContent=loadedVersion;
const errors = {
  outside_weekday_window:'Daily sessions run November9–13, 10:00–24:00 Kyiv only.',
  pairing_expired:'The ten-minute connection code expired. Start a new receiver for a fresh code.',
  open_terminal:'Open a signed-in Newsfilter search or latest-news page, then reopen this extension.',
  invalid_connection:'Enter the code from a running local receiver.',
  expired_connection:'This connection expired. Start a new local receiver for a fresh code.',
  connection_code:'That code does not match. Check the eight characters and try again.',
  pairing_locked:'Too many incorrect codes. Start a new local receiver.',
  already_paired:'This receiver is already paired with another extension.',
  local_receiver_unavailable:'The local receiver is not running. Start it to get a new code.',
  observer_not_started:'The terminal capture could not start. Reload Newsfilter and try again.',
  start_in_progress:'The connection is still starting.',
  bridge_error:'The extension could not complete this action. Reopen it and try again.'
};
let state = {}, busy = false;
function render() {
  const p = state.pairing, valid = !!p && p.end > Date.now(), active = valid && p.active;
  const stale = active && ['stale','no_updates'].includes(CefBridgeSession.freshness(state));
  const received = active && !stale && state.captureState?.status === 'receiving';
  const saved = active && (state.frames || 0) > 0;
  const resumable = valid && !p.active;
  $('session-label').textContent = stale?'No recent updates':received?'Receiving news':saved?'Connected · updates saved':active?'Connected · waiting':resumable?'Capture paused':'Ready to connect';
  $('lamp').className = 'lamp'+(received?' live':active?'':' off');
  $('remaining').textContent = valid?Math.max(1,Math.ceil((p.end-Date.now())/60000))+' MIN LEFT':'MANUAL SESSION';
  const reason = state.captureState?.reason;
  $('status').textContent = active
    ? (received?'News responses are being saved locally.':saved?'Your saved updates are retained. Listening for the next response on this page.':'Listening. Browse or search Newsfilter normally; this bridge does not refresh the site.')
    : resumable?'Newsfilter reloaded. Click Resume capture to start a fresh baseline.'
    : reason==='user_stopped'?(state.captureState.receiverStopped?'Capture and the local receiver have stopped.':'Capture stopped. Receiver shutdown is unconfirmed; its time limit still applies.')
    : reason==='tab_closed'?'The terminal tab closed. Open Newsfilter and use a new connection code.'
    : reason==='left_news_page'?'Capture stopped because the paired tab left the supported news pages. Return to a Newsfilter search or latest-news page and use a new connection code.'
    : reason==='navigation_attach_failed'?'Capture stopped because the extension could not attach to the new page. Its original connection has not been reused.'
    : reason==='oversized_capture'?'Capture stopped because the copied response exceeded the local size limit. The saved updates remain available.'
    : reason==='start_failed'?'The extension could not attach to this page. Capture did not start.'
    : reason==='local_receiver_unavailable'?errors.local_receiver_unavailable
    : ['held','native_error','receiver_error'].includes(reason)?'Capture stopped after a source or receiver error. The saved evidence is ready for review.'
    : p || reason==='expired_connection'?'The session ended. Use a new code for another capture.'
    :'Open Newsfilter, enter the connection code, then start.';
  if(stale) $('status').textContent='No native news response for over three minutes. This interval is unobserved; a connected tab does not prove a live feed.';
  const assessment=state.lastResult?.assessment, scope=assessment?.filter_scope;
  $('coverage').textContent=scope ? scope.eligible_count+'/'+scope.universe_count+' CEF symbols eligible under the last filter. This is not verified release coverage.' : 'CEF filter coverage awaits the first response.';
  if(assessment?.continuity?.full_page) $('coverage').textContent+=' Full result page; overflow remains possible.';
  $('session-note').textContent=valid ? (p.mode==='until_close'?'Daily session · stops at midnight Kyiv.':'Short check · original deadline retained.') : '';
  $('frames').textContent = String(state.frames || 0);
  $('headlines').textContent = String(state.lastResult?.articles ?? '—');
  $('filter').textContent = state.lastResult?.query || 'Appears after the first update';
  $('received').textContent = state.lastResult?.at?'Last update '+new Date(state.lastResult.at).toLocaleTimeString()+(state.dropped?' · '+state.dropped+' skipped while busy':''):active?'Awaiting first update':'Waiting for a connection';
  $('start').disabled = busy || !!active;
  $('start').textContent = busy?'Connecting…':resumable?'Resume capture ↗':'Connect & start ↗';
  $('stop').disabled = busy || !valid;
  $('code').disabled = busy || !!active;
}
async function reloadState() {state=await chrome.storage.session.get(['pairing','captureState','lastResult','frames','dropped','receiverStatus']);render();}
async function activeTab() {
  const [tab] = await chrome.tabs.query({active:true,currentWindow:true});
  if (!tab?.url?.startsWith('https://newsfilter.io/')) throw new Error('open_terminal');
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
        response=await fetch('http://127.0.0.1:8767/pair',{method:'POST',credentials:'omit',redirect:'error',cache:'no-store',
          headers:{'Content-Type':'application/json','X-CEF-Bridge':'1'},body:JSON.stringify({code,client_id:chrome.runtime.id}),
          signal:AbortSignal.timeout(5000)});
      } catch { throw new Error('local_receiver_unavailable'); }
      pairing=await response.json();
      if (!response.ok) throw new Error(pairing.error || 'invalid_connection');
    } else if (state.pairing && state.pairing.end>Date.now()) {
      pairing={url:'http://127.0.0.1:8767/newsfilter',token:state.pairing.token,expires_at:new Date(state.pairing.end).toISOString(),mode:state.pairing.mode||'short'};
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
chrome.storage.onChanged.addListener((changes,area) => {if(area==='session') void reloadState();});
void reloadState();
setInterval(render,1000);
