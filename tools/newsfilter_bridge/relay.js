// Only selected news fields cross this boundary. The pairing token never does.
if (globalThis.__cefNewsfilterRelayInstalled !== 2) {
globalThis.__cefNewsfilterRelayInstalled = 2;
window.addEventListener('message', event => {
  if (event.source !== window || event.origin !== 'https://newsfilter.io' || event.data?.type !== 'CEF_NEWSFILTER_NATIVE_COPY') return;
  const payload = event.data.payload;
  try {
    if (JSON.stringify(payload).length > 1024 * 1024) return;
    chrome.runtime.sendMessage({type:'capture',payload}).catch(() => {});
  } catch { /* An invalid page message is not a capture. */ }
});
// This handshake carries no pairing token or page data. The background checks
// the exact tab/document and original deadline before enabling body copies.
try { chrome.runtime.sendMessage({type:'document_ready'}).catch(()=>{}); } catch {}
}
