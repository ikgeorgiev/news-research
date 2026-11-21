// Only selected news fields cross this boundary. The pairing token never does.
if (!globalThis.__cefGodelRelayInstalled) {
globalThis.__cefGodelRelayInstalled = true;
window.addEventListener('message', event => {
  if (event.source !== window || event.origin !== 'https://app.godelterminal.com' || event.data?.type !== 'CEF_GODEL_NATIVE_COPY') return;
  const payload = event.data.payload;
  try {
    if (JSON.stringify(payload).length > 1024 * 1024) return;
    chrome.runtime.sendMessage({type:'capture',payload}).catch(() => {});
  } catch { /* An invalid page message is not a capture. */ }
});
}
