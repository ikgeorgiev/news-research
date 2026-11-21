/* Shared by both unpacked bridges. No network or browser actions. */
(() => {
  'use strict';
  const DAYS = new Set(['2026-11-09','2026-11-10','2026-11-11','2026-11-12','2026-11-13']);
  function end(lease, now = Date.now()) {
    const expires = Date.parse(lease?.expires_at);
    if (!Number.isFinite(expires) || expires <= now) throw new Error('expired_connection');
    if (lease.mode === 'until_close') {
      const date = new Date(now), day = date.toISOString().slice(0,10);
      const close = Date.parse(day+'T22:00:00Z');
      if (!DAYS.has(day) || date.getUTCHours()<8 || date.getUTCHours()>=22 || expires>close)
        throw new Error('outside_weekday_window');
      return expires;
    }
    if (lease.mode && lease.mode !== 'short') throw new Error('invalid_connection');
    return Math.min(expires,now+20*60*1000);
  }
  function observerEnd(options, now = Date.now()) {
    if (options.mode === 'until_close') return end(options,now);
    return Math.min(options.expires_at ? end(options,now) : Infinity,
      now+Math.min(1200,Math.max(1,Number(options.seconds)||600))*1000);
  }
  function freshness(state, now = Date.now()) {
    const p=state.pairing;
    if (!p?.active || p.end<=now) return 'inactive';
    const last=Date.parse(state.lastResult?.at || '');
    if (Number.isFinite(last)) return now-last>180000?'stale':'recent_copy';
    return now-(p.startedAt || now)>180000?'no_updates':'waiting';
  }
  globalThis.CefBridgeSession={end,observerEnd,freshness};
})();
