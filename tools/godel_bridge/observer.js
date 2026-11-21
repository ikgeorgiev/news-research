/* Passive only: never sends a Godel request or reads cookies/credentials. */
(() => {
  'use strict';
  if (window.CefGodelObserver?.version === 7) return;
  window.CefGodelObserver?.stop();
  const ENDPOINT = 'https://terminal-api.godelterminal.com/api/news/items';
  const LIMIT = 1024 * 1024;
  let active = false, timer, end = 0, events = [], dropped = 0, sink = null, generation = '';
  let originalFetch, wrappedFetch, originalOpen, originalSend, wrappedOpen, wrappedSend;
  const xhrMeta = new WeakMap();
  const iso = () => new Date().toISOString();
  function scope(body) {
    let d;
    try { d = typeof body === 'string' ? JSON.parse(body) : null; } catch { return null; }
    if (!d || !Array.isArray(d.sources) || !Array.isArray(d.languages)) return null;
    const list = a => a.length <= 1000 && a.every(x => typeof x === 'string' && x.length <= 300) ? a : null;
    if (!list(d.sources) || !list(d.languages)) return null;
    const selected = {sources:d.sources, languages:d.languages,
      pageable:d.pageable, dateTimeRange:d.dateTimeRange, legal:d.legal};
    // Broad native requests may omit symbols. Preserve omission; do not turn
    // it into an empty filter or claim all-fund eligibility from it.
    if (Object.hasOwn(d,'symbols')) {
      if (!Array.isArray(d.symbols) || !list(d.symbols)) return null;
      selected.symbols=d.symbols;
    }
    return selected;
  }
  function selectedRequest(body, kind) {
    const selected=scope(body);
    let data;
    try {data=typeof body==='string'?JSON.parse(body):null;} catch {}
    const fieldType=k=>!data||!Object.hasOwn(data,k)?'missing':Array.isArray(data[k])?'array':data[k]===null?'null':typeof data[k];
    return {scope:selected,request_scope_diagnostic:{body_kind:kind,status:selected?'selected':'unavailable',
      fields:Object.fromEntries(['symbols','sources','languages','pageable','dateTimeRange'].map(k=>[k,fieldType(k)]))}};
  }
  async function fetchScope(input,init) {
    if(init?.body!==undefined && init.body!==null) return selectedRequest(init.body,typeof init.body);
    if(typeof Request==='undefined'||!(input instanceof Request)) return selectedRequest(null,'missing');
    // Clone before the page's fetch consumes the Request. Read only the small
    // JSON filter body; headers, cookies and credentials are never inspected.
    let reader,timer;
    try {
      const clone=input.clone();
      if(!clone.body) return selectedRequest(null,'Request');
      reader=clone.body.getReader();
      const text=await Promise.race([(async()=>{
        let count=0;const chunks=[];
        while(true){const {done,value}=await reader.read();if(done)break;
          count+=value.byteLength;if(count>65536)throw new Error('request_body_bound');chunks.push(value);}
        const bytes=new Uint8Array(count);let offset=0;
        for(const part of chunks){bytes.set(part,offset);offset+=part.length;}
        return new TextDecoder().decode(bytes);
      })(),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('request_body_timeout')),2000);})]);
      return selectedRequest(text,'Request');
    } catch {
      if(reader)void reader.cancel().catch(()=>{});
      return {scope:null,request_scope_diagnostic:{body_kind:'Request',status:'unreadable'}};
    } finally {clearTimeout(timer);}
  }
  function matches(input, init) {
    try {
      const u = typeof input === 'string' || input instanceof URL ? String(input) : input.url;
      return new URL(u, location.href).href === ENDPOINT && String(init?.method || input?.method || 'GET').toUpperCase() === 'POST';
    } catch { return false; }
  }
  function emit(event) {
    if (!active || Date.now() >= end || event.capture_id !== generation) return;
    if (events.length >= 100) { dropped++; return; }
    if (sink) { try { sink(event); } catch { dropped++; } }
    else events.push(event);
  }
  function select(data) {
    if (!data || !Array.isArray(data.content) || data.content.length > 250) throw new Error('Unrecognized or oversized news envelope');
    return {content: data.content.map(x => {
      if (!x || typeof x.id !== 'string' || typeof x.title !== 'string') throw new Error('Invalid news row');
      const row = {};
      for (const k of ['id','title','providerName','publicationTime','receivedTime','originalUrl','sources','securityIds','feedId','language']) row[k] = x[k] ?? null;
      return row;
    }), envelope_keys: Object.keys(data), pageSize:data.pageSize ?? null,
      nextCursor:data.nextCursor ?? null, prevCursor:data.prevCursor ?? null, totalItems:data.totalItems ?? null};
  }
  async function copy(response, meta) {
    const event = {...meta, version: 1, endpoint: ENDPOINT, status: response.status,
      browser_headers_observed_at: iso(), receipt_kind: 'passive_browser_copy',
      headers: Object.fromEntries(['date','age','cache-control','retry-after'].map(k => [k,response.headers.get(k)]).filter(x => x[1] !== null))};
    if (response.status !== 200) { emit(event); if ([401,403,404,429].includes(response.status)) stop(); return; }
    let reader;
    try {
      reader = response.body.getReader();
      let count = 0, chunks = [];
      const timeout = setTimeout(() => { void reader.cancel(); }, 15000);
      try {
        while (true) {
          const {done,value} = await reader.read();
          if (done) break;
          count += value.byteLength;
          if (count > LIMIT) throw new Error('Response exceeds 1 MiB');
          chunks.push(value);
        }
      } finally { clearTimeout(timeout); }
      const bytes = new Uint8Array(count); let offset = 0;
      for (const c of chunks) { bytes.set(c,offset); offset += c.length; }
      event.payload = select(JSON.parse(new TextDecoder().decode(bytes)));
      event.native_response_bytes = count;
      event.browser_body_observed_at = iso();
    } catch (e) {
      if (reader) void reader.cancel().catch(() => {});
      event.error = String(e.message).slice(0,300);
    }
    emit(event);
  }
  function stop() {
    active = false; clearTimeout(timer);
    window.CefGodelStream?.stop();
    window.CefGodelWorker?.stop();
    if (window.fetch === wrappedFetch) window.fetch = originalFetch;
    if (XMLHttpRequest.prototype.open === wrappedOpen) XMLHttpRequest.prototype.open = originalOpen;
    if (XMLHttpRequest.prototype.send === wrappedSend) XMLHttpRequest.prototype.send = originalSend;
    return {active, buffered: events.length, dropped};
  }
  function start(options = {}) {
    stop(); events = []; dropped = 0;
    end = CefBridgeSession.observerEnd(options);
    sink = typeof options.onEvent === 'function' ? options.onEvent : null;
    active = true; generation = crypto.randomUUID();
    originalFetch = window.fetch;
    wrappedFetch = function(input, init) {
      const matched = matches(input,init);
      const meta = matched ? {capture_id:generation,browser_request_observed_at: iso(),transport: 'fetch'} : null;
      const selected=matched?fetchScope(input,init):null;
      const promise = originalFetch.apply(this,arguments);
      if (meta) void promise.then(r => {
        if(!active||meta.capture_id!==generation)return;
        // Clone immediately; the page remains free to consume its response.
        const response=r.clone();
        void selected.then(fields=>{if(active&&meta.capture_id===generation)void copy(response,{...meta,...fields}).catch(()=>{});});
      }, () => {}).catch(()=>{});
      return promise;
    };
    window.fetch = wrappedFetch;
    originalOpen = XMLHttpRequest.prototype.open;
    originalSend = XMLHttpRequest.prototype.send;
    wrappedOpen = function(method,url) {
      xhrMeta.set(this, matches(url,{method}) ? {} : null);
      return originalOpen.apply(this,arguments);
    };
    wrappedSend = function(body) {
      if (xhrMeta.get(this)) {
        const meta = {capture_id:generation,browser_request_observed_at: iso(),...selectedRequest(body,typeof body),transport: 'xhr'};
        this.addEventListener('load', () => {
          if (!active) return;
          const event = {...meta,version:1,endpoint:ENDPOINT,status:this.status,receipt_kind:'passive_browser_copy',headers:{}};
          try {
            if (this.status === 200) {
              const value = this.responseType === 'json' ? JSON.stringify(this.response) : this.responseText;
              if (new TextEncoder().encode(value).length > LIMIT) throw new Error('Response exceeds 1 MiB');
              event.payload = select(JSON.parse(value));
              event.browser_body_observed_at = iso();
            }
          } catch (e) { event.error = String(e.message).slice(0,300); }
          emit(event);
          if ([401,403,404,429].includes(this.status)) stop();
        }, {once:true});
      }
      return originalSend.apply(this,arguments);
    };
    XMLHttpRequest.prototype.open = wrappedOpen;
    XMLHttpRequest.prototype.send = wrappedSend;
    timer = setTimeout(stop, Math.max(1,end-Date.now()));
    window.CefGodelStream?.start({end,capture_id:generation,onEvent:emit});
    window.CefGodelWorker?.start({end,capture_id:generation,onEvent:emit});
    return {active:true,capture_id:generation,expires_at:new Date(end).toISOString(), endpoint:ENDPOINT, sends_source_requests:false};
  }
  window.CefGodelObserver = {version:7,start,stop,drain: () => events.splice(0), status: () => ({active,capture_id:generation,buffered:events.length,dropped,expires_at:new Date(end || 0).toISOString()})};
})();
