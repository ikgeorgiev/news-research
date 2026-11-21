/* Passive news-only observation. Never sends Newsfilter requests or reads auth. */
(() => {
  'use strict';
  if (window.CefNewsfilterObserver?.version === 5) return;
  window.CefNewsfilterObserver?.stop();
  const ENDPOINT = 'https://api.newsfilter.io/actions', LIMIT = 1024 * 1024;
  let active = false, prepared = false, epoch = 0, allowedAfter = 0, timer, end = 0, events = [], pending = [], dropped = 0, sink, generation;
  let originalFetch, wrappedFetch, originalOpen, originalSend, wrappedOpen, wrappedSend;
  const requests = new WeakMap(), iso = () => new Date().toISOString();
  function scope(body) {
    let d;
    try { d = typeof body === 'string' ? JSON.parse(body) : null; } catch { return null; }
    if (!d || d.type !== 'filterArticles' || typeof d.queryString !== 'string' || d.queryString.length > 8192) return null;
    if (!Number.isInteger(d.from) || d.from !== 0 || !Number.isInteger(d.size) || d.size < 1 || d.size > 100) return null;
    // Strict allowlist: never copy account, login, cookie or token fields.
    return {type:d.type,queryString:d.queryString,from:d.from,size:d.size};
  }
  function matches(input, init) {
    try {
      const u = typeof input === 'string' || input instanceof URL ? String(input) : input.url;
      return new URL(u, location.href).href === ENDPOINT && String(init?.method || input?.method || 'GET').toUpperCase() === 'POST';
    } catch { return false; }
  }
  function select(data) {
    if (!data || !Array.isArray(data.articles) || data.articles.length > 100) throw new Error('Unrecognized news envelope');
    return {articles:data.articles.map(x => {
      if (!x || typeof x.id !== 'string' || typeof x.title !== 'string') throw new Error('Invalid news row');
      const row = {};
      for (const k of ['id','title','sourceUrl','url','publishedAt','symbols']) row[k] = x[k] ?? null;
      row.source = x.source && {id:x.source.id,name:x.source.name};
      return row;
    }), total:data.total && {value:data.total.value,relation:data.total.relation},
      from:data.from, size:data.size};
  }
  function emit(event) {
    if (!active || Date.now() >= end || event.capture_id !== generation) return;
    if (events.length >= 100) { dropped++; return; }
    if (sink) { try { sink(event); } catch { dropped++; } } else events.push(event);
  }
  function exposedXhrHeaders(xhr) {
    const headers = {};
    // Chrome logs an error for getResponseHeader(name) when CORS hides that
    // name. Enumerate only the browser-exposed headers; never probe hidden ones.
    const visible = xhr.getAllResponseHeaders();
    if (typeof visible !== 'string') return headers;
    for (const line of visible.split(/\r?\n/)) {
      const match = /^(date|age|cache-control|retry-after):[\t ]*(.*)$/i.exec(line);
      if (match) headers[match[1].toLowerCase()] = match[2].trim();
    }
    return headers;
  }
  async function copy(response, meta) {
    const event = {...meta,version:1,endpoint:ENDPOINT,status:response.status,
      receipt_kind:'passive_browser_copy',browser_headers_observed_at:iso(),
      headers:Object.fromEntries(['date','age','cache-control','retry-after'].map(k=>[k,response.headers.get(k)]).filter(x=>x[1]!==null))};
    if (response.status !== 200) { emit(event); if(event.capture_id===generation) stop(); return; }
    let reader;
    try {
      reader = response.body.getReader();
      let count = 0, chunks = [], timedOut = false;
      const deadline = setTimeout(() => { timedOut = true; void reader.cancel(); },15000);
      try {
        while (true) {
          const {done,value} = await reader.read();
          if (timedOut) throw new Error('Response copy timed out');
          if (done) break;
          count += value.byteLength;
          if (count > LIMIT) throw new Error('Response exceeds 1 MiB');
          chunks.push(value);
        }
      } finally { clearTimeout(deadline); }
      const bytes = new Uint8Array(count); let offset = 0;
      for (const chunk of chunks) { bytes.set(chunk,offset); offset += chunk.length; }
      event.payload = select(JSON.parse(new TextDecoder().decode(bytes)));
      event.native_response_bytes = count; event.browser_body_observed_at = iso();
    } catch {
      if (reader) void reader.cancel().catch(()=>{});
      event.error = 'news_response_copy_or_schema_error';
    }
    emit(event); if (event.error && event.capture_id===generation) stop();
  }
  function stop() {
    active = false; prepared = false; pending = []; epoch++; clearTimeout(timer);
    if (window.fetch === wrappedFetch) window.fetch = originalFetch;
    if (XMLHttpRequest.prototype.open === wrappedOpen) XMLHttpRequest.prototype.open = originalOpen;
    if (XMLHttpRequest.prototype.send === wrappedSend) XMLHttpRequest.prototype.send = originalSend;
    return {active,buffered:events.length,dropped};
  }
  function start(options = {}) {
    if (!prepared) prepare();
    clearTimeout(timer); events = []; dropped = 0;
    end = CefBridgeSession.observerEnd(options);
    sink = typeof options.onEvent === 'function' ? options.onEvent : null;
    allowedAfter = Number.isFinite(options.startedAt) ? options.startedAt : Date.now();
    active = true; prepared = false;
    generation = options.captureId || crypto.randomUUID();
    timer=setTimeout(stop,Math.max(1,end-Date.now()));
    // XHR already retains its response for the page. Keep only bounded callbacks
    // during the handshake; read selected fields only after this active lease.
    const completed = pending; pending = [];
    for (const deliver of completed) deliver();
    return {active,capture_id:generation,expires_at:new Date(end).toISOString(),sends_source_requests:false};
  }
  function prepare() {
    stop(); prepared = true;
    const currentEpoch = epoch;
    const eligible = meta => active && meta.epoch === epoch && meta.started >= allowedAfter && Date.now() < end;
    const whenAuthorized = (meta, deliver) => {
      if (eligible(meta)) deliver();
      else if (prepared && meta.epoch === epoch && pending.length < 20) pending.push(deliver);
    };
    const selected = meta => ({capture_id:generation,scope:meta.scope,browser_request_observed_at:meta.browser_request_observed_at,transport:meta.transport});
    originalFetch = window.fetch;
    wrappedFetch = function(input,init) {
      const filters = matches(input,init) ? scope(init?.body) : null;
      const meta = filters ? {epoch:currentEpoch,started:Date.now(),scope:filters,browser_request_observed_at:iso(),transport:'fetch'} : null;
      const promise = originalFetch.apply(this,arguments);
      if (meta) void promise.then(r=>{if(eligible(meta)) void copy(r.clone(),selected(meta)).catch(()=>{});},()=>{
        if (!eligible(meta)) return;
        emit({...selected(meta),version:1,endpoint:ENDPOINT,receipt_kind:'passive_browser_copy',status:0,error:'native_network_error'}); stop();
      });
      return promise;
    };
    window.fetch = wrappedFetch;
    originalOpen = XMLHttpRequest.prototype.open; originalSend = XMLHttpRequest.prototype.send;
    wrappedOpen = function(method,url) { requests.set(this,{matches:matches(url,{method})}); return originalOpen.apply(this,arguments); };
    wrappedSend = function(body) {
      const request = requests.get(this);
      const filters = request?.matches ? scope(body) : null;
      if (filters) {
        const meta = {epoch:currentEpoch,started:Date.now(),scope:filters,browser_request_observed_at:iso(),transport:'xhr'};
        this.addEventListener('load',()=>{
          const completedAt = iso();
          whenAuthorized(meta,()=>{
          if (!eligible(meta) || requests.get(this) !== request) return;
          const event = {...selected(meta),version:1,endpoint:ENDPOINT,status:this.status,receipt_kind:'passive_browser_copy',browser_response_completed_at:completedAt,headers:{}};
          try {
            event.headers = exposedXhrHeaders(this);
            if (this.status === 200) {
              const value = this.responseType==='json' ? JSON.stringify(this.response) : this.responseText;
              if (new TextEncoder().encode(value).length > LIMIT) throw new Error('Response exceeds 1 MiB');
              event.payload = select(JSON.parse(value)); event.browser_body_observed_at = iso();
            }
          } catch { event.error = 'news_response_copy_or_schema_error'; }
          emit(event); if (this.status!==200 || event.error) stop();
          });
        },{once:true});
        this.addEventListener('error',()=>whenAuthorized(meta,()=>{
          if (!eligible(meta) || requests.get(this) !== request) return;
          emit({...selected(meta),version:1,endpoint:ENDPOINT,status:0,receipt_kind:'passive_browser_copy',error:'native_network_error'}); stop();
        }),{once:true});
      }
      return originalSend.apply(this,arguments);
    };
    XMLHttpRequest.prototype.open=wrappedOpen; XMLHttpRequest.prototype.send=wrappedSend;
    timer=setTimeout(stop,15000);
    return {active:false,prepared:true,sends_source_requests:false};
  }
  window.CefNewsfilterObserver={version:5,prepare,start,stop,drain:()=>events.splice(0),status:()=>({active,prepared,capture_id:generation,buffered:events.length,dropped,expires_at:new Date(end||0).toISOString()})};
})();
