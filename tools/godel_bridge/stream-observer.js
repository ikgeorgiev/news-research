/* Observe the terminal's existing STOMP news stream. Never connect, subscribe,
 * acknowledge, retry, or read authentication. Before Start, retain only bounded
 * news-subscription filters in memory; no incoming body listener is installed. */
(() => {
  'use strict';
  if (window.CefGodelStream?.version === 1 || typeof WebSocket === 'undefined') return;
  const ENDPOINT='wss://terminal-api.godelterminal.com/events', LIMIT=1024*1024;
  const NativeSocket=window.WebSocket, sockets=new Map();
  let active=false,end=0,generation='',sink,timer,pending=[],bytes=0,dropped=0;
  const iso=()=>new Date().toISOString(), size=s=>new TextEncoder().encode(s).length;
  const newsDestination=s=>s==='/topic/news/godel-headlines'||/^\/user\/queue\/filterResponse\/\d{1,12}$/.test(s||'');
  const decode=s=>s.replace(/\\(r|n|c|\\)/g,(_,c)=>({r:'\r',n:'\n',c:':','\\':'\\'}[c]));
  function frame(value) {
    if(typeof value!=='string'||size(value)>LIMIT) return null;
    const split=value.indexOf('\n\n')>=0?'\n\n':'\r\n\r\n', at=value.indexOf(split);
    if(at<0||at>65536) return null;
    const lines=value.slice(0,at).split(/\r?\n/), command=lines.shift(), headers={};
    for(const line of lines) {
      const colon=line.indexOf(':');if(colon<1)continue;
      const key=decode(line.slice(0,colon));
      // No auth, CONNECT headers, arbitrary account metadata, or full header map.
      if(['id','destination','filter','subscription','message-id','content-length','content-type'].includes(key)&&!Object.hasOwn(headers,key)) headers[key]=decode(line.slice(colon+1));
    }
    const body=value.slice(at+split.length).replace(/\0$/,'');
    if(headers['content-length']!==undefined && (!/^\d+$/.test(headers['content-length'])||Number(headers['content-length'])!==size(body))) return null;
    return {command,headers,body};
  }
  function selectedScope(text,destination) {
    if(destination==='/topic/news/godel-headlines') return {sources:['Godel Headlines'],languages:[]};
    let input;try {input=JSON.parse(text);} catch {return null;}
    if(!input||typeof input!=='object')return null;
    const out={};
    for(const key of ['sources','languages','symbols']) {
      if(key==='symbols'&&!Object.hasOwn(input,key))continue;
      const a=input[key];if(!Array.isArray(a)||a.length>1000||a.some(x=>typeof x!=='string'||x.length>300))return null;
      out[key]=a.slice();
    }
    return out;
  }
  function outbound(info,data) {
    // The site's send is already complete. Ignore every command except native
    // news SUBSCRIBE/UNSUBSCRIBE; never inspect CONNECT or SEND credentials.
    if(typeof data!=='string'||! /^(SUBSCRIBE|UNSUBSCRIBE)\r?\n/.test(data)||size(data)>65536)return;
    const f=frame(data);if(!f)return;
    const id=f.headers.id;if(typeof id!=='string'||id.length>100)return;
    if(f.command==='UNSUBSCRIBE'){info.subscriptions.delete(id);return;}
    if(!newsDestination(f.headers.destination))return;
    const scope=selectedScope(f.headers.filter,f.headers.destination);
    if(!scope||info.subscriptions.size>=32&&!info.subscriptions.has(id))return;
    info.subscriptions.set(id,{scope,destination:f.headers.destination,observed_at:iso()});
  }
  function selectedArticle(input) {
    if(!input||typeof input.id!=='string'||typeof input.title!=='string')throw new Error('news_row');
    const out={};
    for(const key of ['id','title','providerName','publicationTime','receivedTime','originalUrl','sources','securityIds','feedId','language'])out[key]=input[key]??null;
    return out;
  }
  function enqueue(record) {
    const n=size(JSON.stringify(record));
    if(pending.length>=100||bytes+n>700*1024){dropped++;return;}
    pending.push(record);bytes+=n;
  }
  function receiveText(info,text,observed,capture) {
    if(!active||capture!==generation||Date.now()>=end)return;
    info.buffer+=text;
    if(size(info.buffer)>LIMIT){info.buffer='';dropped++;return;}
    let at;
    while((at=info.buffer.indexOf('\0'))>=0) {
      const raw=info.buffer.slice(0,at+1).replace(/^\r?\n+/,'');info.buffer=info.buffer.slice(at+1);
      if(!raw.startsWith('MESSAGE\n')&&!raw.startsWith('MESSAGE\r\n'))continue;
      const f=frame(raw);if(!f){dropped++;continue;}
      const sub=info.subscriptions.get(f.headers.subscription);
      if(!sub||!newsDestination(f.headers.destination)||f.headers.destination!==sub.destination)continue;
      try {
        enqueue({socket_id:info.id,subscription:f.headers.subscription,destination:sub.destination,
          message_id:(f.headers['message-id']||'').slice(0,500),scope:sub.scope,
          subscription_observed_at:sub.observed_at,browser_message_observed_at:observed,
          browser_body_observed_at:iso(),native_frame_bytes:size(raw),article:selectedArticle(JSON.parse(f.body))});
      } catch {dropped++;}
    }
    if(/^\s*$/.test(info.buffer))info.buffer='';
  }
  function listen(socket,info) {
    if(info.listener)return;
    info.buffer='';info.chain=Promise.resolve();
    info.listener=event=>{
      if(!active||Date.now()>=end)return;
      const observed=iso(),capture=generation,data=event.data;
      // Bound queued decoding as well as retained records during bursts.
      if(info.queued>=100){dropped++;return;}
      info.queued++;
      info.chain=info.chain.then(async()=>{
        if(!active||capture!==generation)return;
        let value;
        if(typeof data==='string')value=data;
        else if(typeof Blob!=='undefined'&&data instanceof Blob){if(data.size>LIMIT){dropped++;return;}value=await data.text();}
        else if(data instanceof ArrayBuffer){if(data.byteLength>LIMIT){dropped++;return;}value=new TextDecoder().decode(data);}
        else {dropped++;return;}
        receiveText(info,value,observed,capture);
      }).catch(()=>{dropped++;}).finally(()=>{info.queued--;});
    };
    socket.addEventListener('message',info.listener);
  }
  function register(socket,url) {
    let parsed;try {parsed=new URL(String(url));}catch{return;}
    if(parsed.origin!=='wss://terminal-api.godelterminal.com'||parsed.pathname!=='/events'||sockets.size>=4)return;
    const info={id:crypto.randomUUID(),subscriptions:new Map(),queued:0,buffer:'',listener:null};
    sockets.set(socket,info);
    const nativeSend=socket.send;
    socket.send=function(data){const result=nativeSend.apply(this,arguments);try{outbound(info,data);}catch{}return result;};
    socket.addEventListener('close',()=>{
      if(info.listener)socket.removeEventListener('message',info.listener);
      sockets.delete(socket);
      if(active)dropped++; // A reconnect starts a new socket identity, never borrowed continuity.
    },{once:true});
    if(active)listen(socket,info);
  }
  const WrappedSocket=new Proxy(NativeSocket,{construct(target,args,newTarget){
    const socket=Reflect.construct(target,args,newTarget);try{register(socket,args[0]);}catch{}return socket;
  }});
  window.WebSocket=WrappedSocket;
  function flush() {
    clearTimeout(timer);
    if(!active||Date.now()>=end){stop();return;}
    if(pending.length||dropped) {
      const records=pending;pending=[];bytes=0;
      try {sink({version:2,endpoint:ENDPOINT,receipt_kind:'passive_browser_stream_batch',capture_id:generation,
        browser_body_observed_at:iso(),records,dropped_frames:dropped});}catch{dropped+=records.length;}
    }
    timer=setTimeout(flush,Math.min(5000,Math.max(1,end-Date.now())));
  }
  function stop() {
    active=false;clearTimeout(timer);pending=[];bytes=0;
    for(const [socket,info]of sockets){if(info.listener)socket.removeEventListener('message',info.listener);info.listener=null;info.buffer='';}
  }
  function start(options) {
    stop();end=options.end;generation=options.capture_id;sink=options.onEvent;dropped=0;active=true;
    for(const [socket,info]of sockets)listen(socket,info);
    timer=setTimeout(flush,Math.min(5000,Math.max(1,end-Date.now())));
    return {tracked_sockets:sockets.size,sends_source_requests:false};
  }
  window.CefGodelStream={version:1,start,stop,flush,status:()=>({active,tracked_sockets:sockets.size,buffered:pending.length,dropped})};
})();
