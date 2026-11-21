/* Observe the terminal's own STOMP worker messages. Never create a worker,
 * subscribe, reconnect, or read INIT/authentication/debug message contents. */
(() => {
  'use strict';
  if (window.CefGodelWorker?.version === 1 || typeof Worker === 'undefined') return;
  const NativeWorker=window.Worker, workers=new Map(), LIMIT=1024*1024;
  const origin='https://app.godelterminal.com';
  let active=false,end=0,capture='',sink,timer,records=[],bytes=0,dropped=0,lastSent=0;
  const iso=()=>new Date().toISOString(), size=s=>new TextEncoder().encode(s).length;
  const news=s=>s==='/topic/news/godel-headlines'||/^\/user\/queue\/filterResponse\/\d{1,12}$/.test(s||'');
  function scope(message) {
    if(message.topic==='/topic/news/godel-headlines')return {sources:['Godel Headlines'],languages:[]};
    let value;try{value=JSON.parse(message.subscriptionOptions?.filter);}catch{return null;}
    if(!value||typeof value!=='object')return null;
    const selected={};
    for(const key of ['sources','languages','symbols']) {
      if(key==='symbols'&&!Object.hasOwn(value,key))continue;
      const a=value[key];if(!Array.isArray(a)||a.length>1000||a.some(x=>typeof x!=='string'||x.length>300))return null;
      selected[key]=a.slice();
    }
    return selected;
  }
  function outbound(info,message) {
    if(!message||!['SUBSCRIBE','UNSUBSCRIBE'].includes(message.type)||!news(message.topic))return;
    const id=message.listenerId;
    if(typeof id!=='string'||id.length>200)return;
    const existing=info.topics.get(message.topic);
    if(message.type==='UNSUBSCRIBE') {
      if(existing){existing.listeners.delete(id);if(!existing.listeners.size)info.topics.delete(message.topic);}
      return;
    }
    // The native worker retains the first filter for an already-open topic.
    if(existing){if(existing.listeners.size<100)existing.listeners.add(id);return;}
    const selected=scope(message);
    if(!selected||info.topics.size>=32)return;
    info.topics.set(message.topic,{scope:selected,at:iso(),listeners:new Set([id])});
  }
  function selectedArticle(value) {
    if(!value||typeof value.id!=='string'||typeof value.title!=='string')throw Error('news_row');
    const selected={};
    for(const key of ['id','title','providerName','publicationTime','receivedTime','originalUrl','sources','securityIds','feedId','language'])selected[key]=value[key]??null;
    return selected;
  }
  function listen(worker,info) {
    if(info.listener)return;
    info.listener=event=>{
      if(!active||Date.now()>=end)return;
      const message=event.data;
      // Check type/topic before accessing any data/debug/account payload.
      if(!message||!['EMIT:BUFFERED','EMIT:CONFLATED'].includes(message.type)||!news(message.topic))return;
      const sub=info.topics.get(message.topic);if(!sub)return;
      if(typeof message.isInitial!=='boolean'){dropped++;return;}
      const rows=message.type==='EMIT:BUFFERED'?message.data:[message.data];
      if(!Array.isArray(rows)||rows.length>100){dropped++;return;}
      const observed=iso();
      for(const value of rows) {
        try {
          const article=selectedArticle(value), n=size(JSON.stringify(article));
          if(n>LIMIT)throw Error('news_bound');
          const record={worker_id:info.id,subscription:message.topic,destination:message.topic,scope:sub.scope,
            subscription_observed_at:sub.at,browser_message_observed_at:observed,browser_body_observed_at:iso(),
            selected_article_bytes:n,worker_initial:message.isInitial,worker_delivery_mode:message.type,article};
          const length=size(JSON.stringify(record));
          if(records.length>=100||bytes+length>700*1024){dropped++;continue;}
          records.push(record);bytes+=length;
        } catch {dropped++;}
      }
      // Native messages still arrive when Chrome throttles page timers. Drain
      // from that event too, with the same five-second minimum between batches.
      if(Date.now()-lastSent>=5000)flush();
    };
    worker.addEventListener('message',info.listener);
  }
  function register(worker,url) {
    let u;try{u=new URL(String(url),location.href);}catch{return;}
    if(u.origin!==origin||!/^\/_next\/static\/chunks\/STOMPWebWorker\.[a-f0-9]+\.js$/.test(u.pathname)||workers.size>=4)return;
    const info={id:crypto.randomUUID(),topics:new Map(),listener:null};workers.set(worker,info);
    const send=worker.postMessage, terminate=worker.terminate;
    worker.postMessage=function(message){const result=send.apply(this,arguments);try{outbound(info,message);}catch{}return result;};
    worker.terminate=function(){const result=terminate.apply(this,arguments);if(info.listener)worker.removeEventListener('message',info.listener);workers.delete(worker);if(active)dropped++;return result;};
    if(active)listen(worker,info);
  }
  window.Worker=new Proxy(NativeWorker,{construct(target,args,newTarget){
    const worker=Reflect.construct(target,args,newTarget);try{register(worker,args[0]);}catch{}return worker;
  }});
  function flush() {
    clearTimeout(timer);if(!active||Date.now()>=end){stop();return;}
    if(records.length||dropped){lastSent=Date.now();const batch=records;records=[];bytes=0;
      try{sink({version:3,endpoint:origin+'/',receipt_kind:'passive_browser_worker_batch',capture_id:capture,
        browser_body_observed_at:iso(),records:batch,dropped_frames:dropped});}catch{dropped+=batch.length;}}
    timer=setTimeout(flush,Math.min(5000,Math.max(1,end-Date.now())));
  }
  function stop(){active=false;clearTimeout(timer);records=[];bytes=0;for(const [worker,info]of workers){if(info.listener)worker.removeEventListener('message',info.listener);info.listener=null;}}
  function start(options){stop();end=options.end;capture=options.capture_id;sink=options.onEvent;dropped=0;active=true;lastSent=Date.now();
    for(const [worker,info]of workers)listen(worker,info);timer=setTimeout(flush,Math.min(5000,Math.max(1,end-Date.now())));
    return {tracked_workers:workers.size,sends_source_requests:false};}
  window.CefGodelWorker={version:1,start,stop,flush,status:()=>({active,tracked_workers:workers.size,news_topics:Array.from(workers.values()).reduce((n,x)=>n+x.topics.size,0),buffered:records.length,dropped})};
})();
