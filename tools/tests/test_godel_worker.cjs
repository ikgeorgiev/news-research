const {test}=require('node:test');
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const {randomUUID}=require('node:crypto');
const script=fs.readFileSync(path.join(__dirname,'../godel_bridge/worker-observer.js'),'utf8');
const topic='/user/queue/filterResponse/1174';
function env(){const sent=[],batches=[];let made=0;
 class Native extends EventTarget{constructor(url){super();this.url=url;made++;}postMessage(x){sent.push(x);return 'native';}terminate(){return 'terminated';}}
 const c={Worker:Native,URL,TextEncoder,Date,crypto:{randomUUID},setTimeout,clearTimeout,location:{href:'https://app.godelterminal.com/'}};c.window=c;vm.createContext(c);vm.runInContext(script,c);
 const worker=new c.Worker('https://app.godelterminal.com/_next/static/chunks/STOMPWebWorker.76c8648538af0be4.js?dpl=existing');
 const subscribe=(id='first',symbols=['GOF'])=>worker.postMessage({type:'SUBSCRIBE',listenerId:id,topic,subscriptionOptions:{filter:JSON.stringify({sources:['wire'],languages:['en'],symbols,private:'NEVER-STORE'})}});
 const receive=(data)=>worker.dispatchEvent(new MessageEvent('message',{data}));
 const article=id=>({id,title:'Fund distribution',publicationTime:String(Date.now()),securityIds:['GOF'],jwt:'NEVER-STORE'});
 const message=(id='one',extra={})=>({type:'EMIT:BUFFERED',topic,isInitial:false,data:[article(id)],...extra});
 const start=(ms=30000)=>c.CefGodelWorker.start({end:Date.now()+ms,capture_id:'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',onEvent:x=>batches.push(x)});
 return{c,worker,sent,batches,subscribe,receive,article,message,start,made:()=>made};
}
test('worker observer copies native news only and sends no messages itself',()=>{
 const e=env();e.subscribe();let reads=0;const event=new Event('message');Object.defineProperty(event,'data',{get(){reads++;throw Error('dormant');}});e.worker.dispatchEvent(event);assert.equal(reads,0);
 const init={type:'INIT'};Object.defineProperty(init,'jwt',{get(){throw Error('auth read');}});e.worker.postMessage(init);
 e.start();e.receive(e.message());e.c.CefGodelWorker.flush();e.c.CefGodelWorker.stop();
 assert.equal(e.made(),1);assert.equal(e.sent.length,2);assert.equal(e.batches[0].version,3);assert.equal(e.batches[0].records[0].scope.symbols[0],'GOF');assert.equal(JSON.stringify(e.batches).includes('NEVER-STORE'),false);
});
test('debug, account, unobserved and unknown topic payloads are never read',()=>{
 const e=env();e.subscribe();e.start();for(const m of [{type:'DEBUG:METRICS:RESPONSE',topic},{type:'EMIT:BUFFERED',topic:'/account/private'},{type:'EMIT:BUFFERED',topic:'/user/queue/filterResponse/999'}]){Object.defineProperty(m,'data',{get(){throw Error('private read');}});e.receive(m);}
 e.c.CefGodelWorker.flush();e.c.CefGodelWorker.stop();assert.equal(e.batches.length,0);
});
test('all native listeners share the original topic filters until final unsubscribe',()=>{
 const e=env();e.subscribe();e.subscribe('second',['PDI']);e.start();e.worker.postMessage({type:'UNSUBSCRIBE',topic,listenerId:'first'});e.receive(e.message());e.worker.postMessage({type:'UNSUBSCRIBE',topic,listenerId:'second'});e.receive(e.message('ignored'));e.c.CefGodelWorker.flush();e.c.CefGodelWorker.stop();assert.equal(e.batches[0].records.length,1);assert.equal(e.batches[0].records[0].scope.symbols[0],'GOF');
});
test('initial and conflated emissions retain limitations and Stop discards pending work',()=>{
 const e=env();e.subscribe();e.start();e.receive(e.message('initial',{isInitial:true}));e.receive(e.message('last',{type:'EMIT:CONFLATED',data:e.article('last')}));e.c.CefGodelWorker.flush();assert.equal(e.batches[0].records[0].worker_initial,true);assert.equal(e.batches[0].records[1].worker_delivery_mode,'EMIT:CONFLATED');e.receive(e.message('discard'));e.c.CefGodelWorker.stop();e.c.CefGodelWorker.flush();assert.equal(e.batches.length,1);
});
test('overflow is counted and expiration removes body listeners',async()=>{
 const e=env();e.subscribe();e.start();for(let i=0;i<120;i++)e.receive(e.message(String(i)));e.c.CefGodelWorker.flush();assert.equal(e.batches[0].records.length,100);assert.equal(e.batches[0].dropped_frames,20);e.c.CefGodelWorker.stop();e.start(5);await new Promise(r=>setTimeout(r,20));assert.equal(e.c.CefGodelWorker.status().active,false);
});

test('native message drains overdue batch even when the page timer has not run',()=>{
 const e=env();e.subscribe();e.start();let now=Date.now()+6000;
 e.c.Date=class extends Date {constructor(...a){super(...(a.length?a:[now]));}static now(){return now;}};
 e.receive(e.message('first'));assert.equal(e.batches.length,1);
 e.receive(e.message('buffered'));assert.equal(e.batches.length,1);
 now+=6000;e.receive(e.message('next'));assert.equal(e.batches.length,2);
 assert.deepEqual(Array.from(e.batches[1].records,r=>r.article.id),['buffered','next']);
 assert.equal(e.sent.length,1);assert.equal(e.made(),1);e.c.CefGodelWorker.stop();
});
