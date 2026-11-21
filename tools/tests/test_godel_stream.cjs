const {test}=require('node:test');
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const {randomUUID}=require('node:crypto');
const script=fs.readFileSync(path.join(__dirname,'../godel_bridge/stream-observer.js'),'utf8');
const endpoint='wss://terminal-api.godelterminal.com/events?jwt=DO-NOT-RETAIN';
const dest='/user/queue/filterResponse/1174',scope={sources:['wire'],languages:['en'],symbols:['GOF']};
const escape=s=>s.replace(/\\/g,'\\\\').replace(/:/g,'\\c').replace(/\n/g,'\\n');
function frame(command,headers,body=''){return command+'\n'+Object.entries(headers).map(([k,v])=>k+':'+escape(String(v))).join('\n')+'\n\n'+body+'\0';}
function env(){const sent=[],batches=[];let created=0;
  class Socket extends EventTarget {constructor(url){super();this.url=url;created++;}send(x){sent.push(x);return 'unchanged';}}
  const c={WebSocket:Socket,URL,TextEncoder,TextDecoder,ArrayBuffer,Blob,Date,crypto:{randomUUID},setTimeout,clearTimeout};c.window=c;
  vm.createContext(c);vm.runInContext(script,c);
  const socket=new c.WebSocket(endpoint);
  const subscribe=(symbols=['GOF'])=>socket.send(frame('SUBSCRIBE',{id:'sub-1',destination:dest,filter:JSON.stringify({...scope,symbols,privateAccount:'DO-NOT-RETAIN'}),authorization:'DO-NOT-RETAIN'}));
  const article=id=>({id,title:'Example café distribution',publicationTime:String(Date.now()),receivedTime:new Date().toISOString(),securityIds:['GOF'],originalUrl:null,privateBody:'DO-NOT-RETAIN'});
  const message=(id='one',headers={})=>{const body=JSON.stringify(article(id));return frame('MESSAGE',{destination:dest,subscription:'sub-1','message-id':'native-'+id,'content-length':Buffer.byteLength(body),...headers},body);};
  const receive=data=>socket.dispatchEvent(new MessageEvent('message',{data}));
  const start=(ms=30000)=>c.CefGodelStream.start({end:Date.now()+ms,capture_id:'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',onEvent:x=>batches.push(x)});
  const settle=async()=>{await new Promise(r=>setTimeout(r,15));c.CefGodelStream.flush();};
  return {c,socket,subscribe,message,receive,start,settle,batches,sent,created:()=>created};
}
test('dormant installation reads no incoming bodies and creates/sends nothing itself',async()=>{
 const e=env();e.subscribe();let read=0;const event=new Event('message');Object.defineProperty(event,'data',{get(){read++;throw Error('private');}});e.socket.dispatchEvent(event);
 assert.equal(read,0);assert.equal(e.created(),1);assert.equal(e.sent.length,1);assert.equal(e.batches.length,0);
 e.start();e.receive(e.message());await e.settle();e.c.CefGodelStream.stop();
 assert.equal(e.batches[0].records[0].scope.symbols[0],'GOF');assert.equal(e.sent.length,1);
 assert.equal(JSON.stringify(e.batches).includes('DO-NOT-RETAIN'),false);assert.equal(e.batches[0].endpoint.includes('?'),false);
});
test('account notifications and unmatched subscriptions never leave the page',async()=>{
 const e=env();e.subscribe();e.start();e.receive(e.message('private',{destination:'/user/queue/notifications'}));e.receive(e.message('unknown',{subscription:'sub-9'}));await e.settle();e.c.CefGodelStream.stop();assert.equal(e.batches.length,0);
});
test('fragmented and multiple native frames keep separate identities and message times',async()=>{
 const e=env();e.subscribe();e.start();const first=e.message('one');e.receive(first.slice(0,55));e.receive(first.slice(55)+e.message('two'));await e.settle();e.c.CefGodelStream.stop();
 assert.deepEqual(Array.from(e.batches[0].records,x=>x.article.id),['one','two']);assert.ok(e.batches[0].records.every(x=>x.browser_message_observed_at&&x.browser_body_observed_at));
});
test('native filter changes preserve each earlier message scope',async()=>{
 const e=env();e.subscribe();e.start();e.receive(e.message('one'));await new Promise(r=>setTimeout(r,10));e.subscribe(['PDI']);e.receive(e.message('two'));await e.settle();e.c.CefGodelStream.stop();
 assert.deepEqual(Array.from(e.batches[0].records,x=>x.scope.symbols[0]),['GOF','PDI']);
});
test('Stop drops buffered work and late decoding cannot cross capture generations',async()=>{
 const e=env();e.subscribe();e.start();e.receive(new Blob([e.message()]));e.c.CefGodelStream.stop();await e.settle();assert.equal(e.batches.length,0);
 e.start();await e.settle();e.c.CefGodelStream.stop();assert.equal(e.batches.length,0);
});
test('bounded burst records overflow explicitly without sending more source requests',async()=>{
 const e=env();e.subscribe();e.start();for(let i=0;i<130;i++)e.receive(e.message(String(i)));await e.settle();e.c.CefGodelStream.stop();
 assert.equal(e.batches[0].records.length,100);assert.equal(e.batches[0].dropped_frames,30);assert.equal(e.sent.length,1);
});
test('malformed native byte length is not a successful article',async()=>{
 const e=env();e.subscribe();e.start();e.receive(e.message('one',{'content-length':1}));await e.settle();e.c.CefGodelStream.stop();assert.equal(e.batches[0].records.length,0);assert.equal(e.batches[0].dropped_frames,1);
});
test('expiry stops body capture without closing the terminal socket',async()=>{
 const e=env();e.subscribe();e.start(5);await new Promise(r=>setTimeout(r,20));e.receive(e.message());await e.settle();assert.equal(e.batches.length,0);assert.equal(e.c.CefGodelStream.status().active,false);assert.equal(e.sent.length,1);
});
test('unsubscribe removes eligibility and unrelated socket receives no listener',async()=>{
 const e=env();e.subscribe();e.start();e.socket.send(frame('UNSUBSCRIBE',{id:'sub-1'}));e.receive(e.message());
 const other=new e.c.WebSocket('wss://other.invalid/events');let reads=0;const ev=new Event('message');Object.defineProperty(ev,'data',{get(){reads++;}});other.dispatchEvent(ev);await e.settle();e.c.CefGodelStream.stop();assert.equal(e.batches.length,0);assert.equal(reads,0);
});
