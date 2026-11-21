/* Real unpacked extensions + real Python receivers. All source pages and API
 * responses are local synthetic fixtures; no account or source traffic. */
const assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),net=require('node:net');
const {spawn,spawnSync}=require('node:child_process');
const readline=require('node:readline');
const {createHash}=require('node:crypto');
const {chromium}=require('playwright');
const SCENARIO=process.argv[2]||'navigation';
const SCENARIOS=['navigation','delayed-navigation','left-news-page','tab-closed','refusal','expiry','receiver-down','godel','godel-request','godel-stream','godel-worker','godel-refusal','godel-rejection','godel-delay','cors-headers','cors-hidden-retry','cors-exposed-retry','godel-day','newsfilter-day','godel-reliability'];
if(!SCENARIOS.includes(SCENARIO))throw new Error('Unknown test scenario: '+SCENARIO);
const GODEL=SCENARIO.startsWith('godel');
const RELIABILITY=SCENARIO==='godel-reliability';
const WORKER=['godel-worker','godel-rejection','godel-delay'].includes(SCENARIO)||RELIABILITY;
const DAY=SCENARIO.endsWith('-day')||RELIABILITY;
const PORT=GODEL?8766:8767;
const ORIGIN=GODEL?'https://app.godelterminal.com':'https://newsfilter.io';
const API=GODEL?'https://terminal-api.godelterminal.com/api/news/items':'https://api.newsfilter.io/actions';
const ROOT=path.resolve(__dirname,'../..');
const PY=process.env.CEF_TEST_PYTHON||path.join(ROOT,'backend/.venv',process.platform==='win32'?'Scripts/python.exe':'bin/python');
const universeResult=spawnSync(PY,['-c',"import sys,json;sys.path.insert(0,'tools');import bridge_coverage;print(json.dumps(bridge_coverage.universe()['symbols']))"],{cwd:ROOT,encoding:'utf8',windowsHide:true,timeout:5000});
if(universeResult.status!==0)throw new Error('Cannot read current CEF universe');
const CEF_SYMBOLS=JSON.parse(universeResult.stdout);
const CEF_QUERY='symbols:('+CEF_SYMBOLS.map(s=>'"'+s+'"').join(' OR ')+')';
const EXTENSION=path.join(ROOT,'tools',GODEL?'godel_bridge':'newsfilter_bridge');
const RUN=path.join(ROOT,'.tmp','bridge-browser-tests',new Date().toISOString().replace(/[:.]/g,'-')+'-'+SCENARIO);
const STORE=path.join(RUN,'comparison');
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const report={scenario:SCENARIO,started_at:new Date().toISOString(),fixture_only:true,external_source_requests:0,checks:[],fixture_requests:[],blocked_requests:[],errors:[]};
let context,receiver,ready,popup,news,worker,extensionId,shortLeaseEnd,streamRoute,proxy;
const faults={status:0,capture:0,statusRequests:[],captureRequests:[]};
const browserErrors=[];
const unsafeHeaderWarnings=[];
function snapshot() {
  const code=`import sqlite3,json,sys\nfrom pathlib import Path\nc=sqlite3.connect(Path(sys.argv[1]).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)\nc.execute('pragma query_only=on')\nprint(json.dumps({'responses':[dict(zip(['id','status','meta','error'],x)) for x in c.execute('select id,status,meta,error from responses')],'observations':c.execute('select count(*) from observations').fetchone()[0],'classifications':dict(c.execute('select classification,count(*) from observations group by classification')),'controls':dict(c.execute('select key,value from control'))}))`;
  const result=spawnSync(PY,['-c',code,path.join(STORE,'live.sqlite')],{encoding:'utf8',windowsHide:true,timeout:10000});
  if(result.status!==0)throw new Error(result.stderr);
  return JSON.parse(result.stdout);
}
async function portOpen(port=PORT) {
  return new Promise(resolve=>{const s=net.connect(port,'127.0.0.1');s.setTimeout(500);
    const done=value=>{s.destroy();resolve(value);};s.once('connect',()=>done(true));s.once('error',()=>done(false));s.once('timeout',()=>done(true));});
}
async function startReceiver() {
  if(await portOpen())throw new Error('Port '+PORT+' is already occupied; existing worker left untouched.');
  if(RELIABILITY&&await portOpen(18766))throw new Error('Test upstream port occupied');
  receiver=spawn(PY,[GODEL?'tools/news_live.py':'tools/newsfilter_live.py','--store',STORE,GODEL?'godel-serve':'serve',...(DAY?['--until-close']:['--minutes','2','--compatibility-check']),...(RELIABILITY?['--port','18766']:[])],
    {cwd:ROOT,windowsHide:true,stdio:['ignore','pipe','pipe']});
  receiver.stderr.on('data',data=>fs.appendFileSync(path.join(RUN,'receiver-stderr.log'),data));
  const lines=readline.createInterface({input:receiver.stdout});
  ready=await new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>reject(new Error('Receiver readiness timeout')),10000);
    lines.on('line',line=>{const data=JSON.parse(line);if(data.status==='ready'){clearTimeout(timer);resolve(data);}
      if(data.status==='stopped')report.receiver_stopped_at=data.stopped_at;});
    receiver.once('exit',code=>{clearTimeout(timer);reject(new Error('Receiver exited before ready: '+code));});
  });
  report.receiver={wrapper_pid:receiver.pid,actual_pid:ready.pid,expires_at:ready.expires_at,store:STORE};
  if(RELIABILITY) {
    proxy=await require('./godel-reliability.cjs').proxy(faults);
    receiver.once('exit',()=>proxy.close());
  }
}
async function check(name,fn) {
  const start=Date.now();await fn();report.checks.push({name,status:'passed',duration_ms:Date.now()-start});console.log('PASS '+name);
}
function pageHTML(url) {
  const q=new URL(url).searchParams.get('q')||'baseline';
  if(WORKER)return `<!doctype html><title>Local worker fixture</title><div id="result">loading</div><button id="emit">Native fixture update</button><script>
  const nativeWorker=new Worker('/_next/static/chunks/STOMPWebWorker.aabbcc.js');
  nativeWorker.postMessage({type:'INIT',jwt:'TEST-NEVER-ARCHIVE'});
  nativeWorker.postMessage({type:'SUBSCRIBE',listenerId:'native-panel',topic:'/user/queue/filterResponse/1174',subscriptionOptions:{filter:JSON.stringify({symbols:['GOF'],sources:['fixture'],languages:['en']})}});
  let next=0;document.querySelector('#emit').onclick=()=>nativeWorker.postMessage({type:'FIXTURE_EMIT',id:'fixture-worker-'+(++next)});
  fetch(${JSON.stringify(API)},{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbols:['GOF'],sources:['fixture'],languages:['en'],pageable:{size:20},dateTimeRange:{startTime:'0',endTime:String(Date.now())}})}).then(r=>r.json()).then(()=>document.querySelector('#result').textContent='response 200');
  </script>`;
  if(SCENARIO==='godel-stream')return `<!doctype html><title>Local native stream fixture</title><div id="result">loading</div><script>
  const socket=new WebSocket('wss://terminal-api.godelterminal.com/events?jwt=TEST-NEVER-ARCHIVE');
  socket.onopen=()=>socket.send('SUBSCRIBE\\nfilter:'+JSON.stringify({sources:['fixture'],languages:['en'],symbols:['GOF']}).replace(/:/g,'\\\\c')+'\\nid:sub-1\\ndestination:/user/queue/filterResponse/1174\\n\\n\\0');
  fetch(${JSON.stringify(API)},{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbols:['GOF'],sources:['fixture'],languages:['en'],pageable:{size:20},dateTimeRange:{startTime:'0',endTime:String(Date.now())}})}).then(r=>r.json()).then(()=>document.querySelector('#result').textContent='response 200');
  </script>`;
  if(GODEL)return `<!doctype html><meta charset="utf-8"><title>Local Godel fixture</title>
<form><input id="search"><button>Search</button></form><div id="result">loading</div>
<script>
function request(q){
  document.querySelector('#result').textContent='loading';
  const body=JSON.stringify({symbols:q==='all-cef'?${JSON.stringify(CEF_SYMBOLS)}:[q.replace(/^(fetch|request)-/,'')],sources:['fixture'],languages:['en'],pageable:{size:${SCENARIO==='godel-request'?200:20},afterCursor:null,beforeCursor:null},dateTimeRange:{startTime:'0',endTime:String(Date.now())},legal:false,token:'TEST-NEVER-ARCHIVE'});
  const done=()=>document.querySelector('#result').textContent='response 200';
  if(q.startsWith('request-'))fetch(new Request(${JSON.stringify(API)},{method:'POST',headers:{'Content-Type':'application/json'},body})).then(r=>r.json()).then(done);
  else if(q.startsWith('fetch-'))fetch(${JSON.stringify(API)},{method:'POST',headers:{'Content-Type':'application/json'},body}).then(r=>r.json()).then(done);
  else {const x=new XMLHttpRequest();x.open('POST',${JSON.stringify(API)});x.setRequestHeader('Content-Type','application/json');x.onload=done;x.send(body);}
}
document.querySelector('form').onsubmit=e=>{e.preventDefault();request(document.querySelector('#search').value);};request(${JSON.stringify(q)});
</script>`;
  const action=q==='ignored-login'?'login':'filterArticles';
  const from=q==='ignored-pagination'?50:0;
  const body=JSON.stringify({type:action,queryString:q==='all-cef'?CEF_QUERY:q,from,size:50,token:'TEST-NEVER-ARCHIVE'});
  const request=q.startsWith('fetch-')
    ? `fetch('https://api.newsfilter.io/actions',{method:'POST',headers:{'Content-Type':'application/json'},body:${JSON.stringify(body)}}).then(r=>r.json()).then(()=>document.querySelector('#result').textContent='response 200');`
    : `const x=new XMLHttpRequest();x.open('POST','https://api.newsfilter.io/actions');x.setRequestHeader('Content-Type','application/json');x.onload=()=>{document.querySelector('#result').textContent='response '+x.status;};x.send(${JSON.stringify(body)});`;
  return `<!doctype html><meta charset="utf-8"><title>Local Newsfilter fixture</title>
<form action="/search"><input id="search" name="q"><button>Search</button></form><div id="result">loading</div>
<script>${request}</script>`;
}
async function mock(route) {
  const req=route.request(),u=new URL(req.url());
  if(WORKER&&u.origin===ORIGIN&&u.pathname==='/_next/static/chunks/STOMPWebWorker.aabbcc.js')return route.fulfill({contentType:'application/javascript',body:`let topic;self.onmessage=e=>{const m=e.data;if(m.type==='SUBSCRIBE')topic=m.topic;if(m.type==='FIXTURE_EMIT')self.postMessage({type:'EMIT:BUFFERED',topic,isInitial:false,data:[{id:m.id,title:${SCENARIO==='godel-rejection'?"m.id==='fixture-worker-2'?'':'Synthetic worker news'":"'Synthetic worker news'"},publicationTime:String(Date.now()),receivedTime:new Date().toISOString(),securityIds:['GOF'],originalUrl:null,private:'TEST-NEVER-ARCHIVE'}]});};`});
  if(u.origin===ORIGIN)return route.fulfill({contentType:'text/html',body:pageHTML(req.url())});
  if(u.href===API) {
    if(req.method()==='OPTIONS')return route.fulfill({status:204,headers:{'access-control-allow-origin':ORIGIN,'access-control-allow-methods':'POST','access-control-allow-headers':'content-type'}});
    const rawQuery=GODEL?req.postDataJSON().symbols[0]:req.postDataJSON().queryString;
    const q=rawQuery.startsWith('symbols:(')?'all-cef':rawQuery;
    report.fixture_requests.push({query:q,method:req.method(),at:new Date().toISOString()});
    await wait(q==='fast-first-response'?0:150);
    if(GODEL){
      if(q==='refusal')return route.fulfill({status:403,headers:{'access-control-allow-origin':ORIGIN},body:'synthetic refusal'});
      const count=SCENARIO==='godel-request'?200:1;
      return route.fulfill({status:200,contentType:'application/json',headers:{'access-control-allow-origin':ORIGIN},body:JSON.stringify({content:Array.from({length:count},(_,i)=>({id:'fixture-'+q+'-'+i,title:'Synthetic CEF release '+q+' '+i,providerName:'GlobeNewswire',publicationTime:String(Date.parse('2026-11-06T15:00:00Z')),receivedTime:'2026-11-06T15:00:00Z',originalUrl:null,securityIds:['UNVERIFIED'],description:'TEST-NEVER-ARCHIVE'})),pageSize:SCENARIO==='godel-request'?200:20,totalItems:count})});
    }
    const articles=[{id:'fixture-'+q,title:'Synthetic CEF release '+q,publishedAt:'2026-11-06T15:00:00Z',symbols:['OXLC'],sourceUrl:'https://newsfilter.io/articles/fixture-'+q,url:'https://newsfilter.io/articles/fixture-'+q,source:{id:'globenewswire',name:'GlobeNewswire',token:'TEST-NEVER-ARCHIVE'},description:'TEST-NEVER-ARCHIVE'}];
    const headers={'access-control-allow-origin':'https://newsfilter.io'};
    if(SCENARIO.startsWith('cors-')) {
      Object.assign(headers,{'date':'Sun, 08 Nov 2026 13:00:00 GMT','age':'42','cache-control':'max-age=600','retry-after':'90','x-private-fixture':'TEST-NEVER-ARCHIVE'});
      if(q.includes('exposed'))headers['access-control-expose-headers']='Date, Age, Retry-After';
    }
    const status=q==='refusal'?403:q.startsWith('retry-')?429:200;
    return route.fulfill({status,contentType:'application/json',headers,body:status!==200?'blocked':JSON.stringify({from:0,size:50,total:{value:1,relation:'eq'},articles,token:'TEST-NEVER-ARCHIVE'})});
  }
  if(u.hostname==='127.0.0.1'&&u.port===String(PORT))return route.continue();
  if(u.protocol==='chrome-extension:')return route.continue();
  report.blocked_requests.push(u.origin+u.pathname);return route.abort('blockedbyclient');
}
async function saved(n) {await popup.waitForFunction(count=>Number(document.querySelector('#frames').textContent)===count,n,{timeout:8000});}
async function navigate(q) {
  await news.locator('#search').fill(q);await news.getByRole('button',{name:'Search',exact:true}).click();
  await news.locator('#result').filter({hasText:'response'}).waitFor();
}
async function receiverStopped() {
  for(let i=0;i<60&&receiver.exitCode===null;i++)await wait(50);
  assert.equal(receiver.exitCode,0);assert.equal(await portOpen(),false);
}
async function stopped(reason) {
  const end=Date.now()+5000;
  while(Date.now()<end) {
    if(await popup.evaluate(async expected=>(await chrome.storage.session.get('captureState')).captureState?.reason===expected,reason))return;
    await wait(50);
  }
  throw Error('Capture did not stop: '+reason);
}
async function cleanup() {
  // Only this child and this temporary profile are owned by the test.
  if(receiver&&receiver.exitCode===null&&fs.existsSync(path.join(STORE,'pairing.json'))) {
    const p=JSON.parse(fs.readFileSync(path.join(STORE,'pairing.json'),'utf8'));
    if(p.pid===ready?.pid)try {await fetch(`http://127.0.0.1:${PORT}/stop`,{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+p.token},body:'{}',signal:AbortSignal.timeout(3000)});}catch{}
  }
  if(context)try {await context.close();report.browser_closed=true;}catch(error){report.browser_closed=false;report.errors.push('Browser close failed: '+String(error));}
  if(receiver&&receiver.exitCode===null)await Promise.race([new Promise(resolve=>receiver.once('exit',resolve)),wait(4000)]);
  report.receiver_exited=!!receiver&&receiver.exitCode!==null;
  report.port_closed=!(await portOpen());report.finished_at=new Date().toISOString();
  if((receiver&&(!report.receiver_exited||!report.port_closed))||(context&&!report.browser_closed)) {
    report.status='failed';report.errors.push('Owned resources did not close; inspect this evidence before another run.');process.exitCode=1;
  }
  fs.writeFileSync(path.join(RUN,'results.json'),JSON.stringify(report,null,2)+'\n');
}
(async()=>{
  fs.mkdirSync(RUN,{recursive:true});
  report.code_sha256=Object.fromEntries([...fs.readdirSync(EXTENSION).map(f=>path.join(EXTENSION,f)),path.join(ROOT,'tools/news_live.py'),path.join(ROOT,'tools/newsfilter_live.py'),__filename]
    .map(p=>[path.relative(ROOT,p),createHash('sha256').update(fs.readFileSync(p)).digest('hex')]));
  try {
    await startReceiver();
    context=await chromium.launchPersistentContext(path.join(RUN,'profile'),{channel:'chromium',headless:true,
      ignoreDefaultArgs:['--disable-background-timer-throttling','--disable-backgrounding-occluded-windows','--disable-renderer-backgrounding'],
      args:[`--disable-extensions-except=${EXTENSION}`,`--load-extension=${EXTENSION}`]});
    await context.route('**/*',mock);context.on('weberror',e=>browserErrors.push(String(e.error())));
    if(SCENARIO==='godel-stream')await context.routeWebSocket('**/*',ws=>{
      assert.ok(ws.url().startsWith('wss://terminal-api.godelterminal.com/events?'));
      streamRoute=ws;report.native_stream_commands=report.native_stream_commands||[];
      ws.onMessage(message=>report.native_stream_commands.push(String(message).split('\n')[0]));
      // No connectToServer: every WebSocket remains a local synthetic fixture.
    });
    context.on('console',m=>{if(/Refused to get unsafe header/i.test(m.text()))unsafeHeaderWarnings.push(m.text());});
    if(RELIABILITY)context.on('console',m=>{if(m.text().startsWith('Godel observer stop failed:'))console.log(m.text());});
    [worker]=context.serviceWorkers();if(!worker)worker=await context.waitForEvent('serviceworker',{timeout:10000});
    extensionId=worker.url().split('/')[2];report.extension_id=extensionId;
    report.manifest=await worker.evaluate(()=>chrome.runtime.getManifest());
    report.chromium_version=context.browser()?.version();
    news=await context.newPage();await news.goto(ORIGIN+'/search?q=baseline');await news.locator('#result').filter({hasText:'response'}).waitFor();
    await check('no capture before explicit Start',async()=>{assert.equal(snapshot().responses.length,0);});
    popup=await context.newPage();await popup.goto(`chrome-extension://${extensionId}/popup.html`);await news.bringToFront();
    await check('real popup pairs with real receiver',async()=>{
      if(SCENARIO==='expiry') {
        // A deliberately shorter lease from the real receiver's pairing file.
        // No fake clock, Chrome API stubs or production settings are used.
        const pairing=JSON.parse(fs.readFileSync(path.join(STORE,'pairing.json'),'utf8'));
        shortLeaseEnd=Date.now()+2000;
        pairing.expires_at=new Date(shortLeaseEnd).toISOString();
        await popup.locator('details').filter({has:popup.locator('#pairing')}).evaluate(el=>el.open=true);
        await popup.locator('#pairing').fill(JSON.stringify(pairing));
      } else await popup.locator('#code').fill(ready.connection_code);
      await popup.locator('#start').click();
      await popup.waitForFunction(()=>document.querySelector('#session-label').textContent==='Connected · waiting',null,{timeout:5000});
      assert.equal(report.fixture_requests.length,1,'Connecting must not make a source request');
    });
    if(RELIABILITY) {
      await require('./godel-reliability.cjs').run({context,popup,news,worker,extensionId,report,faults,check,snapshot,saved,stopped,receiverStopped,wait,STORE,PY,RUN});
    } else if(SCENARIO==='godel-refusal') {
      await check('native 403 is held in both receiver and extension and ends capture',async()=>{
        await navigate('refusal');await stopped('native_error');await receiverStopped();
        const s=snapshot();assert.equal(s.observations,0);assert.equal(JSON.parse(s.controls['godel-browser:hold']).status,403);
        const hold=await popup.evaluate(async()=>(await chrome.storage.local.get('sourceHold')).sourceHold);
        assert.equal(hold.status,403);await navigate('after-refusal');await wait(200);
        assert.equal(snapshot().responses.length,1);assert.equal(report.fixture_requests.length,3);
      });
    } else if(SCENARIO==='godel-delay') {
      await check('delayed native copy is review-only and leaves the connection running',async()=>{
        const cdp=await context.newCDPSession(news);await cdp.send('Debugger.enable');
        await news.locator('#emit').click();await news.waitForFunction(()=>CefGodelWorker.status().buffered===1);
        const paused=new Promise(resolve=>cdp.once('Debugger.paused',resolve));await cdp.send('Debugger.pause');await paused;
        report.induced_debugger_pause_started=new Date().toISOString();await wait(40000);
        await cdp.send('Debugger.resume');report.induced_debugger_pause_ended=new Date().toISOString();
        await saved(1);
        const result=await popup.evaluate(async()=>(await chrome.storage.session.get('lastResult')).lastResult);
        assert.equal(result.http_status,200);assert.equal(result.articles,0);assert.equal(result.review_only,true);assert.equal(result.delayed_records,1);
        assert.equal(await popup.locator('#session-label').textContent(),'Delayed copy · monitoring');
        assert.equal(receiver.exitCode,null);assert.equal(snapshot().observations,0);
        assert.equal(snapshot().responses.filter(r=>r.error==='delayed_browser_copy_review_only').length,1);
        assert.equal(await news.evaluate(()=>CefGodelWorker.status().active),true);
      });
      await check('the next valid native copy saves as a new baseline without re-pairing',async()=>{
        await news.locator('#emit').click();await saved(2);const s=snapshot();
        assert.equal(s.observations,1);assert.equal(s.classifications.compatibility_history,1);
        const latest=s.responses.filter(r=>JSON.parse(r.meta).receipt_kind==='passive_browser_worker_copy').at(-1);
        assert.ok(JSON.parse(latest.meta).baseline_after_delayed_response);
        assert.equal(report.fixture_requests.length,1);assert.equal(await popup.locator('#session-label').textContent(),'Receiving news');
        await popup.locator('#stop').click();await stopped('user_stopped');await receiverStopped();
      });
    } else if(SCENARIO==='godel-rejection') {
      await check('one valid native worker copy saves normally',async()=>{
        await news.locator('#emit').click();await saved(1);assert.equal(snapshot().observations,1);
      });
      await check('malformed native record retains exact safe rejection and stops',async()=>{
        await news.locator('#emit').click();await stopped('receiver_error');await receiverStopped();
        const result=await popup.evaluate(async()=>(await chrome.storage.session.get('lastResult')).lastResult);
        assert.equal(result.http_status,400);assert.equal(result.error,'invalid_capture');
        assert.equal(result.diagnostic.reason,'missing_or_oversized_text_field');assert.equal(result.diagnostic.record_index,0);
        const files=fs.readdirSync(path.join(STORE,'rejections'));assert.equal(files.length,1);
        const text=fs.readFileSync(path.join(STORE,'rejections',files[0]),'utf8'),diagnostic=JSON.parse(text);
        assert.deepEqual(diagnostic.article_fields.title,{type:'str',length:0});assert.equal(diagnostic.reason,result.diagnostic.reason);
        assert.equal(text.includes('TEST-NEVER-ARCHIVE'),false);assert.equal(snapshot().observations,1);
        assert.equal(report.fixture_requests.length,1);await news.locator('#emit').click();await wait(200);assert.equal(snapshot().observations,1);
      });
    } else if(SCENARIO==='godel-worker') {
      await check('real worker created before Start delivers selected news to the receiver',async()=>{
        assert.equal(await news.evaluate(()=>window.CefGodelWorker.status().tracked_workers),1);
        await news.locator('#emit').click();await saved(1);assert.equal(snapshot().observations,1);
        const s=snapshot();assert.equal(s.responses.filter(r=>JSON.parse(r.meta).receipt_kind==='passive_browser_worker_copy').length,1);
        for(const file of fs.readdirSync(path.join(STORE,'responses')))assert.equal(fs.readFileSync(path.join(STORE,'responses',file),'utf8').includes('TEST-NEVER-ARCHIVE'),false);
        assert.equal(report.fixture_requests.length,1);
      });
      await check('worker capture pauses on reload and resumes only by explicit action',async()=>{
        const previous=await worker.evaluate(async()=>(await chrome.storage.session.get('pairing')).pairing.captureId);
        await news.reload();await news.locator('#result').filter({hasText:'response'}).waitFor();await stopped('tab_reloaded');
        await news.locator('#emit').click();await wait(200);assert.equal(snapshot().observations,1);
        await news.bringToFront();await popup.locator('#start').click();await popup.waitForFunction(()=>document.querySelector('#session-label').textContent==='Connected · waiting');
        const current=await worker.evaluate(async()=>(await chrome.storage.session.get('pairing')).pairing.captureId);assert.notEqual(current,previous);
        await news.locator('#emit').click();await saved(1);assert.equal(snapshot().observations,2);
      });
      await check('Stop closes the receiver and removes worker capture listeners',async()=>{
        await popup.locator('#stop').click();await stopped('user_stopped');await receiverStopped();await news.locator('#emit').click();await wait(200);assert.equal(snapshot().observations,2);
        assert.equal(await news.evaluate(()=>window.CefGodelWorker.status().active),false);
      });
    } else if(SCENARIO==='godel-stream') {
      const sendFrame=(id,destination='/user/queue/filterResponse/1174')=>{
        const body=JSON.stringify({id,title:'Synthetic stream '+id,providerName:'fixture',publicationTime:String(Date.now()),receivedTime:new Date().toISOString(),securityIds:['GOF'],originalUrl:null,description:'TEST-NEVER-ARCHIVE'});
        streamRoute.send('MESSAGE\ndestination:'+destination+'\nsubscription:sub-1\nmessage-id:'+id+'\ncontent-length:'+Buffer.byteLength(body)+'\n\n'+body+'\0');
      };
      await check('native messages reach real receiver with independent clocks and no account data',async()=>{
        sendFrame('first');sendFrame('second');sendFrame('private','/user/queue/notifications');await saved(1);
        const s=snapshot();assert.equal(s.observations,2);assert.deepEqual(s.classifications,{compatibility_history:2});
        for(const file of fs.readdirSync(path.join(STORE,'responses')))assert.equal(fs.readFileSync(path.join(STORE,'responses',file),'utf8').includes('TEST-NEVER-ARCHIVE'),false);
        assert.equal(report.native_stream_commands.length,1);assert.equal(report.fixture_requests.length,1);
      });
      await check('reload pauses stream until explicit Resume and keeps fresh capture identity',async()=>{
        const previous=await worker.evaluate(async()=>(await chrome.storage.session.get('pairing')).pairing.captureId);
        await news.reload();await news.locator('#result').filter({hasText:'response'}).waitFor();await stopped('tab_reloaded');
        sendFrame('paused');await wait(200);assert.equal(snapshot().observations,2);
        await news.bringToFront();await popup.locator('#start').click();await popup.waitForFunction(()=>document.querySelector('#session-label').textContent==='Connected · waiting');
        const current=await worker.evaluate(async()=>(await chrome.storage.session.get('pairing')).pairing.captureId);assert.notEqual(current,previous);
        sendFrame('resumed');await saved(1);assert.equal(snapshot().observations,3);
      });
      await check('Stop ends receiver and prevents further stream capture',async()=>{
        await popup.locator('#stop').click();await stopped('user_stopped');await receiverStopped();sendFrame('after-stop');await wait(200);assert.equal(snapshot().observations,3);
      });
    } else if(DAY) {
      await check('daily receiver and installed observer share the same midnight deadline',async()=>{
        assert.equal(ready.mode,'until_close');
        const savedPairing=await worker.evaluate(async()=>(await chrome.storage.session.get('pairing')).pairing);
        assert.equal(savedPairing.mode,'until_close');
        assert.ok(savedPairing.end-Date.now()>20*60*1000);
        assert.equal(savedPairing.end,Date.parse(ready.expires_at));
        const observer=await news.evaluate(godel=>(godel?window.CefGodelObserver:window.CefNewsfilterObserver).status(),GODEL);
        assert.equal(Date.parse(observer.expires_at),savedPairing.end);
        assert.equal(new Date(savedPairing.end).getUTCHours(),22);
      });
      await check('all367 selected symbols survive native request copying without ticker validation',async()=>{
        await navigate('all-cef');await saved(1);
        const assessment=JSON.parse(snapshot().responses.at(-1).meta).assessment;
        assert.equal(assessment.filter_scope.eligible_count,367);
        assert.equal(assessment.filter_scope.universe_count,367);
        assert.match(await popup.locator('#coverage').innerText(),/367\/367/);
        assert.match(await popup.locator('#session-note').innerText(),/midnight Kyiv/);
      });
      await check('status alarm reads only loopback; native silence is not hidden by pairing',async()=>{
        const count=report.fixture_requests.length;
        await worker.evaluate(()=>monitor.check());
        const status=await worker.evaluate(async()=>(await chrome.storage.session.get('receiverStatus')).receiverStatus);
        assert.equal(status.native_status,'fresh');assert.equal(status.fresh_filter_eligible_count,367);
        assert.equal(status.continuous_coverage_verified,false);
        assert.equal(report.fixture_requests.length,count);
      });
      await popup.locator('body').screenshot({path:path.join(RUN,'popup-verified.png')});
      if(GODEL) await check('user Stop ends an all-day receiver early',async()=>{
        await popup.locator('#stop').click();await stopped('user_stopped');await receiverStopped();
      });
      else await check('deadline alarm stops observer and receiver even without a source update',async()=>{
        await worker.evaluate(async()=>{
          const {pairing}=await chrome.storage.session.get('pairing');const NativeDate=Date;
          globalThis.Date=class extends NativeDate {constructor(...args){super(...(args.length?args:[pairing.end+1]));}static now(){return pairing.end+1;}};
          await monitor.check();
        });
        await stopped('expired_connection');await receiverStopped();
      });
      await navigate('after-stop');assert.equal(snapshot().responses.length,1);
    } else if(SCENARIO==='navigation') {
    await check('first native XHR after full navigation is saved',async()=>{await navigate('OXLC');await saved(1);assert.equal(snapshot().observations,1);});
    await check('immediate first response survives the document handshake',async()=>{await navigate('fast-first-response');await saved(2);});
    await check('unpaired tab does not save data',async()=>{
      const other=await context.newPage();await other.goto('https://newsfilter.io/search?q=other-tab');await other.locator('#result').filter({hasText:'response'}).waitFor();
      await wait(200);assert.equal(snapshot().responses.length,2);await other.close();await news.bringToFront();
    });
    await check('native fetch also survives navigation',async()=>{await navigate('fetch-OXLC');await saved(3);});
    await check('login and pagination responses are excluded',async()=>{
      await navigate('ignored-login');await navigate('ignored-pagination');await wait(200);
      assert.equal(snapshot().responses.length,3);
      assert.equal(await popup.locator('#session-label').innerText(),'Connected · updates saved');
    });
    await check('selected public fields only; history stays excluded',async()=>{
      const s=snapshot();assert.deepEqual(s.classifications,{compatibility_history:3});
      for(const file of fs.readdirSync(path.join(STORE,'responses')))assert.equal(fs.readFileSync(path.join(STORE,'responses',file),'utf8').includes('TEST-NEVER-ARCHIVE'),false);
    });
    await popup.locator('body').screenshot({path:path.join(RUN,'popup-verified.png')});
    await check('popup Stop closes receiver and prevents further capture',async()=>{
      await popup.locator('#stop').click();await popup.locator('#status').filter({hasText:'Capture and the local receiver have stopped.'}).waitFor();
      await receiverStopped();
      await navigate('after-stop');await wait(200);assert.equal(snapshot().responses.length,3);
    });
    } else if(SCENARIO==='cors-headers') {
      await check('XHR and fetch retain only CORS-visible timing headers without warnings',async()=>{
        let count=0;
        for(const query of ['headers-hidden','headers-exposed','fetch-headers-hidden','fetch-headers-exposed']) {
          await navigate(query);await saved(++count);
          const meta=JSON.parse(snapshot().responses.at(-1).meta);
          assert.deepEqual(meta.headers,query.includes('exposed')
            ? {'date':'Sun, 08 Nov 2026 13:00:00 GMT','age':'42','cache-control':'max-age=600','retry-after':'90'}
            : {'cache-control':'max-age=600'});
          assert.deepEqual(unsafeHeaderWarnings,[]);
        }
        assert.deepEqual(snapshot().classifications,{compatibility_history:4});
        for(const file of fs.readdirSync(path.join(STORE,'responses')))assert.equal(fs.readFileSync(path.join(STORE,'responses',file),'utf8').includes('TEST-NEVER-ARCHIVE'),false);
        assert.equal(report.fixture_requests.length,5,'No additional source requests');
      });
      await check('Stop closes the receiver after CORS-filtered captures',async()=>{
        await popup.locator('#stop').click();await receiverStopped();
      });
    } else if(SCENARIO==='cors-hidden-retry'||SCENARIO==='cors-exposed-retry') {
      await check('429 remains held whether Retry-After is CORS-visible or hidden',async()=>{
        const exposed=SCENARIO==='cors-exposed-retry';
        await navigate(exposed?'retry-exposed':'retry-hidden');await stopped('native_error');
        const s=snapshot();assert.equal(s.responses.length,1);assert.equal(s.observations,0);
        assert.equal(JSON.parse(s.controls['newsfilter-browser:hold']).status,429);
        assert.equal(JSON.parse(s.responses[0].meta).headers['retry-after'],exposed?'90':undefined);
        assert.deepEqual(unsafeHeaderWarnings,[]);
        await navigate('after-429');await wait(200);assert.equal(snapshot().responses.length,1);
        assert.equal(report.fixture_requests.length,3,'No automatic retry');
      });
    } else if(SCENARIO==='godel-request') {
      await check('Request-form fetch retains filters on a 200-headline response',async()=>{
        await navigate('request-OXLC');await saved(1);
        const s=snapshot(),meta=JSON.parse(s.responses[0].meta);
        assert.equal(s.observations,200);assert.equal(meta.filters.symbols[0],'OXLC');
        assert.equal(meta.assessment.filter_scope.eligible_count,1);
        assert.equal(meta.assessment.continuity.full_page,true);assert.equal(s.controls['godel-browser:hold'],undefined);
        for(const file of fs.readdirSync(path.join(STORE,'responses'))) {
          const raw=fs.readFileSync(path.join(STORE,'responses',file),'utf8');
          assert.equal(raw.includes('TEST-NEVER-ARCHIVE'),false);
          assert.equal(JSON.parse(raw).request_scope_diagnostic.body_kind,'Request');
        }
        assert.equal(report.fixture_requests.length,2,'The bridge makes no extra source request');
      });
      await check('Request-form capture stops normally',async()=>{await popup.locator('#stop').click();await stopped('user_stopped');await receiverStopped();});
    } else if(SCENARIO==='godel') {
      await check('Godel native XHR and fetch reach the real receiver',async()=>{
        await navigate('GOF');await saved(1);await navigate('fetch-UTG');await saved(2);
        assert.equal(snapshot().observations,2);assert.equal(await popup.locator('#filter').innerText(),'UTG · 1 sources');
      });
      await check('Godel reload pauses until explicit Resume and starts a fresh baseline',async()=>{
        await news.reload();await news.locator('#result').filter({hasText:'response'}).waitFor();await stopped('tab_reloaded');
        assert.equal(await popup.locator('#session-label').innerText(),'Capture paused');assert.equal(snapshot().responses.length,2);
        await popup.locator('#start').click();await popup.waitForFunction(()=>document.querySelector('#session-label').textContent==='Connected · waiting');
        await navigate('PDI');await saved(1);assert.deepEqual(snapshot().classifications,{compatibility_history:3});
        for(const file of fs.readdirSync(path.join(STORE,'responses')))assert.equal(fs.readFileSync(path.join(STORE,'responses',file),'utf8').includes('TEST-NEVER-ARCHIVE'),false);
      });
      await popup.locator('body').screenshot({path:path.join(RUN,'popup-verified.png')});
      await check('Godel popup Stop closes its receiver and prevents further capture',async()=>{
        await popup.locator('#stop').click();await stopped('user_stopped');await receiverStopped();
        await navigate('after-stop');assert.equal(snapshot().responses.length,3);
      });
    } else if(SCENARIO==='delayed-navigation') {
      await check('a response completed during a busy background handshake is retained',async()=>{
        const busyStarted=context.waitForEvent('console',{predicate:m=>m.text()==='CEF_TEST_BACKGROUND_BUSY',timeout:3000});
        const busy=worker.evaluate(()=>{console.info('CEF_TEST_BACKGROUND_BUSY');const until=Date.now()+1000;while(Date.now()<until){}});
        await busyStarted;await navigate('fast-first-response');await busy;await saved(1);
        assert.equal(snapshot().observations,1);
      });
    } else if(SCENARIO==='left-news-page') {
      await check('leaving news for login closes this receiver',async()=>{
        await news.goto('https://newsfilter.io/login');await stopped('left_news_page');await receiverStopped();
        assert.match(await popup.locator('#status').innerText(),/paired tab left the supported news pages/);
        const result=spawnSync(PY,['-c',"import sqlite3,sys;from pathlib import Path;db=sqlite3.connect(Path(sys.argv[1]).resolve().as_uri()+'?mode=ro',uri=True);print(db.execute('select close_reason from bridge_sessions').fetchone()[0])",path.join(STORE,'live.sqlite')],{encoding:'utf8',windowsHide:true,timeout:5000});
        assert.equal(result.status,0);assert.equal(result.stdout.trim(),'left_news_page');
        assert.equal(snapshot().responses.length,0);
        await news.goto('https://newsfilter.io/search?q=back-after-leaving');await news.locator('#result').filter({hasText:'response'}).waitFor();
        assert.equal(snapshot().responses.length,0);
      });
    } else if(SCENARIO==='tab-closed') {
      await check('closing the paired tab closes its receiver',async()=>{await news.close();await stopped('tab_closed');await receiverStopped();});
    } else if(SCENARIO==='refusal') {
      await check('native 403 is held; subsequent browsing is not captured or retried',async()=>{
        await navigate('refusal');await stopped('native_error');
        const s=snapshot();assert.equal(s.responses.length,1);assert.equal(s.observations,0);
        assert.equal(JSON.parse(s.controls['newsfilter-browser:hold']).status,403);
        await navigate('after-refusal');await wait(200);assert.equal(snapshot().responses.length,1);
        assert.equal(report.fixture_requests.length,3);
      });
    } else if(SCENARIO==='expiry') {
      await check('the actual short lease expires and navigation cannot extend it',async()=>{
        await news.waitForFunction(()=>!window.CefNewsfilterObserver.status().active,null,{timeout:4000});
        await popup.waitForFunction(end=>Date.now()>=end,shortLeaseEnd,{timeout:4000});
        await navigate('after-expiry');await wait(200);assert.equal(snapshot().responses.length,0);
        assert.equal(await popup.locator('#start').isEnabled(),true);
      });
    } else if(SCENARIO==='receiver-down') {
      await check('receiver disappearance stops capture without re-pairing or source retry',async()=>{
        const pairing=JSON.parse(fs.readFileSync(path.join(STORE,'pairing.json'),'utf8'));
        assert.equal(pairing.pid,ready.pid);
        await fetch('http://127.0.0.1:8767/stop',{method:'POST',headers:{Authorization:'Bearer '+pairing.token,'Content-Type':'application/json'},body:'{}',signal:AbortSignal.timeout(3000)});
        await receiverStopped();await navigate('receiver-went-away');await stopped('local_receiver_unavailable');
        assert.equal(snapshot().responses.length,0);assert.equal(report.fixture_requests.length,2);
      });
    }
    assert.deepEqual(report.blocked_requests,[]);
    assert.deepEqual(browserErrors,[]);
    assert.deepEqual(unsafeHeaderWarnings,[]);
    report.status='passed';
  }catch(e){report.status='failed';report.errors.push(String(e.stack||e));
    if(worker)try {report.extension_state=await worker.evaluate(async()=>{const {captureState,frames,lastResult}=await chrome.storage.session.get(['captureState','frames','lastResult']);return {captureState,frames,lastResult};});}catch{}
    if(popup)try {report.popup_text=await popup.locator('body').innerText();}catch{}
    console.error(e.stack||e);process.exitCode=1;
  }finally{report.browser_errors=browserErrors;report.unsafe_header_warnings=unsafeHeaderWarnings;await cleanup();console.log('Evidence: '+path.join(RUN,'results.json'));}
})();
