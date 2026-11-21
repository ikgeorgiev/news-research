/* Reproduce a paused page's delayed copy using only a synthetic native worker.
 * No installed extension, source traffic or live comparison store is accessed.
 */
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {spawnSync}=require('node:child_process'),{chromium}=require('playwright');
const ROOT=path.resolve(__dirname,'../..'),RUN=path.join(ROOT,'.tmp/bridge-browser-tests',new Date().toISOString().replace(/[:.]/g,'-')+'-godel-suspension');
fs.mkdirSync(RUN,{recursive:true});
const report={started_at:new Date().toISOString(),fixture_only:true,external_requests:0,checks:[],induced_debugger_pause_seconds:40};
let browser;
(async()=>{try{
  browser=await chromium.launch({headless:true,ignoreDefaultArgs:['--disable-background-timer-throttling','--disable-backgrounding-occluded-windows','--disable-renderer-backgrounding']});
  const context=await browser.newContext();
  await context.addInitScript({path:path.join(ROOT,'tools/godel_bridge/worker-observer.js')});
  await context.route('**/*',route=>{
    const url=new URL(route.request().url());
    if(url.origin!=='https://app.godelterminal.com'){report.external_requests++;return route.abort();}
    if(url.pathname==='/_next/static/chunks/STOMPWebWorker.abc123.js')return route.fulfill({contentType:'application/javascript',body:`onmessage=e=>{if(e.data.type==='fixture')postMessage({type:'EMIT:BUFFERED',topic:'/topic/news/godel-headlines',isInitial:false,data:[{id:'fixture',title:'Synthetic headline',publicationTime:String(Date.now()),receivedTime:new Date().toISOString(),securityIds:[]}]});};`});
    return route.fulfill({contentType:'text/html',body:`<title>Local suspension fixture</title><script>window.nativeWorker=new Worker('/_next/static/chunks/STOMPWebWorker.abc123.js');nativeWorker.postMessage({type:'SUBSCRIBE',listenerId:'fixture',topic:'/topic/news/godel-headlines'});</script>`});
  });
  const page=await context.newPage();await page.goto('https://app.godelterminal.com/');
  await page.evaluate(()=>{window.copies=[];CefGodelWorker.start({end:Date.now()+120000,capture_id:'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',onEvent:x=>copies.push(x)});nativeWorker.postMessage({type:'fixture'});});
  await page.waitForFunction(()=>CefGodelWorker.status().buffered===1);
  const cdp=await context.newCDPSession(page);await cdp.send('Debugger.enable');
  const paused=new Promise(resolve=>cdp.once('Debugger.paused',resolve));
  await cdp.send('Debugger.pause');await paused;report.pause_started_at=new Date().toISOString();
  await new Promise(resolve=>setTimeout(resolve,40000));
  await cdp.send('Debugger.resume');report.pause_ended_at=new Date().toISOString();
  await page.waitForFunction(()=>copies.length===1);
  const event=await page.evaluate(()=>{CefGodelWorker.stop();return copies[0];});
  fs.writeFileSync(path.join(RUN,'fixture-copy.json'),JSON.stringify(event));
  const code="import sys,json;from pathlib import Path;sys.path.insert(0,'tools');import news_live as N,godel_stream as G,bridge_rejection as D;e=json.loads(Path(sys.argv[1]).read_text());\ntry:G.parse_worker_record(N,e['records'][0],N.dt(e['browser_body_observed_at']));raise RuntimeError('Expected preexisting stale-copy rejection')\nexcept ValueError as x:print(json.dumps(D.describe(json.dumps(e).encode(),e,G.InvalidStreamRecord(0,x))))";
  const py=spawnSync(path.join(ROOT,'backend/.venv/Scripts/python.exe'),['-c',code,path.join(RUN,'fixture-copy.json')],{cwd:ROOT,encoding:'utf8',windowsHide:true,timeout:5000});
  assert.equal(py.status,0,py.stderr);const diagnostic=JSON.parse(py.stdout);report.diagnostic=diagnostic;
  assert.equal(diagnostic.reason,'unordered_or_delayed_stream_copy');assert.ok(diagnostic.message_to_batch_seconds>=40);
  assert.equal(report.external_requests,0);report.checks.push('Explicit debugger pause delays a valid native worker copy past the current30-second gate','Selected rejection metadata identifies the clock gap','This is an induced pause, not a reproduction of background throttling or the original native failure; no source request or live-store write');
  report.status='reproduced_independent_suspension_failure';
}catch(error){report.status='failed';report.error=String(error);process.exitCode=1;}
finally{if(browser)await browser.close();report.finished_at=new Date().toISOString();fs.writeFileSync(path.join(RUN,'results.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({...report,output:RUN}));}})();
