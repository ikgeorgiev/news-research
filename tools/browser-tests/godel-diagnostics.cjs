/* Isolated popup update check. No source page, account, receiver or external request. */
const assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path');
const {chromium}=require('playwright');
const ROOT=path.resolve(__dirname,'../..');
const RUN=path.join(ROOT,'.tmp/bridge-browser-tests',new Date().toISOString().replace(/[:.]/g,'-')+'-godel-diagnostics');
const EXT=path.join(RUN,'extension');
fs.mkdirSync(RUN,{recursive:true});fs.cpSync(path.join(ROOT,'tools/godel_bridge'),EXT,{recursive:true});
const BEFORE=path.join(ROOT,'.tmp/realtime-research-20261109/godel-rejection-20261109T1510/before');
for(const file of ['popup.js','popup.html'])fs.copyFileSync(path.join(BEFORE,file),path.join(EXT,file));
let context;const report={started_at:new Date().toISOString(),fixture_only:true,checks:[],external_requests:0};
(async()=>{try{
  context=await chromium.launchPersistentContext(path.join(RUN,'profile'),{channel:'chromium',headless:true,args:[`--disable-extensions-except=${EXT}`,`--load-extension=${EXT}`]});
  await context.route('**/*',route=>{if(route.request().url().startsWith('chrome-extension:'))return route.continue();report.external_requests++;return route.abort();});
  let [worker]=context.serviceWorkers();if(!worker)worker=await context.waitForEvent('serviceworker');
  const extensionId=worker.url().split('/')[2],url=`chrome-extension://${extensionId}/popup.html`;
  await worker.evaluate(async()=>{
    await chrome.storage.session.set({frames:1,dropped:0,captureState:{status:'stopped',reason:'receiver_error',at:'2026-11-09T14:53:21.987Z'},
      lastResult:{at:'2026-11-09T14:53:21.900Z',http_status:400,error:'invalid_capture',transport:'native_worker',private:'NEVER-EXPORT'},
      privateFixture:'NEVER-EXPORT'});
    await chrome.storage.local.set({diagnostics:[{at:'2026-11-09T14:53:21.987Z',kind:'receiver_error',private:'NEVER-EXPORT'}]});
  });
  let popup=await context.newPage();await popup.goto(url);assert.equal(await popup.locator('#diagnostics').count(),0);
  const before=await worker.evaluate(()=>chrome.storage.session.get(null));await popup.close();
  for(const file of ['popup.js','popup.html'])fs.copyFileSync(path.join(ROOT,'tools/godel_bridge',file),path.join(EXT,file));
  popup=await context.newPage();await popup.goto(url);
  await popup.waitForFunction(()=>document.querySelector('#diagnostics')?.value?.includes('invalid_capture'));
  await popup.locator('#connection-diagnostics').evaluate(el=>el.open=true);
  const text=await popup.locator('#diagnostics').inputValue(),data=JSON.parse(text);
  assert.equal(data.last_reply.http_status,400);assert.equal(data.capture.reason,'receiver_error');assert.equal(data.updates_saved,1);
  assert.equal(text.includes('NEVER-EXPORT'),false);report.checks.push('Reopening unpacked popup loads diagnostics without extension reload');
  assert.deepEqual(await worker.evaluate(()=>chrome.storage.session.get(null)),before);report.checks.push('Existing session evidence is preserved');
  assert.equal(report.external_requests,0);report.checks.push('No source or receiver request; only selected diagnostics exported');
  await popup.screenshot({path:path.join(RUN,'popup.png')});report.status='passed';
}catch(error){report.status='failed';report.error=String(error);process.exitCode=1;}
finally{if(context)await context.close();report.finished_at=new Date().toISOString();fs.writeFileSync(path.join(RUN,'results.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({...report,output:RUN}));}})();
