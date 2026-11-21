// Sequential: each scenario owns its profile and the same bounded loopback port.
const {spawnSync}=require('node:child_process');
const path=require('node:path');
const scenarios=['navigation','delayed-navigation','left-news-page','tab-closed','refusal','expiry','receiver-down','godel','godel-request','godel-stream','godel-worker','godel-refusal','godel-rejection','godel-delay','cors-headers','cors-hidden-retry','cors-exposed-retry','godel-day','newsfilter-day'];
// The real-alarm reliability soak takes about12 minutes and requires the actual
// authorized weekday window. Run explicitly with --reliability; never fake time.
if(process.argv.includes('--reliability'))scenarios.push('godel-reliability');
let passed=0,failed=0;
for(const scenario of scenarios) {
  const result=spawnSync(process.execPath,[path.join(__dirname,'bridge.e2e.cjs'),scenario],{stdio:'inherit',windowsHide:true});
  if(result.status!==0){failed++;break;}
  passed++;
}
console.log(`${passed}/${scenarios.length} Chromium scenarios passed${failed?' (stopped on first failure)':''}`);
process.exitCode=failed?1:0;
