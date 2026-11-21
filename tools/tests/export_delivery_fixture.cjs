// Offline compatibility check using the real TypeScript collector, with fake data and clocks.
// Usage: node tools/tests/export_delivery_fixture.cjs <new-output.json>
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const root = path.resolve(__dirname, '../..')
const ts = require(path.join(root, 'frontend/node_modules/typescript'))
const source = fs.readFileSync(path.join(root, 'frontend/lib/delivery-check.ts'), 'utf8')
const compiled = ts.transpileModule(source, { compilerOptions: {
  module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022,
}, reportDiagnostics: true })
if (compiled.diagnostics.some(d => d.category === ts.DiagnosticCategory.Error)) throw new Error('Collector did not compile')
const exported = {}
vm.runInNewContext(compiled.outputText, { exports: exported, Date, performance: { now: () => 0 },
  setTimeout: () => 0, clearTimeout: () => {}, }, { timeout: 1000 })
let elapsed = 0
const wall = Date.parse('2026-11-09T15:00:00Z')
const check = new exported.DeliveryCheck(() => ({ wall: wall + elapsed, mono: 1000 + elapsed }))
const item = (id, seen) => ({ id, title: 'Synthetic fund ' + id, url: 'https://example.com/' + id,
  provider: 'Synthetic provider', source: 'Synthetic source', published_at: new Date(wall).toISOString(),
  first_seen_at: new Date(wall + seen).toISOString(), alert_sent_at: null, summary: null,
  tickers: [], read_key: String(id), dedupe_group: String(id) })
check.start([item(1, -1000)])
elapsed = 100
check.response(check.beginRequest('automatic'), [item(2, 50)])
elapsed += 75
check.viewport(2)
check.response(check.beginRequest('automatic'), [item(3, 50)])
elapsed += 61000
check.viewport(3)
check.response(check.beginRequest('automatic'), [item(4, 50)]) // remains unobserved
check.response(check.beginRequest('queued'), [item(5, 50)])
elapsed += 100
check.viewport(5) // click-to-load must remain excluded
check.response(check.beginRequest('manual'), [item(6, 50)])
check.stop()
const result = check.export()
result.synthetic_fixture = true
result.fixture_note = 'Synthetic data and clock; exported by the actual frontend DeliveryCheck class. Not live latency evidence.'
fs.writeFileSync(process.argv[2], JSON.stringify(result, null, 2) + '\n', { flag: 'wx' })
console.log(`Exported ${result.articles.length} synthetic rows from the real TypeScript collector`)
