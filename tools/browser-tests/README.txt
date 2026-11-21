Browser bridge development checks

Run the actual unpacked Newsfilter and Godel extensions in isolated Chromium,
with the real Python receiver and SQLite storage. The browser popup, content
scripts, service worker, navigation, pairing and Stop are exercised together.
No production service, user browser profile, source login or credential is used.

From C:\Project\sec-research\tools\browser-tests:
  npm ci
  npx playwright install chromium
  npm test

The project backend/.venv Python is used by default. Set CEF_TEST_PYTHON to an
appropriate Python executable if this checkout uses a different environment.
Playwright is pinned in package-lock.json; no frontend dependencies changed.
Use bundled Chromium: normal branded Chrome/Edge do not support this unpacked
extension sideload workflow. Reference: https://playwright.dev/docs/chrome-extensions

Focused runs:
  npm run test:newsfilter
  npm run test:godel
  node bridge.e2e.cjs godel-request
  node bridge.e2e.cjs godel-day
  node bridge.e2e.cjs newsfilter-day
  node bridge.e2e.cjs delayed-navigation
  node bridge.e2e.cjs refusal
  node bridge.e2e.cjs expiry
  node bridge.e2e.cjs cors-headers
  node bridge.e2e.cjs cors-hidden-retry
  node bridge.e2e.cjs cors-exposed-retry

Every run creates a fresh profile and comparison store in:
  C:\Project\sec-research\.tmp\bridge-browser-tests\<timestamp>-<scenario>
results.json contains assertions, version, owned process IDs and shutdown
results. Successful main flows also save popup-verified.png. Evidence from
earlier versions remains in .tmp/newsfilter-browser-tests.

All source page/API requests are fulfilled by synthetic local fixtures. Only
the test's authenticated loopback receiver receives actual HTTP requests.
Unexpected page requests are blocked. The development fixture performs normal
XHR/fetch calls; the extension observes them and never makes source requests.
No original news responses, account tokens or production articles are loaded.
Only public fields should survive; fixtures include sentinel private fields
whose absence from the receiver archives is asserted.

The suite is sequential because Newsfilter uses 8767 and Godel uses 8766.
An occupied port aborts that case without touching its owner. A child receiver
normally has a two-minute bound and writes into that case's store. The two daily
fixtures run only during an authorized weekday and test the actual22:00UTC
deadline; the harness explicitly ends their owned receivers immediately after
the assertions. Cleanup uses only
the matching child identity and its own short-lived stop token, then closes the
owned Chromium context. No PID-only termination or Chrome-wide kill is used.
The test runner stops the suite if cleanup fails; inspect that case's evidence
before retrying. Profiles are retained as diagnostic evidence and are not a
persistent login or production collection environment.

The delayed-navigation case temporarily occupies only the test extension's
background event loop for one second. It tests a real first-response race,
without stubbing Chrome APIs or modifying the shipped extension. The expiry
case gives the real popup a shorter valid lease and waits for its actual end.
The CORS cases use real cross-origin Chromium XHR/fetch responses with timing
headers either exposed or hidden. They assert the saved header subset, absence
of unsafe-header console errors, and durable 429 holds with or without an
accessible Retry-After. All scenarios fail on unsafe-header console warnings.

Passing these checks validates connector mechanics, not source coverage,
publication-to-receipt speed, complete CEF ticker matching or screen delivery.
Newsfilter's completed XHR can be read after a delayed lease; a fetch response
already consumed before activation can still be missed. A single native
signed-in acceptance check remains necessary after the user reloads a release.
Both sources remain manually started comparisons. No server deployment or
unattended source browsing is enabled by this harness.

The godel-request regression exercises a native Request-form fetch with200
synthetic headlines, selected filters and sentinel private fields. It verifies
filter preservation, finite-page classification, no extra source request and
normal Stop. The left-news-page case verifies the popup's explicit stop message
and the receiver's stored reason. Unknown live request shapes still require
review; the fixture does not prove which request form a lost old scope used.
