// Observe request starts before page scripts, but never read response bodies
// until the background confirms an existing, unexpired session for this tab.
window.CefNewsfilterObserver?.prepare();
