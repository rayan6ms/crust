# Current task: Raydio finite-source stutters

- [x] Reproduce the 90–130 ms periodic source-read stalls at 20 ms paced delivery.
- [x] Keep 16 bounded encoded frames ahead of delivery using the existing media worker pool.
- [x] Preserve delivered position, EOF/error ordering, pause/resume, seek, replacement, and stop.
- [x] Run adapter conformance, regression tests, formatting, and Clippy.
- [x] Reproduce and fix filter updates blocking buffered delivery while an HTTP read is pending; bound and test deferred updates.
- [x] Repeat the three-minute real-source timing test: 11 deadline-exceeding reads become zero.
- [x] Complete prototype full-bot receiver, memory, controls, and UDP send-timing measurements in Raydio; residual receiver loss remains separately qualified.
- [x] Pin the validated public revision in Raydio and publish corrected native packages.

- [x] Reproduce and report terminal Oto sender failures instead of retaining a playing zombie.
- [ ] Classify the Oracle terminal stop and verify the corrected stack on the existing free VM.
- [x] Replace Tokio try_recv's possible parking path with poll_recv, remove the duplicate waker, and test cancellation, cooperative yielding, concurrent delivery, and terminal ordering.
- [x] Retain Oto wall/CPU overrun timings in the terminal warning; run adapter tests, Clippy, and before/after bridge benchmark.
- [x] Isolate the repeated overrun to producer notification and replace Notify's waiter lock with an atomic consumption permit; test readiness/cancellation and concurrent ordering.
- [x] Test and revert 64-frame prefetch: receiver gaps remained and source-read causality was unproved. Retain lifecycle sender counters for the next diagnosis.
- [x] Replace the remaining Tokio frame channel with a capacity-one rtrb ring and remove producer task waking from the audio callback; test startup cancellation and benchmark the timer cost.
- [ ] Qualify the ring bridge on Oracle; CPU-bound source kill gate remains unchanged.
- [x] Correlate a receiver gap against actual outgoing packet timings and correct Raydio's suppressed lifecycle-summary log filter.
- [x] Reproduce a 3.94-second frame-source wait without Discord and independently decode the actual Oracle output: no mid-song silence, clipping, or malformed durations.
- [x] Measure bounded compressed-source staging: max source read 244.864 to 0.957 ms; keep downstream gaps separately qualified.

- [x] Prove disabled RoutePlanner unnecessarily opens two TCP connections for two source requests.
- [x] Use Mantle's pooled source/control and playback paths only when RoutePlanner is disabled; preserve enabled route isolation.
- [x] Run regression, adapter/filter tests, and Clippy.
- [x] Compare pooled playback on Oracle; retain the proved connection reuse reduction without claiming an audio speedup.

- [x] Integrate opt-in staging and retain one completed compressed input per player, with fresh playback state and cancellation on repeat.
- [x] Test repeat sequence reset, stopped controls, cancellation preflight, replacement failure, explicit stop/shutdown, oversized fallback, and existing adapter conformance.
- [x] Run final adapter/server Clippy and publish the integration pin.
- [ ] Compare the complete Oracle source and receiver paths in Raydio.

- [x] Use Oto's concrete owned frame channel, removing duplicate Crust queue/readiness/consumption implementation without exempting arbitrary callbacks.
- [x] Preserve cancellation, retained source errors, frame ordering, replacement, and source-read acknowledgement: 14 adapter tests pass with the published Oto pin; Clippy passes with warnings denied.
- [ ] Validate the committed owned-channel integration on Oracle using Raydio's receiver and host metrics.

- [x] Pin Oto d95798f: owned-channel consumption wakes its producer immediately; generic callbacks retain the timer fallback. All 14 adapter tests and all-target Clippy pass. Oto benchmark records 2633 to 501 producer polls over 250 frames; live audio qualification remains pending.

## Raydio control continuity (September 9)

- [x] Reproduce four unnecessary voice-source replacements across four gain/filter updates.
- [x] Preserve the paced source for gain/filter controls; retain replacement for transport, seek, pause/resume and track changes.
- [x] Reproduce redundant filters waiting on a pending read; skip identical committed settings without reordering queued changes.
- [x] Compare two full plays of the same staged input at volume 70: identical updates change 8,238 packets before, zero after, out of 10,653 frames.
- [x] Run workspace regression/conformance tests: 152 pass, four existing manual benchmarks ignored.
- [ ] Verify the integrated candidate with a fresh Discord receiver on Oracle; source-only comparisons do not establish delivery quality.
