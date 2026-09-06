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

- [x] Prove disabled RoutePlanner unnecessarily opens two TCP connections for two source requests.
- [x] Use Mantle's pooled source/control and playback paths only when RoutePlanner is disabled; preserve enabled route isolation.
- [x] Run regression, adapter/filter tests, and Clippy.
- [x] Compare pooled playback on Oracle; retain the proved connection reuse reduction without claiming an audio speedup.
