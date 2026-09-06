# Active integration findings

Raydio's original one-frame handoff exposed synchronous finite-source HTTP range
reopens directly to Oto's 20 ms deadline. A local three-minute real-source run at
volume 70 recorded 11 reads taking 90–130 ms, while the median was 0.538 ms. Five
consecutive empty frame opportunities make Oto drain silence and turn Speaking
off, matching the user's mid-song stutter report.

The Mantle adapter now holds up to 16 encoded frames (320 ms) and one owned read
on the existing blocking pool. Full stops read ahead; no frame is dropped.
Prefetched EOF/errors follow queued media, position follows delivery, paused
audio is retained, seek/replacement discard it, and stop releases the allocation.
Live-source polling is unchanged. Existing buffered media can delay audible
filter/volume changes by up to 320 ms; no new dedicated worker thread is added.

The matching isolated source run produced 9,000 frames in 180 seconds, zero
reads over 20 ms, and a 0.618 ms maximum consumer read. Receiver and whole-bot
memory checks were completed in Raydio. Playback PSS rose 193 KiB; this is an
audio fix, not a memory reduction. Two fresh 150-second receiver runs had no
silent PCM blocks or clipping, but still reported 3 and 5 lost packets and
concealment. The second run recorded 7,503 successful outgoing packets with
no sequence/timestamp discontinuities and a 21.041 ms maximum send interval.
This isolates the periodic source starvation fix without claiming lossless
Discord reception. A further deterministic test reproduced filter commands
blocking queued audio behind an unfinished source read. Such updates now wait
in an explicitly bounded queue while frames remain deliverable; accepted updates
retain order and drain before source mutation/stop. All 18 adapter tests pass
(two existing manual benchmarks excluded), plus Clippy. Raw evidence lives in Raydio's
`evidence/STUTTER-DIAGNOSIS.md` and `examples/source_timing.rs`.

Oracle receiver testing exposed a separate playing-zombie failure: terminal Oto AudioChanged failures were ignored. A deterministic invalid-frame test timed out before the repair and now receives a typed VoiceClose; the adapter consults the current durable sender snapshot so stale source events cannot close healthy replacement audio. A warning records only typed failure/counters. Oto revision 3577fed also avoids fatal attribution of Linux off-CPU wall-time delays. 10 bridge tests pass; the earlier cloud terminal failure is still unclassified and must not be claimed fixed solely from this regression.

The disabled RoutePlanner was passed as a policy to Mantle, unnecessarily disabling HTTP pooling. The deterministic two-load fixture opened two TCP connections before and one after using ordinary Mantle source/control and playback APIs for the disabled case. Enabled routing retains no-cross-route reuse. 19 adapter/filter tests and Clippy pass. A one-worker Oracle full track completed without send or RTP errors; maximum interval 85.607 ms, 28 gaps over 40 ms. This variable cloud result is not a proved audio speedup. Temporary source-read logging was removed before publication. Full comparative audio evidence is retained in Raydio.

The continuous Oracle attempt stopped at 18:05 UTC on 2026-09-06 with one source
overrun and no send failures. Tokio 1.53.1 mpsc try_recv can park while a producer
is publishing, violating the bridge's nonblocking contract. The bridge now uses
poll_recv's register/recheck readiness and channel-closure wakeups; the redundant
AtomicWaker was removed. Capacity remains one. The cooperative-yield regression
fails on the former bridge; cancellation and 4,096 concurrent ordered frames
also pass. This proves handoff behavior, not the cause of the cloud CPU overrun.
Oto 3b770a9 retains wall/CPU timings, included in the terminal warning, with the
CPU kill gate unchanged. All 13 adapter tests and Clippy pass. The 10-sender
release benchmark retained 1,491 allocations / 1,500 frames (boxed async source
futures), no new threads, and sub-millisecond p99 batch interval error in both
runs. PSS/CPU variation is not claimed as an improvement. Full before/after
evidence is in Raydio; six-hour receiver qualification is still pending.

The poll_recv candidate still terminated on Oracle. A temporary stage-timing
build reproduced a 2.205 ms wall/2.210 ms CPU overrun: receive 2.485 us, copy
1.062 us, Notify::notify_one 2,191.168 us. That call includes a waiter mutex
and runtime task wake; the trace does not separate their costs. The consumption
handoff now uses one AtomicBool permit and AtomicWaker register/recheck instead
of Notify's per-frame waiter lock. Early consumption, latest-waker replacement,
4,096 concurrent ordered frames, bounded capacity, and cancellation all pass
(14 tests). Temporary stage timing is removed; the CPU kill gate is unchanged.
Live qualification still must show whether runtime wake cost also needs repair.

The atomic handoff run stayed connected for 7m19s but recorded a 605 ms
mid-song quiet interval and 1.404 seconds total receiver concealment, with no
reported packet loss. A 64-frame (1.28 s) prefetch experiment passed 19 adapter
tests and Clippy, but its fresh 86-second receiver run still had a 751 ms
mid-song quiet interval and four lost packets. That experiment did not isolate
source-read delay and did not demonstrate a benefit, so production read-ahead
is restored to 16 frames (320 ms). A lifecycle-only audio shutdown log now
preserves all sender counters; the latest node window was too late to classify
the receiver gap. Do not claim the remaining gaps originate at the source,
network, VM scheduler, or receiver until corresponding evidence is collected.

The AtomicWaker consumption handoff still terminated on Oracle at 19:44:36 UTC:
5.907 ms wall, 5.913 ms thread CPU, zero skipped deadlines and send failures.
The new ring bridge removes both the mpsc receive semaphore lock and producer
task wake from normal audio callbacks. A capacity-one rtrb 0.3.5 ring retains
register/recheck consumer readiness; completion guards cover cancellation before
the producer's first poll. The producer checks the consumption atomic after
1 ms sleeps while its one frame is queued; it does not poll the media source on
a timer. Cancellation still selects immediately. This is a deliberate prototype
tradeoff pending Oracle results: the ten-sender benchmark increases CPU from
roughly 1% to 3.33% of one core but lowers warmed process PSS from 3,694 to
2,696 KiB; allocations remain 1,491/1,500 frames and no new threads. The 14 bridge
tests plus abort-before-start regression and Clippy pass. No six-hour claim.

Ring live diagnostics (Raydio evidence/ORACLE-ENDURANCE.md): four short runs
had no CPU source overruns, but receiver gaps remain. One 461 ms quiet interval
had 200 outgoing packets in a surrounding four-second window, max 26.676 ms
spacing, no sequence/timestamp gaps, and no Discord DAVE failures. A different
stats-only run had source starvation and 3.45 s silent concealment. Raydio's
log filter had suppressed this adapter's info-level shutdown counters; b446bd6
corrects it. An isolated source pull reproduced a 3.94 s wait; independent
libopus decoding of Oracle output found no mid-song silence or malformed
20 ms frames. Socket-read traces did not reproduce that long wait. Compressed
file staging is being benchmarked as a bounded experiment before changing the
media implementation. Neither downstream delivery nor six-hour reliability is
qualified by these short runs.
