//! Deterministic test support for Crust runtime development.
//!
//! `FakeMantle` is never a production fallback. The exported conformance suite
//! is designed to run unchanged against the real P04 adapter.

mod conformance;
mod fake_mantle;
mod fake_voice;

pub use conformance::{
    ADAPTER_CONFORMANCE_CHECKS, AdapterConformanceFailure, AdapterConformanceReport,
    run_adapter_conformance,
};
pub use fake_mantle::{FakeMantle, FakeMantleConfig, ManualClock};
pub use fake_voice::{FakeVoiceBackend, FakeVoiceConnection, FakeVoiceRecord};
