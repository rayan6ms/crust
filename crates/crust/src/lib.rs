//! Crust-owned runtime contracts.
//!
//! P03 intentionally provides no HTTP server, real media implementation, or
//! Discord integration. The modules here are the smallest contracts needed to
//! test those future orchestration layers independently and under overload.

#[cfg(test)]
#[global_allocator]
static TEST_ALLOCATOR: &stats_alloc::StatsAlloc<std::alloc::System> =
    &stats_alloc::INSTRUMENTED_SYSTEM;

pub mod extensions;
pub mod filters;
pub mod lifecycle;
pub mod media;
pub mod overload;
pub mod resources;
pub mod routeplanner;
pub mod voice;
