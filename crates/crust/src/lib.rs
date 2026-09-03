//! Crust-owned runtime contracts.
//!
//! P03 intentionally provides no HTTP server, real media implementation, or
//! Discord integration. The modules here are the smallest contracts needed to
//! test those future orchestration layers independently and under overload.

pub mod filters;
pub mod lifecycle;
pub mod media;
pub mod overload;
pub mod resources;
pub mod routeplanner;
pub mod voice;
