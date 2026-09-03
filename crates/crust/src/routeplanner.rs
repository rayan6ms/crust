//! Bounded RoutePlanner state shared with outbound source transports.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

/// Stable identity and local bind address for one configured route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteEntry {
    pub identity: u64,
    pub local_address: IpAddr,
}

/// Transport and source classifications that affect route health.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteOutcome {
    ConnectionEstablished,
    DestinationDenied,
    Timeout,
    TransportFailure,
    SourceSuccess,
    SourceRateLimited,
    SourceUnavailable,
    SourceFailure,
}

impl RouteOutcome {
    const fn failed(self) -> bool {
        !matches!(self, Self::ConnectionEstablished | Self::SourceSuccess)
    }
}

/// Observable bounded health for one configured route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteHealth {
    pub route: RouteEntry,
    pub successes: u64,
    pub failures: u64,
    pub consecutive_failures: u32,
    pub last_outcome: Option<RouteOutcome>,
}

#[derive(Debug)]
struct State {
    next_index: usize,
    routes: Vec<RouteHealth>,
}

/// Rotation and health state. Route identities remain stable for this planner's lifetime.
#[derive(Clone, Debug)]
pub struct RoutePlanner {
    state: Arc<Mutex<State>>,
}

impl RoutePlanner {
    /// Builds a bounded planner. Identity zero is reserved for "no route".
    #[must_use]
    pub fn new(addresses: impl IntoIterator<Item = IpAddr>) -> Self {
        let routes = addresses
            .into_iter()
            .enumerate()
            .map(|(index, local_address)| RouteHealth {
                route: RouteEntry {
                    identity: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                    local_address,
                },
                successes: 0,
                failures: 0,
                consecutive_failures: 0,
                last_outcome: None,
            })
            .collect();
        Self {
            state: Arc::new(Mutex::new(State {
                next_index: 0,
                routes,
            })),
        }
    }

    /// Selects the next compatible route. A known destination family is enforced here;
    /// Mantle enforces the same rule after DNS resolution for host names.
    #[must_use]
    pub fn select(&self, destination: Option<IpAddr>) -> Option<RouteEntry> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = state.routes.len();
        for offset in 0..count {
            let index = state.next_index.wrapping_add(offset) % count;
            let route = state.routes[index].route;
            if destination
                .is_none_or(|destination| destination.is_ipv4() == route.local_address.is_ipv4())
            {
                state.next_index = index.wrapping_add(1) % count;
                return Some(route);
            }
        }
        None
    }

    /// Selects for Mantle's credential-safe authority context.
    #[must_use]
    pub fn select_for_authority(&self, authority: &str) -> Option<RouteEntry> {
        self.select(authority_ip(authority))
    }

    /// Applies one transport or source classification to the matching stable identity.
    pub fn report(&self, identity: u64, outcome: RouteOutcome) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(health) = state
            .routes
            .iter_mut()
            .find(|health| health.route.identity == identity)
        else {
            return;
        };
        health.last_outcome = Some(outcome);
        if outcome.failed() {
            health.failures = health.failures.saturating_add(1);
            health.consecutive_failures = health.consecutive_failures.saturating_add(1);
        } else {
            health.successes = health.successes.saturating_add(1);
            health.consecutive_failures = 0;
        }
    }

    #[must_use]
    pub fn health(&self) -> Vec<RouteHealth> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .routes
            .clone()
    }
}

fn authority_ip(authority: &str) -> Option<IpAddr> {
    authority
        .parse::<SocketAddr>()
        .map(|address| address.ip())
        .ok()
        .or_else(|| authority.trim_matches(['[', ']']).parse().ok())
        .or_else(|| {
            let (host, port) = authority.rsplit_once(':')?;
            port.parse::<u16>().ok()?;
            host.parse().ok()
        })
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn identities_are_stable_and_rotation_respects_destination_family() {
        let planner = RoutePlanner::new([
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        ]);
        assert_eq!(planner.health()[0].route.identity, 1);
        assert_eq!(planner.health()[1].route.identity, 2);
        assert_eq!(
            planner.select_for_authority("127.0.0.1:8080"),
            Some(RouteEntry {
                identity: 2,
                local_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            })
        );
        assert_eq!(
            planner.select_for_authority("[::1]:8080"),
            Some(RouteEntry {
                identity: 1,
                local_address: IpAddr::V6(Ipv6Addr::LOCALHOST),
            })
        );
    }

    #[test]
    fn health_combines_transport_and_source_classifications() {
        let planner = RoutePlanner::new([IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        planner.report(1, RouteOutcome::ConnectionEstablished);
        planner.report(1, RouteOutcome::SourceRateLimited);
        let health = planner.health()[0];
        assert_eq!(health.successes, 1);
        assert_eq!(health.failures, 1);
        assert_eq!(health.last_outcome, Some(RouteOutcome::SourceRateLimited));
    }
}
