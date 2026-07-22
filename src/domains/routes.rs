//! Opaque route wire-size checks.
use crate::error::{FitzError, Result};

fn validate_wire_size(route: &str) -> Result<()> {
    if route.len() > u16::MAX as usize {
        Err(FitzError::Protocol(
            "route exceeds the 65,535-byte TLV value limit".into(),
        ))
    } else {
        Ok(())
    }
}

pub fn validate_concrete_route(route: &str, _scheme: &str) -> Result<()> {
    validate_wire_size(route)
}
pub fn validate_fixed_route(route: &str, _scheme: &str, _segments: usize) -> Result<()> {
    validate_wire_size(route)
}
pub fn validate_selector_route(
    route: &str,
    _scheme: &str,
    _segments: usize,
    _realm_wildcard: bool,
) -> Result<()> {
    validate_wire_size(route)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn routes_are_opaque() {
        for route in ["anything", "queue://realm/*/x", "not even a URI"] {
            validate_fixed_route(route, "ignored", 99).unwrap();
        }
    }
}
