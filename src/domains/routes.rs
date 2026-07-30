//! Allocation-free client-side route-shape validation.
use crate::error::{FitzError, Result};

#[derive(Clone, Copy)]
struct Shape {
    segments: usize,
    first_wildcard: Option<usize>,
    double_wildcard: bool,
    wildcard_suffix: bool,
}

///
/// # Errors
/// Returns an error when validation, encoding, transport, or broker processing fails.
pub fn validate_concrete_route(route: &str, scheme: &str) -> Result<()> {
    let shape = scan(route, scheme)?;
    if shape.first_wildcard.is_some() {
        return invalid("wildcards are not allowed in concrete routes");
    }
    Ok(())
}

///
/// # Errors
/// Returns an error when validation, encoding, transport, or broker processing fails.
pub fn validate_fixed_route(route: &str, scheme: &str, segments: usize) -> Result<()> {
    let shape = scan(route, scheme)?;
    if shape.segments != segments || shape.first_wildcard.is_some() {
        return invalid("route has the wrong shape");
    }
    Ok(())
}

///
/// # Errors
/// Returns an error when validation, encoding, transport, or broker processing fails.
pub fn validate_selector_route(
    route: &str,
    scheme: &str,
    segments: usize,
    realm_wildcard: bool,
) -> Result<()> {
    let shape = scan(route, scheme)?;
    if shape.segments != segments || shape.first_wildcard == Some(0) {
        return invalid("selector has the wrong shape");
    }
    let Some(first) = shape.first_wildcard else {
        return Ok(());
    };
    if shape.double_wildcard || !shape.wildcard_suffix {
        return invalid("selector has invalid wildcard placement");
    }
    if first == segments - 1 || (realm_wildcard && first == 1) {
        return Ok(());
    }
    invalid("selector has invalid wildcard placement")
}

fn scan(route: &str, scheme: &str) -> Result<Shape> {
    if route.len() > u16::MAX as usize {
        return invalid("route exceeds the 65,535-byte TLV value limit");
    }
    let bytes = route.as_bytes();
    let scheme_bytes = scheme.as_bytes();
    let start = scheme_bytes.len() + 3;
    if scheme_bytes.is_empty()
        || bytes.len() <= start
        || !bytes.starts_with(scheme_bytes)
        || bytes.get(scheme_bytes.len()..start) != Some(b"://")
    {
        return invalid("route has the wrong scheme");
    }

    let mut shape = Shape {
        segments: 0,
        first_wildcard: None,
        double_wildcard: false,
        wildcard_suffix: true,
    };
    let mut segment_start = start;
    for index in start..=bytes.len() {
        if index != bytes.len() && bytes[index] != b'/' {
            continue;
        }
        if index == segment_start {
            return invalid("route contains an empty segment");
        }
        let segment = &bytes[segment_start..index];
        let wildcard = segment == b"*";
        let double_wildcard = segment == b"**";
        if wildcard || double_wildcard {
            shape.first_wildcard.get_or_insert(shape.segments);
            shape.double_wildcard |= double_wildcard;
        } else {
            if segment.contains(&b'*') {
                return invalid("wildcards must occupy a complete segment");
            }
            if shape.first_wildcard.is_some() {
                shape.wildcard_suffix = false;
            }
        }
        shape.segments += 1;
        segment_start = index + 1;
    }
    Ok(shape)
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(FitzError::Protocol(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_concrete_route_shape_given_valid_scheme_and_segment_count_when_validation_runs()
     {
        // Arrange
        let route = "queue://realm/area/resource";

        // Act
        let result = validate_fixed_route(route, "queue", 3);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_accept_selector_routes_given_terminal_wildcards_when_validation_runs() {
        // Arrange
        let selectors = ["queue://realm/area/*", "queue://realm/*/*"];

        // Act
        let results = selectors.map(|route| validate_selector_route(route, "queue", 3, true));

        // Assert
        assert!(results.iter().all(Result::is_ok));
    }

    #[test]
    fn should_reject_wrong_scheme_given_domain_route_when_validation_runs() {
        // Arrange
        let route = "notice://realm/area/resource";

        // Act
        let result = validate_fixed_route(route, "queue", 3);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_empty_segment_given_route_with_empty_component_when_validation_runs() {
        // Arrange
        let route = "queue://realm//resource";

        // Act
        let result = validate_fixed_route(route, "queue", 3);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_nonterminal_wildcard_given_selector_wildcard_in_middle_when_validation_runs() {
        // Arrange
        let route = "queue://realm/*/resource";

        // Act
        let result = validate_selector_route(route, "queue", 3, true);

        // Assert
        assert!(result.is_err());
    }
}
