//! Allocation-free client-side route-shape validation.
use crate::error::{FitzError, Result};

#[derive(Clone, Copy)]
struct Shape {
    segments: usize,
    first_wildcard: Option<usize>,
    double_wildcard: bool,
    double_wildcard_count: usize,
    wildcard_suffix: bool,
}

/// Validate an exact route or strict whole-segment registration pattern.
/// `required_segments == 0` keeps route depth flexible for Notice and RPC.
///
/// # Errors
/// Returns an error when the scheme, segments, wildcard tokens, or required
/// concrete depth are invalid.
pub fn validate_registration_pattern(
    route: &str,
    scheme: &str,
    required_segments: usize,
) -> Result<()> {
    let shape = scan(route, scheme)?;
    if required_segments == 0 {
        return Ok(());
    }
    if shape.double_wildcard_count == 0 {
        if shape.segments == required_segments {
            return Ok(());
        }
    } else if shape.segments - shape.double_wildcard_count <= required_segments {
        return Ok(());
    }
    invalid("registration pattern cannot match the required route depth")
}

/// Validate the full selector matrix shared by Stream READ and SUBSCRIBE.
pub fn validate_stream_selector(route: &str) -> Result<()> {
    if route == "stream://**" {
        return Ok(());
    }
    scan(route, "stream")?;
    let Some(path) = route.strip_prefix("stream://") else {
        return invalid("stream selector has the wrong scheme");
    };
    let parts = path.split('/').collect::<Vec<_>>();
    if let [realm, "**"] = parts.as_slice()
        && !realm.is_empty()
        && !realm.contains('*')
    {
        return Ok(());
    }
    let mut segments = parts.into_iter();
    let (Some(realm), Some(area), Some(resource), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return invalid("stream selector has the wrong shape");
    };
    let literal = |segment: &str| !segment.contains('*');
    if (literal(realm) || realm == "*")
        && (literal(area) || area == "*")
        && (literal(resource) || resource == "*")
    {
        return Ok(());
    }
    invalid("stream selector has the wrong shape")
}

#[must_use]
pub fn route_matches_pattern(route: &str, pattern: &str) -> bool {
    let Some((route_scheme, route_path)) = route.split_once("://") else {
        return false;
    };
    let Some((pattern_scheme, pattern_path)) = pattern.split_once("://") else {
        return false;
    };
    if route_scheme != pattern_scheme {
        return false;
    }
    if route_path.is_empty() || pattern_path.is_empty() {
        return false;
    }
    let (mut route_index, mut pattern_index) = (0, 0);
    let mut last_double_wildcard = None;
    let mut last_double_match = 0;

    while route_index < route_path.len() {
        let Some((route_segment, next_route_index)) = next_segment(route_path, route_index) else {
            return false;
        };
        match next_segment(pattern_path, pattern_index) {
            Some((segment, next_pattern_index)) if segment == "*" || segment == route_segment => {
                route_index = next_route_index;
                pattern_index = next_pattern_index;
            }
            Some(("**", next_pattern_index)) => {
                last_double_wildcard = Some(next_pattern_index);
                last_double_match = route_index;
                pattern_index = next_pattern_index;
            }
            _ => {
                let Some(after_double_wildcard) = last_double_wildcard else {
                    return false;
                };
                let Some((_, next_double_match)) = next_segment(route_path, last_double_match)
                else {
                    return false;
                };
                last_double_match = next_double_match;
                route_index = next_double_match;
                pattern_index = after_double_wildcard;
            }
        }
    }
    while let Some((segment, next_pattern_index)) = next_segment(pattern_path, pattern_index) {
        if segment != "**" {
            return false;
        }
        pattern_index = next_pattern_index;
    }
    pattern_index == pattern_path.len()
}

fn next_segment(path: &str, start: usize) -> Option<(&str, usize)> {
    if start >= path.len() {
        return None;
    }
    match path[start..].find('/') {
        Some(offset) => Some((&path[start..start + offset], start + offset + 1)),
        None => Some((&path[start..], path.len())),
    }
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

pub(crate) fn validate_response_fixed_route(
    route: &str,
    scheme: &str,
    segments: usize,
    operation: &str,
) -> Result<()> {
    validate_fixed_route(route, scheme, segments).map_err(|_| {
        FitzError::Protocol(format!(
            "{operation} response contains invalid concrete {scheme} route: {route}"
        ))
    })
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
        double_wildcard_count: 0,
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
            if double_wildcard {
                shape.double_wildcard_count += 1;
            }
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
    fn should_accept_registration_patterns_given_exact_and_whole_segment_wildcards_when_validation_runs()
     {
        // Arrange
        let patterns = [
            "queue://realm/area/resource",
            "queue://realm/area/*",
            "queue://realm/**",
            "queue://*/area/resource",
            "queue://**/resource",
            "queue://realm/**/**",
        ];

        // Act
        let results = patterns.map(|pattern| validate_registration_pattern(pattern, "queue", 3));

        // Assert
        assert!(results.iter().all(Result::is_ok));
    }

    #[test]
    fn should_reject_registration_patterns_given_invalid_shape_when_validation_runs() {
        // Arrange
        let patterns = [
            "stream://realm/area/resource",
            "queue://realm//resource",
            "queue://realm/area/res*",
            "queue://realm/area",
            "queue://realm/area/resource/extra/**",
        ];

        // Act
        let results = patterns.map(|pattern| validate_registration_pattern(pattern, "queue", 3));

        // Assert
        assert!(results.iter().all(Result::is_err));
    }

    #[test]
    fn should_match_concrete_routes_given_middle_and_repeated_double_wildcards_when_matching_runs()
    {
        // Arrange
        let cases = [
            ("rpc://acme/orders/v1/create", "rpc://*/orders/**", true),
            ("rpc://acme/orders/create", "rpc://acme/**/**", true),
            ("rpc://acme/create", "rpc://acme/**/orders", false),
            ("queue://acme/app/jobs", "stream://**", false),
        ];

        // Act
        let results = cases.map(|(route, pattern, _)| route_matches_pattern(route, pattern));

        // Assert
        assert_eq!(results, cases.map(|(_, _, expected)| expected));
    }

    #[test]
    fn should_validate_stream_selectors_given_server_grammar() {
        // Arrange
        let valid = [
            "stream://realm/area/resource",
            "stream://realm/area/*",
            "stream://realm/*/resource",
            "stream://realm/*/*",
            "stream://realm/**",
            "stream://*/area/resource",
            "stream://*/area/*",
            "stream://*/*/resource",
            "stream://*/*/*",
            "stream://**",
        ];
        let invalid = [
            "stream://realm/area/**",
            "stream://*/**",
            "stream://realm/area",
        ];

        // Act
        let valid_results = valid.map(validate_stream_selector);
        let invalid_results = invalid.map(validate_stream_selector);

        // Assert
        assert!(valid_results.iter().all(Result::is_ok));
        assert!(invalid_results.iter().all(Result::is_err));
    }
}
