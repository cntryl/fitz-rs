use crate::error::{FitzError, Result};

pub fn validate_concrete_route(route: &str, scheme: &str) -> Result<()> {
    let segments = parse_route_path(route, scheme)?;
    if segments.iter().any(|segment| *segment == "*" || *segment == "**") {
        return Err(FitzError::DomainError(format!(
            "{scheme} route {route:?} must not contain wildcards"
        )));
    }

    Ok(())
}

pub fn validate_fixed_route(route: &str, scheme: &str, segment_count: usize) -> Result<()> {
    let segments = parse_route_path(route, scheme)?;
    if segments.len() != segment_count {
        return Err(FitzError::DomainError(format!(
            "{scheme} route {route:?} must be {}",
            route_shape(scheme, segment_count)
        )));
    }

    if segments.iter().any(|segment| *segment == "*" || *segment == "**") {
        return Err(FitzError::DomainError(format!(
            "{scheme} route {route:?} must not contain wildcards"
        )));
    }

    Ok(())
}

pub fn validate_selector_route(
    route: &str,
    scheme: &str,
    segment_count: usize,
    allow_realm_wildcard: bool,
) -> Result<()> {
    let segments = parse_route_path(route, scheme)?;

    if segments.len() == segment_count {
        if segments_are_concrete(&segments) {
            return Ok(());
        }

        if segments[segment_count - 1] == "*" && segments[..segment_count - 1].iter().all(|segment| segment != "*" && segment != "**") {
            return Ok(());
        }
    }

    if allow_realm_wildcard && segments.len() == 3 && segments[0] != "*" && segments[0] != "**" && segments[1] == "*" && segments[2] == "*" {
        return Ok(());
    }

    Err(FitzError::DomainError(format!(
        "{scheme} route {route:?} must be one of {}",
        selector_route_shapes(scheme, segment_count, allow_realm_wildcard)
    )))
}

fn parse_route_path(route: &str, scheme: &str) -> Result<Vec<String>> {
    if route.is_empty() {
        return Err(FitzError::DomainError(format!(
            "{scheme} route must be non-empty"
        )));
    }

    let prefix = format!("{scheme}://");
    let remainder = route.strip_prefix(&prefix).ok_or_else(|| {
        FitzError::DomainError(format!("{scheme} route {route:?} must start with {prefix}"))
    })?;

    if remainder.is_empty() {
        return Err(FitzError::DomainError(format!(
            "{scheme} route {route:?} segments must be non-empty"
        )));
    }

    let mut segments = Vec::new();
    for segment in remainder.split('/') {
        if segment.is_empty() {
            return Err(FitzError::DomainError(format!(
                "{scheme} route {route:?} segments must be non-empty"
            )));
        }
        segments.push(segment.to_string());
    }

    Ok(segments)
}

fn segments_are_concrete(segments: &[String]) -> bool {
    segments.iter().all(|segment| segment != "*" && segment != "**")
}

fn route_shape(scheme: &str, segment_count: usize) -> String {
    let mut parts = Vec::with_capacity(segment_count);
    for index in 0..segment_count {
        parts.push(match index {
            0 => "{realm}",
            1 => "{area}",
            2 => "{resource}",
            3 => "{operation}",
            _ => "{segment}",
        });
    }

    format!("{scheme}://{}", parts.join("/"))
}

fn selector_route_shapes(scheme: &str, segment_count: usize, allow_realm_wildcard: bool) -> String {
    let exact = route_shape(scheme, segment_count);
    if segment_count == 3 {
        if allow_realm_wildcard {
            return format!(
                "{exact}, {scheme}://{{realm}}/{{area}}/*, or {scheme}://{{realm}}/*/*"
            );
        }

        return format!("{exact} or {scheme}://{{realm}}/{{area}}/*");
    }

    if allow_realm_wildcard {
        return format!("{exact} or {scheme}://{{realm}}/*/*");
    }

    exact
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_validate_concrete_route_with_variable_depth() {
        validate_concrete_route("rpc://realm/app/jobs/process", "rpc").unwrap();
    }

    #[test]
    fn should_reject_concrete_route_with_wildcard() {
        let err = validate_concrete_route("rpc://realm/app/*", "rpc").unwrap_err();
        assert!(err.to_string().contains("must not contain wildcards"));
    }

    #[test]
    fn should_validate_fixed_route() {
        validate_fixed_route("lease://realm/area/resource", "lease", 3).unwrap();
    }

    #[test]
    fn should_reject_fixed_route_with_wrong_shape() {
        let err = validate_fixed_route("lease://realm/area/*", "lease", 3).unwrap_err();
        assert!(err.to_string().contains("must not contain wildcards"));
    }

    #[test]
    fn should_validate_selector_route_with_realm_wildcard() {
        validate_selector_route("notice://realm/*/*", "notice", 3, true).unwrap();
    }

    #[test]
    fn should_reject_selector_route_with_unsupported_wildcard() {
        let err = validate_selector_route("queue://realm/*/*", "queue", 3, false).unwrap_err();
        assert!(err.to_string().contains("must be one of"));
    }
}
