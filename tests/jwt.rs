#![allow(dead_code)]

use jsonwebtoken::{EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
struct TestTokenClaims {
    iss: String,
    aud: String,
    sub: String,
    tid: String,
    exp: u64,
    iat: u64,
    permissions: Vec<String>,
}

pub fn make_test_jwt(realm: &str, secret: &str) -> String {
    make_signed_jwt(realm, secret)
}

pub fn make_invalid_jwt(realm: &str, secret: &str) -> String {
    make_signed_jwt(realm, &format!("{secret}-invalid"))
}

fn make_signed_jwt(_realm: &str, secret: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_secs();
    let audience = std::env::var("FITZ_BROKER_JWT_AUDIENCE").unwrap_or_else(|_| "fitz".to_string());

    let claims = TestTokenClaims {
        iss: String::new(),
        aud: audience,
        sub: "fitz-rs-tests".to_string(),
        tid: std::env::var("FITZ_BROKER_JWT_TENANT").unwrap_or_else(|_| "dev".to_string()),
        exp: now + 3600,
        iat: now,
        permissions: vec![
            "kv://**#*".to_string(),
            "queue://**#*".to_string(),
            "rpc://**#*".to_string(),
            "notice://**#*".to_string(),
            "lease://**#*".to_string(),
            "stream://**#*".to_string(),
            "schedule://**#*".to_string(),
        ],
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("JWT encode failed")
}
