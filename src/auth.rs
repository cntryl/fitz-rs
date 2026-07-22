//! Authentication - JWT token generation for testing

#![allow(dead_code)]

use crate::error::{FitzError, Result};
use jsonwebtoken::{EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
pub struct FitzClaims {
    pub permissions: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestTokenClaims {
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub tid: String,
    pub exp: u64,
    pub iat: u64,
    pub fitz: FitzClaims,
}

pub struct TestTokenGenerator {
    secret: String,
}

impl TestTokenGenerator {
    pub fn new(secret: &str) -> Self {
        Self {
            secret: secret.to_string(),
        }
    }

    pub fn generate(&self, realm: &str, user: &str) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| FitzError::AuthFailed("System time error".into()))?
            .as_secs();

        let exp = now + 3600; // 1 hour

        let claims = TestTokenClaims {
            iss: String::new(),
            aud: "fitz".to_string(),
            sub: user.to_string(),
            tid: realm.to_string(),
            exp,
            iat: now,
            fitz: FitzClaims {
                permissions: vec![
                    "kv://**#*".to_string(),
                    "queue://**#*".to_string(),
                    "rpc://**#*".to_string(),
                    "notice://**#*".to_string(),
                    "lease://**#*".to_string(),
                    "stream://**#*".to_string(),
                    "schedule://**#*".to_string(),
                ],
            },
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|e| FitzError::AuthFailed(format!("JWT encode failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_generate_test_token() {
        let generator = TestTokenGenerator::new("test-secret");
        let token = generator.generate("test-realm", "test-user").unwrap();
        assert!(!token.is_empty());
    }

    #[test]
    fn should_create_valid_jwt_structure() {
        let generator = TestTokenGenerator::new("test-secret");
        let token = generator.generate("test-realm", "test-user").unwrap();

        // JWT is 3 parts separated by dots
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
    }
}
