use crate::error::AppError;
use axum::{
    extract::State,
    http::{HeaderMap, header},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct AuthState {
    tokens: Arc<Vec<String>>,
}

impl AuthState {
    pub fn new(tokens: Vec<String>) -> Self {
        Self {
            tokens: Arc::new(tokens),
        }
    }

    fn is_allowed(&self, candidate: &str) -> bool {
        !self.tokens.is_empty() && self.tokens.iter().any(|token| token == candidate)
    }
}

pub async fn require_auth(
    State(auth): State<AuthState>,
    headers: HeaderMap,
    request: axum::extract::Request,
    next: Next,
) -> Result<Response, AppError> {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        tracing::debug!("authorization failed: missing header");
        return Err(AppError::Unauthorized);
    };

    let Ok(value) = value.to_str() else {
        tracing::debug!("authorization failed: header is not valid UTF-8");
        return Err(AppError::Unauthorized);
    };

    let Some(token) = value.strip_prefix("Bearer ") else {
        tracing::debug!("authorization failed: unsupported authorization scheme");
        return Err(AppError::Unauthorized);
    };

    if !auth.is_allowed(token) {
        tracing::debug!("authorization failed: bearer token mismatch");
        return Err(AppError::Unauthorized);
    }

    tracing::debug!("authorization succeeded");
    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_rejects_empty_token_list() {
        let auth = AuthState::new(vec![]);
        assert!(!auth.is_allowed("token"));
    }

    #[test]
    fn auth_matches_exact_token() {
        let auth = AuthState::new(vec!["secret".to_string()]);
        assert!(auth.is_allowed("secret"));
        assert!(!auth.is_allowed(" secret"));
        assert!(!auth.is_allowed("secret "));
    }
}
