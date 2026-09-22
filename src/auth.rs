use crate::error::AppError;
use axum::{
    extract::State,
    http::{HeaderMap, header},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::{Choice, ConstantTimeEq};

#[derive(Clone)]
pub struct AuthState {
    token_digests: Arc<Vec<[u8; 32]>>,
}

impl std::fmt::Debug for AuthState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthState")
            .field("token_count", &self.token_digests.len())
            .finish_non_exhaustive()
    }
}

impl AuthState {
    pub(crate) fn new(tokens: Vec<String>) -> Self {
        Self {
            token_digests: Arc::new(
                tokens
                    .into_iter()
                    .map(|token| Sha256::digest(token.as_bytes()).into())
                    .collect(),
            ),
        }
    }

    fn is_allowed(&self, candidate: &str) -> bool {
        let candidate: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
        // Evaluate every configured digest rather than revealing which token matched.
        bool::from(
            self.token_digests
                .iter()
                .fold(Choice::from(0), |matched, token| {
                    matched | token.ct_eq(&candidate)
                }),
        )
    }
}

pub async fn require_auth(
    State(auth): State<AuthState>,
    headers: HeaderMap,
    request: axum::extract::Request,
    next: Next,
) -> Result<Response, AppError> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        tracing::debug!("authorization failed: missing header");
        return Err(AppError::Unauthorized);
    };

    if values.next().is_some() {
        tracing::debug!("authorization failed: multiple authorization headers");
        return Err(AppError::Unauthorized);
    }

    let Ok(value) = value.to_str() else {
        tracing::debug!("authorization failed: header is not valid ASCII");
        return Err(AppError::Unauthorized);
    };

    let Some(token) = bearer_token(value) else {
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

fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, credential) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    // HTTP authentication permits one or more SP between scheme and credentials.
    // Only the scheme is case-insensitive; preserve the credential exactly.
    let token = credential.trim_start_matches(' ');
    (!token.is_empty()).then_some(token)
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
        assert!(!auth.is_allowed("Secret"));
    }

    #[test]
    fn bearer_scheme_is_case_insensitive_without_normalizing_credentials() {
        assert_eq!(bearer_token("Bearer secret"), Some("secret"));
        assert_eq!(bearer_token("bEaReR  Secret"), Some("Secret"));
        assert_eq!(bearer_token("bearer secret "), Some("secret "));
        for value in [
            "Basic secret",
            "Bearer",
            "Bearer ",
            "Bearer\tsecret",
            " Bearer secret",
        ] {
            assert_eq!(bearer_token(value), None);
        }
    }
}
