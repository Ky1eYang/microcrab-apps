//! Application JWTs are distinct from IdP tokens and expire during socket use.
use super::{Auth, AuthError, MAX_REVOKED, SESSION_COOKIE, cookie, now, oauth::Identity};
use axum::http::HeaderMap;
use jwt_simple::prelude::{HS256Key, MACLike};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Serialize)]
pub struct Principal {
    pub owner: String,
    pub subject: String,
    pub display_name: String,
    pub expires_at: u64,
    #[serde(skip_serializing)]
    pub jti: String,
    #[serde(skip_serializing)]
    generation: String,
}
#[derive(Serialize, Deserialize)]
struct Claims {
    iss: String,
    aud: String,
    sub: String,
    provider_issuer: String,
    owner: String,
    name: String,
    iat: u64,
    nbf: u64,
    exp: u64,
    jti: String,
    generation: String,
}
impl Auth {
    pub fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let token = cookie(headers, SESSION_COOKIE)?.ok_or(AuthError::Unauthorized)?;
        let header = super::jwt::header(&token)?;
        if header.alg != "HS256" || header.typ.as_deref() != Some("JWT") {
            return Err(AuthError::Unauthorized);
        }
        let config = &self.inner.config;
        HS256Key::from_bytes(config.jwt_secret.as_bytes())
            .verify_token::<serde_json::Value>(
                &token,
                Some(super::jwt::options(
                    &token,
                    &config.jwt_issuer,
                    &config.jwt_audience,
                    0,
                )?),
            )
            .map_err(|_| AuthError::Unauthorized)?;
        super::jwt::not_before(&token, 0)?;
        let claims: Claims = super::jwt::payload(&token)?;
        let current = now()?;
        if claims.provider_issuer != config.issuer
            || claims.generation != self.inner.generation
            || claims.sub.is_empty()
            || claims.sub.len() > 1024
            || claims.jti.len() > 128
            || claims.name.len() > 256
            || claims.iat > current
            || claims.nbf != claims.iat
            || claims.exp <= claims.iat
            || claims.exp - claims.iat > config.session_ttl_seconds
            || claims.owner != owner(&config.issuer, &claims.sub)?
        {
            return Err(AuthError::Unauthorized);
        }
        let principal = Principal {
            owner: claims.owner,
            subject: claims.sub,
            display_name: claims.name,
            expires_at: claims.exp,
            jti: claims.jti,
            generation: claims.generation,
        };
        self.check_active(&principal)?;
        Ok(principal)
    }
    /// Call on each command and before outbound socket delivery, plus expiry timer.
    /// A restart requires reauthentication rather than resurrecting revoked cookies.
    pub fn check_active(&self, principal: &Principal) -> Result<(), AuthError> {
        let current = now()?;
        if principal.expires_at <= current || principal.generation != self.inner.generation {
            return Err(AuthError::Unauthorized);
        }
        let mut revoked = self
            .inner
            .revoked
            .lock()
            .map_err(|_| AuthError::Unavailable)?;
        revoked.retain(|_, expires| *expires > current);
        if revoked.contains_key(&principal.jti) {
            return Err(AuthError::Unauthorized);
        }
        Ok(())
    }
    pub(super) fn revoke(&self, principal: &Principal) -> Result<(), AuthError> {
        self.check_active(principal)?;
        let mut revoked = self
            .inner
            .revoked
            .lock()
            .map_err(|_| AuthError::Unavailable)?;
        if revoked.len() >= MAX_REVOKED {
            return Err(AuthError::Unavailable);
        }
        revoked.insert(principal.jti.clone(), principal.expires_at);
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn issue(&self, subject: String, display_name: String) -> Result<String, AuthError> {
        self.issue_claims(subject, display_name)
            .map(|(token, _)| token)
    }
    /// Issue the application cookie for a verified IdP identity and keep the ID
    /// Token server-side (bounded, expiring with the cookie) for logout hints.
    pub(super) fn issue_identity(&self, identity: Identity) -> Result<String, AuthError> {
        let (token, claims) = self.issue_claims(identity.subject, identity.display_name)?;
        let current = now()?;
        let mut tokens = self
            .inner
            .id_tokens
            .lock()
            .map_err(|_| AuthError::Unavailable)?;
        tokens.retain(|_, (_, expires)| *expires > current);
        // Capacity exhaustion only degrades logout to a client_id-based IdP
        // logout; it never blocks a valid login.
        if tokens.len() < MAX_REVOKED {
            tokens.insert(claims.jti, (identity.id_token, claims.exp));
        }
        Ok(token)
    }
    /// Remove and return the stored ID Token for a session being logged out.
    pub(super) fn take_id_token(&self, principal: &Principal) -> Option<String> {
        let mut tokens = self.inner.id_tokens.lock().ok()?;
        tokens.remove(&principal.jti).map(|(token, _)| token)
    }
    fn issue_claims(
        &self,
        subject: String,
        display_name: String,
    ) -> Result<(String, Claims), AuthError> {
        if subject.is_empty()
            || subject.len() > 1024
            || display_name.len() > 256
            || display_name.chars().any(char::is_control)
        {
            return Err(AuthError::Unauthorized);
        }
        let current = now()?;
        let config = &self.inner.config;
        let claims = Claims {
            iss: config.jwt_issuer.clone(),
            aud: config.jwt_audience.clone(),
            provider_issuer: config.issuer.clone(),
            owner: owner(&config.issuer, &subject)?,
            sub: subject,
            name: display_name,
            iat: current,
            nbf: current,
            exp: current
                .checked_add(config.session_ttl_seconds)
                .ok_or(AuthError::Unavailable)?,
            jti: uuid::Uuid::new_v4().to_string(),
            generation: self.inner.generation.clone(),
        };
        let token = super::jwt::sign_hs256(&claims, config.jwt_secret.as_bytes())?;
        Ok((token, claims))
    }
}
fn owner(issuer: &str, subject: &str) -> Result<String, AuthError> {
    let identity = serde_json::to_vec(&("microcrab-owner-v1", issuer, subject))
        .map_err(|_| AuthError::Unavailable)?;
    Ok(format!("{:x}", Sha256::digest(&identity)))
}

#[cfg(test)]
impl Principal {
    pub(crate) fn for_test(owner: &str) -> Self {
        Self {
            owner: owner.into(),
            subject: owner.into(),
            display_name: owner.into(),
            expires_at: now().unwrap() + 3600,
            jti: "test".into(),
            generation: "test".into(),
        }
    }
}

#[cfg(test)]
impl Auth {
    pub(crate) fn login_for_test(&self, owner: &str) -> String {
        self.issue(owner.into(), owner.into()).unwrap()
    }
}
