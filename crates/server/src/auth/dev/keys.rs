//! Fresh OS-random 2048-bit keys; no checked-in private key or key files.
use super::{AuthConfig, AuthError, URL_SAFE_NO_PAD};
use crate::auth::jwt;
use base64::Engine;
use jwt_simple::prelude::*;
use rsa::{RsaPrivateKey, pkcs1::EncodeRsaPrivateKey, rand_core::OsRng, traits::PublicKeyParts};
use serde_json::{Value, json};

pub(super) struct KeyRing {
    keys: Vec<Key>,
}
struct Key {
    kid: String,
    encoding: RS256KeyPair,
    decoding: RS256PublicKey,
    public: Value,
}
impl Key {
    fn generate() -> Result<Self, AuthError> {
        let private = RsaPrivateKey::new(&mut OsRng, 2048).map_err(|_| AuthError::Unavailable)?;
        let document = private.to_pkcs1_der().map_err(|_| AuthError::Unavailable)?;
        let n = URL_SAFE_NO_PAD.encode(private.n().to_bytes_be());
        let e = URL_SAFE_NO_PAD.encode(private.e().to_bytes_be());
        let kid = uuid::Uuid::new_v4().to_string();
        Ok(Self {
            public: json!({"kid":kid,"kty":"RSA","alg":"RS256","use":"sig","key_ops":["verify"],"n":n,"e":e}),
            kid: kid.clone(),
            encoding: RS256KeyPair::from_der(document.as_bytes())
                .map_err(|_| AuthError::Unavailable)?
                .with_key_id(&kid),
            decoding: RS256PublicKey::from_components(
                &private.n().to_bytes_be(),
                &private.e().to_bytes_be(),
            )
            .map_err(|_| AuthError::Unavailable)?,
        })
    }
}
impl KeyRing {
    pub(super) fn new() -> Result<Self, AuthError> {
        Ok(Self {
            keys: vec![Key::generate()?],
        })
    }
    #[cfg(test)]
    pub(super) fn rotate(&mut self) -> Result<(), AuthError> {
        self.install(Self::replacement()?);
        Ok(())
    }
    pub(super) fn replacement() -> Result<Self, AuthError> {
        Self::new()
    }
    pub(super) fn install(&mut self, replacement: Self) {
        self.keys.extend(replacement.keys);
        if self.keys.len() > 2 {
            self.keys.remove(0);
        }
    }
    pub(super) fn jwks(&self) -> Value {
        json!({"keys":self.keys.iter().map(|key| &key.public).collect::<Vec<_>>()})
    }
    pub(super) fn sign(&self, claims: &Value) -> Result<String, AuthError> {
        let key = self.keys.last().ok_or(AuthError::Unavailable)?;
        key.encoding
            .sign(jwt::claims(claims)?)
            .map_err(|_| AuthError::Unavailable)
    }
    pub(super) fn verify_logout(&self, token: &str, config: &AuthConfig) -> Result<(), AuthError> {
        let header = jwt::header(token).map_err(|_| AuthError::InvalidRequest)?;
        if header.alg != "RS256" {
            return Err(AuthError::InvalidRequest);
        }
        let key = self
            .keys
            .iter()
            .find(|key| Some(&key.kid) == header.kid.as_ref())
            .ok_or(AuthError::InvalidRequest)?;
        let mut options = jwt::options(token, &config.issuer, &config.client_id, 0)
            .map_err(|_| AuthError::InvalidRequest)?;
        // RP logout accepts expired ID Tokens. At epoch zero every unsigned
        // NumericDate expiry passes; accept_future skips both iat and nbf.
        // Signature, issuer and audience remain verified against this key ring.
        options.artificial_time = Some(UnixTimeStamp::from_secs(0));
        key.decoding
            .verify_token::<Value>(token, Some(options))
            .map_err(|_| AuthError::InvalidRequest)?;
        let claims: Value = jwt::payload(token).map_err(|_| AuthError::InvalidRequest)?;
        if claims.get("exp").and_then(Value::as_u64).is_none() {
            return Err(AuthError::InvalidRequest);
        }
        if claims["token_use"] != "id" || claims["azp"] != config.client_id {
            return Err(AuthError::InvalidRequest);
        }
        Ok(())
    }
}
