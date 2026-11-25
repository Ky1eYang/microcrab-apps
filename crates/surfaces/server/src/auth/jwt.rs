//! Authentication-specific JWT wire schema and explicit verification policy.
//! Standard claims are separated by jwt-simple; custom fields remain flat on wire.
use super::{AuthError, MAX_TOKEN_BYTES, now};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jwt_simple::prelude::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::HashSet;

#[derive(Deserialize)]
pub(super) struct Header {
    pub alg: String,
    pub typ: Option<String>,
    pub kid: Option<String>,
}
pub(super) fn header(token: &str) -> Result<Header, AuthError> {
    if token.len() > MAX_TOKEN_BYTES {
        return Err(AuthError::Unauthorized);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(token.split('.').next().ok_or(AuthError::Unauthorized)?)
        .map_err(|_| AuthError::Unauthorized)?;
    serde_json::from_slice(&bytes).map_err(|_| AuthError::Unauthorized)
}

pub(super) fn claims(value: &impl Serialize) -> Result<JWTClaims<Value>, AuthError> {
    serde_json::from_value(serde_json::to_value(value).map_err(|_| AuthError::Unavailable)?)
        .map_err(|_| AuthError::Unavailable)
}

/// Decode the original signed bytes after verification, preserving audience array
/// cardinality and integer/required-field checks in the domain claim structures.
pub(super) fn payload<T: DeserializeOwned>(token: &str) -> Result<T, AuthError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token.split('.').nth(1).ok_or(AuthError::Unauthorized)?)
        .map_err(|_| AuthError::Unauthorized)?;
    serde_json::from_slice(&bytes).map_err(|_| AuthError::Unauthorized)
}

pub(super) fn options(
    token: &str,
    issuer: &str,
    audience: &str,
    skew: u64,
) -> Result<VerificationOptions, AuthError> {
    Ok(VerificationOptions {
        // Preserve the provider's existing typ handling. Cookie callers pin JWT
        // before verification; OIDC has no additional typ allowlist.
        required_signature_type: header(token)?.typ,
        allowed_issuers: Some(HashSet::from([issuer.to_owned()])),
        allowed_audiences: Some(HashSet::from([audience.to_owned()])),
        time_tolerance: Some(Duration::from_secs(skew)),
        artificial_time: Some(UnixTimeStamp::from_secs(now()?)),
        // The caller retains the existing explicit iat bound. nbf is checked
        // below because jwt-simple couples the two under accept_future.
        accept_future: true,
        max_token_length: Some(MAX_TOKEN_BYTES),
        ..Default::default()
    })
}

pub(super) fn not_before(token: &str, skew: u64) -> Result<(), AuthError> {
    let value: Value = payload(token)?;
    if let Some(nbf) = value.get("nbf") {
        let nbf = nbf.as_u64().ok_or(AuthError::Unauthorized)?;
        if nbf > now()?.saturating_add(skew) {
            return Err(AuthError::Unauthorized);
        }
    }
    Ok(())
}

pub(super) fn sign_hs256(value: &impl Serialize, secret: &[u8]) -> Result<String, AuthError> {
    HS256Key::from_bytes(secret)
        .authenticate(claims(value)?)
        .map_err(|_| AuthError::Unavailable)
}

#[derive(Deserialize)]
pub(super) struct JwkSet {
    pub keys: Vec<Jwk>,
}
#[derive(Deserialize)]
pub(super) struct Jwk {
    pub kid: Option<String>,
    kty: String,
    alg: Option<String>,
    #[serde(rename = "use")]
    usage: Option<String>,
    key_ops: Option<Vec<String>>,
    n: Option<String>,
    e: Option<String>,
}
impl Jwk {
    pub fn decoding_key(&self) -> Result<RS256PublicKey, AuthError> {
        if self.kty != "RSA"
            || self.alg.as_deref().is_some_and(|alg| alg != "RS256")
            || self.usage.as_deref().is_some_and(|usage| usage != "sig")
            || self
                .key_ops
                .as_ref()
                .is_some_and(|ops| !ops.iter().any(|op| op == "verify"))
        {
            return Err(AuthError::Unauthorized);
        }
        let n = URL_SAFE_NO_PAD
            .decode(self.n.as_ref().ok_or(AuthError::Unauthorized)?)
            .map_err(|_| AuthError::Unauthorized)?;
        let e = URL_SAFE_NO_PAD
            .decode(self.e.as_ref().ok_or(AuthError::Unauthorized)?)
            .map_err(|_| AuthError::Unauthorized)?;
        RS256PublicKey::from_components(&n, &e).map_err(|_| AuthError::Unauthorized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    include!("tests/idp_test_key.rs");

    #[test]
    fn jwks_constraints_remain_pinned_to_signature_verification_with_rs256() {
        let components = RS256KeyPair::from_pem(TEST_KEY)
            .unwrap()
            .public_key()
            .to_components();
        let valid = json!({"kid":"test-key", "kty":"RSA", "alg":"RS256", "use":"sig",
            "key_ops":["verify"], "n":URL_SAFE_NO_PAD.encode(components.n),
            "e":URL_SAFE_NO_PAD.encode(components.e)});
        let key: Jwk = serde_json::from_value(valid.clone()).unwrap();
        assert!(key.decoding_key().is_ok());
        for (field, value) in [
            ("kty", json!("oct")),
            ("alg", json!("HS256")),
            ("alg", json!("RS384")),
            ("use", json!("enc")),
            ("key_ops", json!(["sign"])),
            ("key_ops", json!([])),
            ("n", json!("!")),
            ("e", json!("!")),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            let key: Jwk = serde_json::from_value(invalid).unwrap();
            assert!(key.decoding_key().is_err());
        }
        let mut optional = valid;
        for field in ["alg", "use", "key_ops"] {
            optional.as_object_mut().unwrap().remove(field);
        }
        assert!(
            serde_json::from_value::<Jwk>(optional)
                .unwrap()
                .decoding_key()
                .is_ok()
        );
    }
}
