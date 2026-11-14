//! HTTP exceptions for the browser OAuth redirect flow; business APIs remain WS.
use super::{
    Auth, AuthError, LOGIN_COOKIE, MAX_PENDING, PendingLogin, SESSION_COOKIE, cookie, now,
    same_secret,
};
use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use oauth2::{CsrfToken, PkceCodeChallenge, Scope};
use serde::Deserialize;
use serde_json::json;

pub fn router(auth: Auth) -> Router {
    Router::new()
        .route("/api/auth/login", get(login))
        .route("/api/auth/callback", get(callback))
        .route("/api/auth/logout", post(logout))
        .route("/api/auth/session", get(session))
        .with_state(auth)
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginQuery {
    login_hint: Option<String>,
}
#[derive(Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    /// RFC 9207 authorization response issuer identifier.
    iss: Option<String>,
}
/// OAuth/OIDC error codes that may be echoed to the login page. Anything else
/// (including `error_description`) is collapsed so IdP text never reaches HTML.
const DISPLAYABLE_ERRORS: &[&str] = &[
    "access_denied",
    "login_required",
    "consent_required",
    "interaction_required",
    "account_selection_required",
    "invalid_request",
    "invalid_scope",
    "unauthorized_client",
    "unsupported_response_type",
    "server_error",
    "temporarily_unavailable",
];
async fn login(
    State(auth): State<Auth>,
    query: Result<Query<LoginQuery>, QueryRejection>,
) -> Result<Response, AuthError> {
    let Query(query) = query.map_err(|_| AuthError::InvalidRequest)?;
    if query
        .login_hint
        .as_ref()
        .is_some_and(|hint| hint.len() > 256 || hint.chars().any(char::is_control))
    {
        return Err(AuthError::InvalidRequest);
    }
    let client = auth.oauth_client()?;
    let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
    let nonce = CsrfToken::new_random().secret().to_owned();
    let binding = CsrfToken::new_random().secret().to_owned();
    let mut request = client
        .authorize_url(CsrfToken::new_random)
        .add_scopes(auth.inner.config.scopes.iter().cloned().map(Scope::new))
        .set_pkce_challenge(challenge)
        .add_extra_param("nonce", nonce.clone());
    if let Some(hint) = query.login_hint {
        request = request.add_extra_param("login_hint", hint);
    }
    let (url, state) = request.url();
    let current = now()?;
    {
        let mut pending = auth
            .inner
            .pending
            .lock()
            .map_err(|_| AuthError::Unavailable)?;
        pending.retain(|_, login| login.expires > current);
        if pending.len() >= MAX_PENDING {
            return Err(AuthError::Unavailable);
        }
        pending.insert(
            state.secret().to_owned(),
            PendingLogin {
                verifier: verifier.secret().to_owned(),
                nonce,
                binding: binding.clone(),
                expires: current.checked_add(300).ok_or(AuthError::Unavailable)?,
            },
        );
    }
    let mut response = Redirect::temporary(url.as_str()).into_response();
    add_cookie(
        &mut response,
        &auth,
        LOGIN_COOKIE,
        &binding,
        "/api/auth",
        300,
    )?;
    secure_response(&mut response);
    Ok(response)
}
async fn callback(
    State(auth): State<Auth>,
    headers: HeaderMap,
    query: Result<Query<CallbackQuery>, QueryRejection>,
) -> Result<Response, AuthError> {
    let Query(query) = query.map_err(|_| AuthError::InvalidRequest)?;
    // Mix-up defence (RFC 9207): a present issuer must match exactly, also on
    // error responses; providers advertising support must send it.
    match &query.iss {
        Some(iss) if *iss != auth.inner.config.issuer => {
            return Err(AuthError::UnauthorizedReason(
                "authentication callback issuer does not match configured issuer",
            ));
        }
        None if auth.inner.endpoints.require_callback_iss => {
            return Err(AuthError::UnauthorizedReason(
                "authentication callback is missing required issuer",
            ));
        }
        _ => {}
    }
    // Both success and provider errors must belong to this browser's live login.
    // Reject ambiguous responses before consuming state or changing cookies.
    if query.code.is_some() == query.error.is_some()
        || query
            .code
            .as_ref()
            .is_some_and(|code| code.is_empty() || code.len() > 4096)
    {
        return Err(AuthError::Unauthorized);
    }
    let state = query.state.ok_or(AuthError::UnauthorizedReason(
        "authentication callback is missing state",
    ))?;
    if state.is_empty() || state.len() > 512 {
        return Err(AuthError::Unauthorized);
    }
    let binding = cookie(&headers, LOGIN_COOKIE)?.ok_or(AuthError::UnauthorizedReason(
        "authentication login cookie is missing or expired",
    ))?;
    let pending = {
        let current = now()?;
        let mut logins = auth
            .inner
            .pending
            .lock()
            .map_err(|_| AuthError::Unavailable)?;
        let login = logins.get(&state).ok_or(AuthError::UnauthorizedReason(
            "authentication state is unknown or expired",
        ))?;
        if login.expires <= current {
            return Err(AuthError::UnauthorizedReason(
                "authentication login state expired",
            ));
        }
        if !same_secret(&binding, &login.binding) {
            return Err(AuthError::UnauthorizedReason(
                "authentication login cookie does not match state binding",
            ));
        }
        // Consume success and error callbacks once under the same lock, before
        // exchanging a code or redirecting. Concurrent replays cannot both win.
        logins.remove(&state).ok_or(AuthError::Unauthorized)?
    };
    if let Some(error) = &query.error {
        let reason = if DISPLAYABLE_ERRORS.contains(&error.as_str()) {
            error.as_str()
        } else {
            "authentication_failed"
        };
        let mut response = Redirect::to(&format!("/login?auth_error={reason}")).into_response();
        add_cookie(&mut response, &auth, LOGIN_COOKIE, "", "/api/auth", 0)?;
        secure_response(&mut response);
        return Ok(response);
    }
    let code = query.code.ok_or(AuthError::Unauthorized)?;
    // Admission: any identity whose ID Token verifies is a user; isolation is
    // by owner = H(issuer, sub) downstream, not by an allowlist here.
    let identity = auth
        .exchange(code, pending.verifier, &pending.nonce)
        .await?;
    let token = auth.issue_identity(identity)?;
    let mut response = Redirect::to(&auth.inner.config.post_login_redirect).into_response();
    add_cookie(
        &mut response,
        &auth,
        SESSION_COOKIE,
        &token,
        "/",
        auth.inner.config.session_ttl_seconds,
    )?;
    add_cookie(&mut response, &auth, LOGIN_COOKIE, "", "/api/auth", 0)?;
    secure_response(&mut response);
    Ok(response)
}
async fn logout(State(auth): State<Auth>, headers: HeaderMap) -> Result<Response, AuthError> {
    auth.check_origin(&headers)?;
    // Missing/expired credentials still allow cookie cleanup, but a live token
    // is revoked before reporting success so existing sockets stop being valid.
    let id_token = match auth.authenticate(&headers) {
        Ok(principal) => {
            auth.revoke(&principal)?;
            auth.take_id_token(&principal)
        }
        Err(AuthError::Unauthorized | AuthError::UnauthorizedReason(_)) => None,
        Err(error) => return Err(error),
    };
    // RP-Initiated Logout: the browser must visit the IdP so its SSO session
    // cookie is cleared; otherwise the next login silently signs back in.
    let mut response = match end_session_url(&auth, id_token.as_deref())? {
        Some(redirect) => Json(json!({ "redirect": redirect })).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    };
    add_cookie(&mut response, &auth, SESSION_COOKIE, "", "/", 0)?;
    add_cookie(&mut response, &auth, LOGIN_COOKIE, "", "/api/auth", 0)?;
    secure_response(&mut response);
    Ok(response)
}
async fn session(State(auth): State<Auth>, headers: HeaderMap) -> Result<Response, AuthError> {
    let principal = auth.authenticate(&headers)?;
    let mut response = Json(json!({"authenticated":true,"principal":principal})).into_response();
    secure_response(&mut response);
    Ok(response)
}
/// Build `end_session_endpoint?id_token_hint&post_logout_redirect_uri&client_id`.
/// Without a hint (expired local session) AuthPortal still ends its own session
/// and validates the return URI through `client_id`.
pub(super) fn end_session_url(
    auth: &Auth,
    id_token: Option<&str>,
) -> Result<Option<String>, AuthError> {
    let Some(endpoint) = &auth.inner.endpoints.end_session else {
        return Ok(None);
    };
    let config = &auth.inner.config;
    let mut url = url::Url::parse(endpoint).map_err(|_| AuthError::Configuration)?;
    {
        let mut query = url.query_pairs_mut();
        if let Some(token) = id_token {
            query.append_pair("id_token_hint", token);
        }
        if let Some(target) = &config.post_logout_redirect_uri {
            query.append_pair("post_logout_redirect_uri", target);
        }
        query.append_pair("client_id", &config.client_id);
    }
    Ok(Some(url.into()))
}
fn add_cookie(
    response: &mut Response,
    auth: &Auth,
    name: &str,
    value: &str,
    path: &str,
    lifetime: u64,
) -> Result<(), AuthError> {
    let secure = if auth.inner.config.cookie_secure {
        "; Secure"
    } else {
        ""
    };
    let value =
        format!("{name}={value}; Path={path}; Max-Age={lifetime}; HttpOnly; SameSite=Lax{secure}");
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&value).map_err(|_| AuthError::Unavailable)?,
    );
    Ok(())
}
fn secure_response(response: &mut Response) {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
}
