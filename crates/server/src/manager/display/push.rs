//! Socket readers reuse the diagnostic query's selected ancestry and scope checks.
//! Delivery and backpressure belong to the socket, outside all Hub locks.
use super::super::Manager;
use crate::auth::Principal;
use microcrab_protocol::application::WireError;
use serde_json::Value;

impl Manager {
    pub(crate) async fn display_page(
        &self,
        principal: &Principal,
        session: &str,
        params: Value,
        selection: &mut Option<super::query::Selection>,
    ) -> Result<Value, WireError> {
        if principal.expires_at <= super::super::now() / 1000 {
            return Err(WireError::new("unauthenticated", "Authentication expired"));
        }
        let request = serde_json::from_value(params).map_err(|error| {
            WireError::new("invalid_params", format!("Invalid display query: {error}"))
        })?;
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| WireError::new("capacity", "Display read capacity reached"))?;
        super::query::query_cached(
            self,
            &principal.owner,
            session,
            request,
            selection,
            Some(permit),
        )
        .await
    }
}
