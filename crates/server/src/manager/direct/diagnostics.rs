//! Diagnostic witnesses never participate in route or lease authorization.
//! Authenticated peers can still supply arbitrary strings; persistence accepts
//! only the native bridge's UUID instance and bounded decimal lease vocabulary.
#[derive(Clone)]
pub(in crate::manager) struct Correlation {
    project: String,
    socket: String,
    generation: u64,
    lease: String,
    bridge: String,
}
#[derive(Clone, Copy)]
pub(super) enum StopReason {
    StopCancelled,
    ReaderEof,
    ReaderErr,
    WriterErr,
    HealthUnhealthy,
}
impl StopReason {
    fn label(self) -> &'static str {
        match self {
            Self::StopCancelled => "stop_cancelled",
            Self::ReaderEof => "reader_eof",
            Self::ReaderErr => "reader_err",
            Self::WriterErr => "writer_err",
            Self::HealthUnhealthy => "health_unhealthy",
        }
    }
}
impl Correlation {
    pub(in crate::manager) fn new(
        project: &str,
        socket: &str,
        generation: u64,
        lease: &str,
        bridge: &str,
    ) -> Self {
        Self {
            project: project.into(),
            socket: socket.into(),
            generation,
            lease: lease.into(),
            bridge: bridge.into(),
        }
    }
    pub(in crate::manager) fn lease(&self) -> &str {
        if !self.lease.is_empty()
            && self.lease.len() <= 20
            && self.lease.bytes().all(|byte| byte.is_ascii_digit())
        {
            &self.lease
        } else {
            "invalid"
        }
    }
    fn bridge(&self) -> Option<uuid::Uuid> {
        uuid::Uuid::parse_str(&self.bridge).ok()
    }
    pub(super) fn connect(&self, mode: &'static str, has_token: bool, force: bool) {
        tracing::info!(event = "host.connect", mode, project_id = %self.project,
            socket_id = %self.socket, connection_generation = self.generation,
            lease_generation = self.lease(), bridge_instance_id = ?self.bridge(),
            has_token, force, "Native Host connection selected");
    }
    pub(super) fn relay_attach(&self) {
        tracing::info!(event = "host.relay_attach", project_id = %self.project,
            socket_id = %self.socket, connection_generation = self.generation,
            lease_generation = self.lease(), bridge_instance_id = ?self.bridge(),
            "Native Host relay authenticated");
    }
    pub(super) fn stopped(&self, reason: StopReason, cause: &'static str) {
        tracing::info!(event = "host.worker_stopped", project_id = %self.project,
            socket_id = %self.socket, connection_generation = self.generation,
            lease_generation = self.lease(), bridge_instance_id = ?self.bridge(),
            stop_reason = reason.label(), cause, "Native Host worker stopped");
    }
    pub(in crate::manager) fn route_changed(
        &self,
        action: &'static str,
        code: Option<&microcrab_host_protocol::ErrorCode>,
        reason: &'static str,
    ) {
        tracing::info!(event = "host.route_changed", action, project_id = %self.project,
            socket_id = %self.socket, connection_generation = self.generation,
            lease_generation = self.lease(), bridge_instance_id = ?self.bridge(),
            error_code = ?code, reason, "Native Host route changed");
    }
}
