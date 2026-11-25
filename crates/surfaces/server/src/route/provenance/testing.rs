//! Shared native diagnostic capture. A single process subscriber also sees real
//! upgrade tasks; per-test UUIDs isolate assertions from other route fixtures.
use std::sync::{Arc, Mutex, OnceLock};
#[derive(Clone, Default)]
pub(crate) struct Capture(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Capture {
    pub(crate) fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}
pub(crate) fn capture() -> Capture {
    static CAPTURE: OnceLock<Capture> = OnceLock::new();
    CAPTURE
        .get_or_init(|| {
            let capture = Capture::default();
            let writer = capture.clone();
            tracing_subscriber::fmt()
                .with_env_filter("microcrab_server=info")
                .with_ansi(false)
                .with_target(true)
                .with_writer(move || writer.clone())
                .init();
            capture
        })
        .clone()
}
