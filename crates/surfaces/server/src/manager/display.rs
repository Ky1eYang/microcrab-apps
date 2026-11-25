//! Creator display archive assembly; no edge into model Context or request audit.
pub(super) mod anchors;
mod checkpoint;
mod child;
pub(super) use checkpoint::CheckpointDisplay;
pub(super) mod fork;
mod graph;
mod push;
mod query;
mod selection;
use crate::config::{Config, Storage};
pub(super) use child::ChildDisplay;
use microcrab_protocol::application::WireError;
use microcrab_storage::display::{DisplayScope, DisplaySource};
pub(super) use query::query;
pub(super) use selection::{
    begin_selection, bind_failure, finish_selection, selected_attempts, unadmitted_heads,
};
use std::path::PathBuf;
fn error(message: &str) -> WireError {
    WireError::new("display_unavailable", message)
}
pub(super) fn source(
    config: &Config,
    owner: &str,
    root: &str,
    session: &str,
    directory: PathBuf,
) -> DisplaySource {
    let scope = DisplayScope::new(owner, root, session);
    match config.storage {
        Storage::File => DisplaySource::File { directory, scope },
        Storage::Sqlite => DisplaySource::Sqlite {
            database: config.data_dir.join("storage.sqlite"),
            scope,
        },
    }
}
