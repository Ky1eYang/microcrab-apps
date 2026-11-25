//! Prepare a Session with caller-supplied Core wiring and history.

use microcrab_session::{Error, Recover, Session, SessionBuilder};

/// The Core construction operation selected by the caller.
pub enum Preparation<'a> {
    Create,
    Restore(Box<Recover>),
    Fork(&'a Session),
}

/// Construct a Session without starting it. The caller owns mode selection,
/// configuration, publication, and when to call `Session::start`.
pub async fn prepare(
    builder: SessionBuilder,
    preparation: Preparation<'_>,
) -> Result<Session, Error> {
    match preparation {
        Preparation::Create => builder.build(),
        Preparation::Restore(recover) => Session::restore(builder, *recover).await,
        Preparation::Fork(source) => source.fork(builder).await,
    }
}
