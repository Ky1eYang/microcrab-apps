use super::{CAPACITY, Event, RETENTION_MS};
use crate::config::{Config, Storage};
use microcrab_session::Error;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const QUEUE_SIZE: usize = 1024;
const WRITE_BATCH_SIZE: usize = 128;
const PRUNE_INTERVAL: Duration = Duration::from_secs(3600);
const MAX_FILE_BYTES: u64 = CAPACITY as u64 * 4096;
const BASENAME: &str = "usage-events.jsonl";

enum Command {
    Record(String, Event),
    Read(String, oneshot::Sender<Result<Snapshot, Error>>),
    Close,
}

pub(super) struct Snapshot {
    pub rows: Vec<Event>,
    pub observed_since_ms: u64,
    pub capacity_reached: bool,
}

#[derive(Clone)]
pub(crate) struct Dashboard(Arc<Worker>);
struct Worker {
    commands: mpsc::SyncSender<Command>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl Dashboard {
    pub(in crate::manager) async fn open(config: &Config) -> Result<Self, Error> {
        let data_dir = config.data_dir.clone();
        let backend = config.storage;
        tokio::task::spawn_blocking(move || {
            let store = match backend {
                Storage::File => Store::File(data_dir.join("storage").join("owners")),
                Storage::Sqlite => {
                    #[cfg(feature = "storage-sqlite")]
                    {
                        Store::Sqlite(open_sqlite(&data_dir.join("storage.sqlite"))?)
                    }
                    #[cfg(not(feature = "storage-sqlite"))]
                    {
                        return Err(Error::config("SQLite support is not compiled"));
                    }
                }
            };
            Self::start(store, &data_dir.join("dashboard-activation.json"))
        })
        .await
        .map_err(|error| {
            Error::audit_with_private(
                "Dashboard storage initialization worker failed",
                error.to_string(),
            )
        })?
    }

    fn start(mut store: Store, activation_file: &Path) -> Result<Self, Error> {
        #[derive(Deserialize)]
        struct Activation {
            observed_since_ms: u64,
        }
        let observed_since_ms = match File::open(activation_file) {
            Ok(file) => serde_json::from_reader::<_, Activation>(file)?.observed_since_ms,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let ms = super::super::now();
                atomic_write(activation_file, |writer| {
                    serde_json::to_writer(writer, &serde_json::json!({"observed_since_ms": ms}))?;
                    Ok(())
                })?;
                ms
            }
            Err(error) => return Err(error.into()),
        };
        store.prune(super::super::now().saturating_sub(RETENTION_MS))?;
        let (commands, receiver) = mpsc::sync_channel(QUEUE_SIZE);
        let join = std::thread::Builder::new()
            .name("microcrab-dashboard".into())
            .spawn(move || {
                let mut prune_at = Instant::now() + PRUNE_INTERVAL;
                let mut deferred = None;
                loop {
                    if Instant::now() >= prune_at {
                        if store
                            .prune(super::super::now().saturating_sub(RETENTION_MS))
                            .is_err()
                        {
                            tracing::warn!("Dashboard retention cleanup failed");
                        }
                        prune_at = Instant::now() + PRUNE_INTERVAL;
                    }
                    let command = match deferred.take() {
                        Some(command) => command,
                        None => match receiver
                            .recv_timeout(prune_at.saturating_duration_since(Instant::now()))
                        {
                            Ok(command) => command,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        },
                    };
                    let cutoff = super::super::now().saturating_sub(RETENTION_MS);
                    match command {
                        Command::Record(owner, event) => {
                            let mut batch = BTreeMap::new();
                            batch.insert(owner, vec![event]);
                            for _ in 1..WRITE_BATCH_SIZE {
                                match receiver.try_recv() {
                                    Ok(Command::Record(owner, event)) => {
                                        batch.entry(owner).or_default().push(event)
                                    }
                                    Ok(command) => {
                                        deferred = Some(command);
                                        break;
                                    }
                                    Err(_) => break,
                                }
                            }
                            for (owner, rows) in batch {
                                if store.record(&owner, rows, cutoff).is_err() {
                                    tracing::warn!("Dashboard event projection write failed");
                                }
                            }
                        }
                        Command::Read(owner, reply) => {
                            let result = store.read(&owner, cutoff).map(|rows| Snapshot {
                                capacity_reached: rows.len() >= CAPACITY,
                                rows,
                                observed_since_ms,
                            });
                            let _ = reply.send(result);
                        }
                        Command::Close => break,
                    }
                }
            })?;
        Ok(Self(Arc::new(Worker {
            commands,
            join: Mutex::new(Some(join)),
        })))
    }

    /// Provider completion never waits for projection I/O or exposes private data
    /// in diagnostics. The read API calls out that collection is not authoritative.
    pub(super) fn record(&self, owner: String, event: Event) {
        if self
            .0
            .commands
            .try_send(Command::Record(owner, event))
            .is_err()
        {
            tracing::warn!(
                "Dashboard event dropped because the bounded projection worker could not admit it"
            );
        }
    }

    pub(super) async fn snapshot(&self, owner: &str) -> Result<Snapshot, Error> {
        let (reply, response) = oneshot::channel();
        self.0
            .commands
            .try_send(Command::Read(super::owner_key(owner), reply))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => Error::Busy,
                mpsc::TrySendError::Disconnected(_) => {
                    Error::audit("Dashboard projection worker unavailable")
                }
            })?;
        response
            .await
            .map_err(|_| Error::audit("Dashboard projection read was not acknowledged"))?
    }

    pub(in crate::manager) async fn close(&self) -> Result<(), Error> {
        let worker = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let join = worker
                .join
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            if let Some(join) = join {
                let _ = worker.commands.send(Command::Close);
                join.join()
                    .map_err(|_| Error::audit("Dashboard projection worker panicked"))?;
            }
            Ok(())
        })
        .await
        .map_err(|error| {
            Error::audit_with_private("Dashboard projection stop failed", error.to_string())
        })?
    }
}

enum Store {
    File(PathBuf),
    #[cfg(feature = "storage-sqlite")]
    Sqlite(rusqlite::Connection),
}
impl Store {
    fn read(&self, owner: &str, cutoff: u64) -> Result<Vec<Event>, Error> {
        match self {
            Self::File(root) => read_file(&path(root, owner), cutoff),
            #[cfg(feature = "storage-sqlite")]
            Self::Sqlite(db) => {
                let mut statement = db.prepare("SELECT event FROM dashboard_usage_events WHERE owner = ?1 AND ts_ms >= ?2 ORDER BY ts_ms DESC, event_id DESC LIMIT ?3").map_err(sqlite_error)?;
                let bytes = statement
                    .query_map(rusqlite::params![owner, cutoff, CAPACITY], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .map_err(sqlite_error)?;
                bytes
                    .map(|row| Ok(serde_json::from_slice(&row.map_err(sqlite_error)?)?))
                    .collect()
            }
        }
    }

    fn record(&mut self, owner: &str, mut rows: Vec<Event>, cutoff: u64) -> Result<(), Error> {
        rows.retain(|row| row.ts_ms >= cutoff);
        if rows.is_empty() {
            return Ok(());
        }
        match self {
            Self::File(root) => {
                let path = path(root, owner);
                rows.extend(read_file(&path, cutoff)?);
                rows.sort_unstable_by_key(|row| std::cmp::Reverse(row.key()));
                rows.truncate(CAPACITY);
                write_file(&path, &rows)
            }
            #[cfg(feature = "storage-sqlite")]
            Self::Sqlite(db) => {
                let tx = db.transaction().map_err(sqlite_error)?;
                {
                    let mut insert = tx.prepare_cached("INSERT INTO dashboard_usage_events (owner, ts_ms, event_id, event) VALUES (?1, ?2, ?3, ?4)").map_err(sqlite_error)?;
                    for row in rows {
                        insert
                            .execute(rusqlite::params![
                                owner,
                                row.ts_ms,
                                row.event_id.as_bytes().as_slice(),
                                serde_json::to_vec(&row)?
                            ])
                            .map_err(sqlite_error)?;
                    }
                }
                tx.execute("DELETE FROM dashboard_usage_events WHERE owner = ?1 AND (ts_ms < ?2 OR event_id IN (SELECT event_id FROM dashboard_usage_events WHERE owner = ?1 ORDER BY ts_ms DESC, event_id DESC LIMIT -1 OFFSET ?3))", rusqlite::params![owner, cutoff, CAPACITY]).map_err(sqlite_error)?;
                tx.commit().map_err(sqlite_error)
            }
        }
    }

    fn prune(&mut self, cutoff: u64) -> Result<(), Error> {
        match self {
            Self::File(root) => {
                let entries = match fs::read_dir(&root) {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error.into()),
                };
                for entry in entries {
                    let entry = entry?;
                    let name = entry.file_name();
                    let Some(owner) = name.to_str().filter(|owner| {
                        owner.len() == 64 && owner.bytes().all(|c| c.is_ascii_hexdigit())
                    }) else {
                        continue;
                    };
                    let path = path(root, owner);
                    if !entry.file_type()?.is_dir() || !path.is_file() {
                        continue;
                    }
                    let all = read_file(&path, 0)?;
                    let kept: Vec<_> = all
                        .iter()
                        .filter(|row| row.ts_ms >= cutoff)
                        .cloned()
                        .collect();
                    if kept.is_empty() {
                        fs::remove_file(path)?;
                    } else if kept.len() != all.len() {
                        write_file(&path, &kept)?;
                    }
                }
                Ok(())
            }
            #[cfg(feature = "storage-sqlite")]
            Self::Sqlite(db) => {
                db.execute(
                    "DELETE FROM dashboard_usage_events WHERE ts_ms < ?1",
                    [cutoff],
                )
                .map_err(sqlite_error)?;
                Ok(())
            }
        }
    }
}

fn path(root: &Path, owner: &str) -> PathBuf {
    root.join(owner).join("dashboard").join(BASENAME)
}

fn read_file(path: &Path, cutoff: u64) -> Result<Vec<Event>, Error> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    if file.metadata()?.len() > MAX_FILE_BYTES {
        return Err(Error::audit("Dashboard usage file exceeds its capacity"));
    }
    let mut rows = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        if index >= CAPACITY {
            return Err(Error::audit(
                "Dashboard usage file exceeds its row capacity",
            ));
        }
        let row: Event = serde_json::from_str(&line?)?;
        if row.ts_ms >= cutoff {
            rows.push(row);
        }
    }
    Ok(rows)
}
fn write_file(path: &Path, rows: &[Event]) -> Result<(), Error> {
    atomic_write(path, |writer| {
        for row in rows {
            serde_json::to_writer(&mut *writer, row)?;
            writer.write_all(b"\n")?;
        }
        Ok(())
    })
}
fn atomic_write(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<(), Error>,
) -> Result<(), Error> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::config("Dashboard path has no parent"))?;
    fs::create_dir_all(parent)?;
    let tmp = path.with_extension("dashboard-tmp");
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?,
    );
    write(&mut writer)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(tmp, path)?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(feature = "storage-sqlite")]
fn sqlite_error(error: rusqlite::Error) -> Error {
    Error::audit_with_private("Dashboard SQLite operation failed", error.to_string())
}

#[cfg(feature = "storage-sqlite")]
fn open_sqlite(path: &Path) -> Result<rusqlite::Connection, Error> {
    let connection = rusqlite::Connection::open(path).map_err(sqlite_error)?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(sqlite_error)?;
    connection.execute_batch("CREATE TABLE IF NOT EXISTS dashboard_usage_events (owner TEXT NOT NULL, ts_ms INTEGER NOT NULL, event_id BLOB NOT NULL, event BLOB NOT NULL, PRIMARY KEY(owner, event_id));
        CREATE INDEX IF NOT EXISTS dashboard_usage_owner_time ON dashboard_usage_events(owner, ts_ms DESC, event_id DESC);
        CREATE INDEX IF NOT EXISTS dashboard_usage_retention ON dashboard_usage_events(ts_ms);").map_err(sqlite_error)?;
    Ok(connection)
}
