//! Application-owned bounded tracing persistence. Producers never wait for file
//! or stderr I/O; one binary-owned worker tees complete records and reports loss.
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    time::Duration,
};
use tracing_subscriber::fmt::MakeWriter;

const FILE_BYTES: u64 = 8 * 1024 * 1024;
const KEEP_FILES: usize = 4;
const RECORD_BYTES: usize = 16 * 1024;
const QUEUED_RECORDS: usize = 256;
const TRUNCATED: &[u8] = b" [diagnostic record truncated]\n";

#[derive(Clone)]
pub(crate) struct PersistentWriter(Arc<Queue>);
struct Queue {
    sender: SyncSender<Message>,
    dropped: Arc<AtomicU64>,
}
enum Message {
    Record(Vec<u8>),
    Flush(SyncSender<bool>),
}
/// The application keeps this guard outside the globally installed subscriber.
/// Shutdown gets a bounded chance to persist queued records, never an I/O join.
pub(crate) struct FlushGuard(PersistentWriter);
impl Drop for FlushGuard {
    fn drop(&mut self) {
        let _ = self.0.flush_timeout(Duration::from_millis(250));
    }
}
struct Log {
    directory: PathBuf,
    file: Option<File>,
    length: u64,
    limit: u64,
    failures: u64,
}
pub(crate) struct RecordWriter {
    owner: PersistentWriter,
    bytes: Vec<u8>,
    truncated: bool,
}
fn ordinary(path: &Path) -> io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            #[cfg(windows)]
            let linked = {
                use std::os::windows::fs::MetadataExt;
                metadata.file_attributes() & 0x400 != 0
            };
            #[cfg(not(windows))]
            let linked = metadata.file_type().is_symlink();
            if linked || !metadata.is_file() {
                return Err(io::Error::other("diagnostic log must be an ordinary file"));
            }
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
impl PersistentWriter {
    pub(crate) fn open(state: &Path) -> io::Result<Self> {
        Self::with_limit(state, FILE_BYTES)
    }
    fn with_limit(state: &Path, limit: u64) -> io::Result<Self> {
        Self::spawn(Log::open(state, limit)?, io::stdout())
    }
    fn spawn(log: Log, sink: impl Write + Send + 'static) -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(QUEUED_RECORDS);
        let dropped = Arc::new(AtomicU64::new(0));
        let worker_dropped = dropped.clone();
        // The worker owns no sender/Queue reference. Dropping the final producer
        // closes the channel, drains admitted records and lets the worker exit.
        std::thread::Builder::new()
            .name("server-logging".into())
            .spawn(move || persist(log, sink, receiver, worker_dropped))?;
        Ok(Self(Arc::new(Queue { sender, dropped })))
    }
    pub(crate) fn guard(&self) -> FlushGuard {
        FlushGuard(self.clone())
    }
    /// Fence prior accepted records. Queue-full, worker loss and stalled I/O all
    /// return false within the caller's timeout; no unbounded send or join occurs.
    pub(crate) fn flush_timeout(&self, timeout: Duration) -> bool {
        let (sender, receiver) = mpsc::sync_channel(1);
        if self.0.sender.try_send(Message::Flush(sender)).is_err() {
            return false;
        }
        receiver.recv_timeout(timeout).unwrap_or(false)
    }
}
impl Log {
    fn open(state: &Path, limit: u64) -> io::Result<Self> {
        // Configuration owns the private data directory.
        // Do not follow an existing logs link or redirect files during rotation.
        std::fs::create_dir_all(state)?;
        let state = std::fs::canonicalize(state)?;
        let directory = state.join("logs");
        match std::fs::symlink_metadata(&directory) {
            Ok(metadata) => {
                #[cfg(windows)]
                let linked = {
                    use std::os::windows::fs::MetadataExt;
                    metadata.file_attributes() & 0x400 != 0
                };
                #[cfg(not(windows))]
                let linked = metadata.file_type().is_symlink();
                if linked || !metadata.is_dir() {
                    return Err(io::Error::other("diagnostic directory must not be a link"));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                std::fs::create_dir(&directory)?;
            }
            Err(error) => return Err(error),
        }
        let mut log = Self {
            directory,
            file: None,
            length: 0,
            limit,
            failures: 0,
        };
        log.reopen()?;
        Ok(log)
    }
    fn path(&self, index: usize) -> PathBuf {
        self.directory.join(if index == 0 {
            "server.log".into()
        } else {
            format!("server.{index}.log")
        })
    }
    fn reopen(&mut self) -> io::Result<()> {
        let path = self.path(0);
        ordinary(&path)?;
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        self.length = file.metadata()?.len();
        self.file = Some(file);
        Ok(())
    }
    fn rotate(&mut self) -> io::Result<()> {
        self.file.take();
        // Every absolute target is a fixed child of the validated log directory;
        // check all targets before any delete/rename and never touch other files.
        for index in 0..KEEP_FILES {
            ordinary(&self.path(index))?;
        }
        let oldest = self.path(KEEP_FILES - 1);
        if ordinary(&oldest)? {
            std::fs::remove_file(oldest)?;
        }
        for index in (0..KEEP_FILES - 1).rev() {
            let source = self.path(index);
            if ordinary(&source)? {
                std::fs::rename(source, self.path(index + 1))?;
            }
        }
        self.reopen()
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.file.is_none() {
            self.reopen()?;
        }
        if self.length > 0 && self.length.saturating_add(bytes.len() as u64) > self.limit {
            self.rotate()?;
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("diagnostic file unavailable"))?;
        file.write_all(bytes)?;
        // Flush each event for inspection while running. This is an observation
        // log, not the fsynced mutation receipt authority or a replay source.
        file.flush()?;
        self.length = self.length.saturating_add(bytes.len() as u64);
        Ok(())
    }
    fn record(&mut self, sink: &mut impl Write, bytes: &[u8]) {
        let _ = sink.write_all(bytes);
        if let Err(error) = self.append(bytes) {
            self.file.take();
            self.failures = self.failures.saturating_add(1);
            // The worker never recurses into tracing. Keep failure reporting
            // bounded and use native codes/categories, never native Display text.
            if self.failures.is_power_of_two() {
                let _ = writeln!(
                    sink,
                    "server_log_write_failed error_kind={:?} os_code={:?} lost_records={}",
                    error.kind(),
                    error.raw_os_error(),
                    self.failures
                );
            }
        } else {
            self.failures = 0;
        }
    }
    fn report_drops(&mut self, sink: &mut impl Write, dropped: &AtomicU64) {
        let count = dropped.swap(0, Ordering::Relaxed);
        if count != 0 {
            self.record(
                sink,
                format!("server_log_queue_overflow lost_records={count}\n").as_bytes(),
            );
        }
    }
}
fn persist(
    mut log: Log,
    mut sink: impl Write,
    receiver: Receiver<Message>,
    dropped: Arc<AtomicU64>,
) {
    loop {
        // A final overflow need not have a later successful record to wake this
        // owner. Idle polling reports pending loss once the sink progresses.
        log.report_drops(&mut sink, &dropped);
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Message::Record(bytes)) => log.record(&mut sink, &bytes),
            Ok(Message::Flush(sender)) => {
                log.report_drops(&mut sink, &dropped);
                let flushed = sink.flush().is_ok() && log.failures == 0;
                let _ = sender.try_send(flushed);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                log.report_drops(&mut sink, &dropped);
                let _ = sink.flush();
                break;
            }
        }
    }
}
impl<'a> MakeWriter<'a> for PersistentWriter {
    type Writer = RecordWriter;
    fn make_writer(&'a self) -> Self::Writer {
        RecordWriter {
            owner: self.clone(),
            bytes: Vec::with_capacity(512),
            truncated: false,
        }
    }
}
impl Write for RecordWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = RECORD_BYTES.saturating_sub(self.bytes.len());
        self.bytes
            .extend_from_slice(&bytes[..remaining.min(bytes.len())]);
        self.truncated |= bytes.len() > remaining;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Drop for RecordWriter {
    fn drop(&mut self) {
        if self.truncated {
            self.bytes.truncate(RECORD_BYTES - TRUNCATED.len());
            // fmt writes arbitrary byte chunks. Validate the accumulated prefix,
            // not individual writes, so split Unicode scalars remain intact.
            if let Err(error) = std::str::from_utf8(&self.bytes) {
                self.bytes.truncate(error.valid_up_to());
            }
            self.bytes.extend_from_slice(TRUNCATED);
        }
        if self.bytes.is_empty() {
            return;
        }
        let bytes = std::mem::take(&mut self.bytes);
        if self
            .owner
            .0
            .sender
            .try_send(Message::Record(bytes))
            .is_err()
        {
            let _ =
                self.owner
                    .0
                    .dropped
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                        Some(count.saturating_add(1))
                    });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn rotation_retains_complete_records_and_restart_appends() {
        let directory = tempfile::tempdir().unwrap();
        let writer =
            PersistentWriter::spawn(Log::open(directory.path(), 32).unwrap(), io::sink()).unwrap();
        for i in 0..12 {
            writeln!(writer.make_writer(), "record-{i:02}-complete").unwrap();
        }
        assert!(writer.flush_timeout(Duration::from_secs(2)));
        drop(writer);
        let logs = directory.path().join("logs");
        assert_eq!(std::fs::read_dir(&logs).unwrap().count(), KEEP_FILES);
        for index in 0..KEEP_FILES {
            let path = logs.join(if index == 0 {
                "server.log".into()
            } else {
                format!("server.{index}.log")
            });
            let text = std::fs::read_to_string(path).unwrap();
            assert_eq!(text.lines().count(), 1);
            assert!(text.ends_with("-complete\n"));
        }
        let writer =
            PersistentWriter::spawn(Log::open(directory.path(), 64).unwrap(), io::sink()).unwrap();
        writeln!(writer.make_writer(), "after-reopen").unwrap();
        assert!(writer.flush_timeout(Duration::from_secs(2)));
        let text = std::fs::read_to_string(logs.join("server.log")).unwrap();
        assert!(text.contains("record-11-complete\nafter-reopen\n"));
    }
    #[test]
    fn record_limit_and_nonordinary_targets_are_explicit() {
        let directory = tempfile::tempdir().unwrap();
        let writer =
            PersistentWriter::spawn(Log::open(directory.path(), FILE_BYTES).unwrap(), io::sink())
                .unwrap();
        writer
            .make_writer()
            .write_all(&vec![b'x'; RECORD_BYTES * 2])
            .unwrap();
        assert!(writer.flush_timeout(Duration::from_secs(2)));
        let text = std::fs::read(directory.path().join("logs/server.log")).unwrap();
        assert_eq!(text.len(), RECORD_BYTES);
        assert!(text.ends_with(TRUNCATED));
        drop(writer);
        std::fs::create_dir(directory.path().join("logs/server.1.log")).unwrap();
        let mut log = Log::open(directory.path(), 1).unwrap();
        assert!(log.append(b"force rotation\n").is_err());
        assert!(directory.path().join("logs/server.1.log").is_dir());
        // Validation must finish before rotation mutates any existing evidence.
        assert_eq!(
            std::fs::read(directory.path().join("logs/server.log")).unwrap(),
            text
        );
    }

    struct BlockedSink {
        entered: Option<SyncSender<()>>,
        release: Receiver<()>,
        writes: mpsc::Sender<()>,
        bytes: Arc<Mutex<Vec<u8>>>,
    }
    impl Write for BlockedSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.bytes.lock().unwrap().extend_from_slice(bytes);
            let _ = self.writes.send(());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn blocked_sink_never_blocks_producers_and_reports_overflow_after_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let (entered, entered_receiver) = mpsc::sync_channel(1);
        let (release, release_receiver) = mpsc::sync_channel(1);
        let (writes, written) = mpsc::channel();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = PersistentWriter::spawn(
            Log::open(directory.path(), FILE_BYTES).unwrap(),
            BlockedSink {
                entered: Some(entered),
                release: release_receiver,
                writes,
                bytes: bytes.clone(),
            },
        )
        .unwrap();
        writeln!(writer.make_writer(), "first-complete").unwrap();
        entered_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        // This fence is admitted but the worker cannot acknowledge it yet.
        assert!(!writer.flush_timeout(Duration::from_millis(10)));
        let producer = writer.clone();
        let (done, finished) = mpsc::sync_channel(1);
        let producer = std::thread::spawn(move || {
            for i in 0..QUEUED_RECORDS + 32 {
                writeln!(producer.make_writer(), "queued-{i:03}-complete").unwrap();
            }
            done.send(()).unwrap();
        });
        let producer_returned = finished.recv_timeout(Duration::from_secs(2));
        let lost = writer.0.dropped.load(Ordering::Relaxed);
        let queue_full_flush = writer.flush_timeout(Duration::from_millis(10));
        // Always unblock the detached owner before assertions, even on failure.
        release.send(()).unwrap();
        assert!(
            producer_returned.is_ok(),
            "producer waited for blocked sink"
        );
        producer.join().unwrap();
        assert_eq!(
            lost, 33,
            "the earlier flush occupies one bounded queue slot"
        );
        assert!(!queue_full_flush);
        // First record, then overflow summary, then a queued record: at least one
        // queue slot is now free and no producer can refill it before the fence.
        for _ in 0..3 {
            written.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        assert!(writer.flush_timeout(Duration::from_secs(2)));
        let text = std::fs::read_to_string(directory.path().join("logs/server.log")).unwrap();
        assert!(text.contains("first-complete\n"));
        assert!(text.contains("server_log_queue_overflow lost_records=33\n"));
        assert!(text.contains("queued-254-complete\n"));
        assert!(!text.contains("queued-255-complete\n"));
        assert_eq!(text.as_bytes(), bytes.lock().unwrap().as_slice());
    }
}

#[cfg(test)]
mod unicode_tests {
    use super::*;
    #[test]
    fn truncation_keeps_utf8_and_guard_flushes_complete_formatter_output() {
        let directory = tempfile::tempdir().unwrap();
        let writer =
            PersistentWriter::spawn(Log::open(directory.path(), FILE_BYTES).unwrap(), io::sink())
                .unwrap();
        let guard = writer.guard();
        let mut record = writer.make_writer();
        let scalar = "界".as_bytes();
        for _ in 0..RECORD_BYTES {
            record.write_all(scalar).unwrap();
        }
        drop(record);
        // Formatter may split a scalar across writes before its record ends.
        let mut record = writer.make_writer();
        record.write_all(&scalar[..1]).unwrap();
        record.write_all(&scalar[1..]).unwrap();
        record.write_all(b" complete\n").unwrap();
        drop(record);
        drop(guard);
        let bytes = std::fs::read(directory.path().join("logs/server.log")).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("[diagnostic record truncated]\n"));
        assert!(text.ends_with("界 complete\n"));
        assert!(text.lines().next().unwrap().len() < RECORD_BYTES);
    }

    #[test]
    fn formatter_persists_utc_timestamp_target_without_ansi() {
        let directory = tempfile::tempdir().unwrap();
        let writer =
            PersistentWriter::spawn(Log::open(directory.path(), FILE_BYTES).unwrap(), io::sink())
                .unwrap();
        let guard = writer.guard();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer)
            .with_target(true)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "microcrab_server::fixture", status = 204, "complete formatter record");
        });
        drop(guard);
        let text = std::fs::read_to_string(directory.path().join("logs/server.log")).unwrap();
        assert!(!text.contains('\u{1b}'));
        let timestamp = text.split_whitespace().next().unwrap();
        assert_eq!(timestamp.len(), 27);
        assert!(timestamp.ends_with('Z'));
        assert_eq!(&timestamp[19..20], ".");
        assert!(timestamp[20..26].bytes().all(|byte| byte.is_ascii_digit()));
        assert!(text.contains("microcrab_server::fixture"));
        assert!(text.contains("status=204"));
        assert!(text.contains("complete formatter record"));
    }
}
