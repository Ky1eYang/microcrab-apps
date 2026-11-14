//! Binding-scoped process ownership. Pipe state is bounded and read on demand.
use crate::{Bridge, execution::host_error};
use futures_util::StreamExt;
use microcrab_host_api::process::ProcessHandle;
use microcrab_host_api::{BoxStream, Bytes, HandleId, OpContext, ProcessApi, ShellApi};
use microcrab_host_protocol::{
    BindingScope, ErrorCode, HostRequest, MAX_CHUNK_BYTES, ProcessChunk, ProcessStarted, WireError,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;

struct Pipe {
    stream: BoxStream<Bytes>,
    pending: Bytes,
    eof: bool,
}
pub(crate) struct OwnedProcess {
    pub scope: BindingScope,
    pub handle: ProcessHandle,
    pub pid: u32,
    pub changes: watch::Receiver<u64>,
    pipes: [Mutex<Option<Pipe>>; 2],
    stop: CancellationToken,
}
impl Drop for OwnedProcess {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
pub(crate) struct Processes {
    start_admission: Mutex<()>,
    entries: Mutex<HashMap<ProcessHandle, Arc<OwnedProcess>>>,
    pub changed: watch::Sender<u64>,
}
impl Default for Processes {
    fn default() -> Self {
        Self {
            start_admission: Mutex::new(()),
            entries: Mutex::new(HashMap::new()),
            changed: watch::channel(0).0,
        }
    }
}
impl Bridge {
    pub(crate) async fn start_owned_process(
        &self,
        method: HostRequest,
        scope: &BindingScope,
        ctx: &OpContext,
    ) -> Result<ProcessStarted, WireError> {
        // Admission never holds the table used by kill, status and inventory.
        let _admission = ctx
            .run(async { Ok(self.processes.start_admission.lock().await) })
            .await
            .map_err(host_error)?;
        if self.processes.entries.lock().await.len() >= 1024 {
            return Err(WireError::new(
                ErrorCode::Limit,
                "Host process handle limit reached",
            ));
        }
        let native = match method {
            HostRequest::ProcessSpawn(spec) => self.host.spawn(ctx, spec).await,
            HostRequest::ShellLaunch(request) => self.host.launch(ctx, request).await,
            _ => {
                return Err(WireError::new(
                    ErrorCode::InvalidRequest,
                    "expected process admission",
                ));
            }
        }
        .map_err(host_error)?;
        // Metadata publication is independent of the admission waiter's cancellation.
        let cleanup = OpContext::default();
        let metadata = async {
            let pid = self.host.pid(&cleanup, native).await?;
            let changes = self.host.watch(native)?;
            Ok::<_, microcrab_host_api::HostError>((pid, changes))
        }
        .await;
        let (pid, changes) = match metadata {
            Ok(value) => value,
            Err(error) => {
                let _ = self.host.release(&cleanup, native).await;
                return Err(host_error(error));
            }
        };
        let mut entries = self.processes.entries.lock().await;
        let handle = loop {
            let value = (uuid::Uuid::new_v4().as_u128() as u64) & ((1_u64 << 53) - 1);
            let candidate = ProcessHandle(HandleId(value));
            if value != 0 && !entries.contains_key(&candidate) {
                break candidate;
            }
        };
        tracing::info!(
            wire_process_handle = handle.0.0,
            native_process_handle = native.0.0,
            pid,
            "Host process identity correlated"
        );
        let receipt = ProcessStarted {
            handle,
            pid: Some(pid),
            revision: *changes.borrow(),
        };
        let mut changes_watch = changes.clone();
        let changed = self.processes.changed.clone();
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        entries.insert(
            handle,
            Arc::new(OwnedProcess {
                scope: scope.clone(),
                handle: native,
                pid,
                changes,
                pipes: [Mutex::new(None), Mutex::new(None)],
                stop,
            }),
        );
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stopped.cancelled() => break,
                    result = changes_watch.changed() => {
                        if result.is_err() { break; }
                        changed.send_modify(|revision| *revision = revision.wrapping_add(1));
                    }
                }
            }
        });
        self.processes
            .changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(receipt)
    }
    pub(crate) async fn owned_process(
        &self,
        id: ProcessHandle,
        scope: &BindingScope,
    ) -> Result<Arc<OwnedProcess>, WireError> {
        self.processes
            .entries
            .lock()
            .await
            .get(&id)
            .filter(|entry| &entry.scope == scope)
            .cloned()
            .ok_or_else(|| WireError::new(ErrorCode::NotFound, "unknown Host process"))
    }
    pub(crate) async fn release_owned_process(
        &self,
        id: ProcessHandle,
        scope: &BindingScope,
    ) -> Result<(), WireError> {
        let owned = {
            let entries = self.processes.entries.lock().await;
            match entries.get(&id) {
                Some(entry) if &entry.scope == scope => Some(entry.clone()),
                Some(_) => return Err(WireError::new(ErrorCode::NotFound, "unknown Host process")),
                None => None,
            }
        };
        if let Some(owned) = owned {
            // Remove retained stream state only after native confirmed kill/reap/release.
            self.host
                .release(&OpContext::default(), owned.handle)
                .await
                .map_err(host_error)?;
            self.processes.entries.lock().await.remove(&id);
        }
        Ok(())
    }
    pub(crate) async fn process_inventory(&self, scope: &BindingScope) -> Vec<ProcessStarted> {
        self.processes
            .entries
            .lock()
            .await
            .iter()
            .filter(|(_, entry)| &entry.scope == scope)
            .map(|(handle, entry)| ProcessStarted {
                handle: *handle,
                pid: Some(entry.pid),
                revision: *entry.changes.borrow(),
            })
            .collect()
    }
    pub(crate) async fn process_revisions(
        &self,
        scope: &BindingScope,
    ) -> Vec<(ProcessHandle, u64)> {
        self.process_inventory(scope)
            .await
            .into_iter()
            .map(|entry| (entry.handle, entry.revision))
            .collect()
    }
    pub(crate) async fn open_process_stream(
        &self,
        id: ProcessHandle,
        scope: &BindingScope,
        stderr: bool,
    ) -> Result<(), WireError> {
        let owned = self.owned_process(id, scope).await?;
        let mut pipe = owned.pipes[usize::from(stderr)].lock().await;
        if pipe.is_some() {
            return Err(WireError::new(
                ErrorCode::Conflict,
                "process pipe already taken",
            ));
        }
        let stream = if stderr {
            self.host.stderr(owned.handle)
        } else {
            self.host.stdout(owned.handle)
        }
        .map_err(host_error)?;
        *pipe = Some(Pipe {
            stream,
            pending: Bytes::new(),
            eof: false,
        });
        Ok(())
    }
    pub(crate) async fn read_process_stream(
        &self,
        id: ProcessHandle,
        scope: &BindingScope,
        stderr: bool,
        max_bytes: u32,
        ctx: &OpContext,
    ) -> Result<ProcessChunk, WireError> {
        if max_bytes == 0 || max_bytes as usize > MAX_CHUNK_BYTES {
            return Err(WireError::new(
                ErrorCode::InvalidRequest,
                "invalid process chunk bound",
            ));
        }
        let owned = self.owned_process(id, scope).await?;
        let mut pipe = ctx
            .run(async { Ok(owned.pipes[usize::from(stderr)].lock().await) })
            .await
            .map_err(host_error)?;
        let pipe = pipe
            .as_mut()
            .ok_or_else(|| WireError::new(ErrorCode::Conflict, "process pipe not taken"))?;
        if pipe.pending.is_empty() && !pipe.eof {
            match ctx
                .run(async { Ok(pipe.stream.next().await) })
                .await
                .map_err(host_error)?
            {
                Some(chunk) => pipe.pending = chunk.map_err(host_error)?,
                None => pipe.eof = true,
            }
        }
        let count = pipe.pending.len().min(max_bytes as usize);
        let data = pipe.pending.split_to(count).to_vec();
        Ok(ProcessChunk {
            data,
            eof: pipe.eof && pipe.pending.is_empty(),
        })
    }
}
