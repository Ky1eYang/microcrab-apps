//! Default server has no execution environment. Only explicit embeddings assemble LocalHost.
use microcrab_host_api::{FsApi, Host, HostAPI, NetApi, ProcessApi, ShellApi};

pub(crate) struct NoopHost;
impl ProcessApi for NoopHost {}
impl FsApi for NoopHost {}
impl NetApi for NoopHost {}
impl ShellApi for NoopHost {}
impl HostAPI for NoopHost {}
impl Host for NoopHost {}
