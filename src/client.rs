use libc::{O_CREAT, O_EXCL, O_RDONLY, O_WRONLY};
use std::ffi::{CStr, CString};

use crate::native;
use crate::native::{hdfsFS, hdfsFile, hdfsFileInfo, tObjectKind, tSize};
use bytes::Bytes;
use futures::stream::Stream;
use libc::{c_int, c_short, c_ushort, c_void};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::task;

const DATA_BLOCK_SIZE: usize = 65536;
const MAX_CONNECTIONS: usize = 1;

/// URI schemes recognized by this client.
const SUPPORTED_SCHEMES: &[&str] = &["hdfs://", "hopsfs://"];

#[derive(Error, Debug)]
pub enum HdfsError {
    #[error("File not found at {0}")]
    FileNotFound(String),
    #[error("File already exists at {0}")]
    AlreadyExists(String),
    #[error("Invalid path to {0}")]
    InvalidPath(String),
    #[error("{0}")]
    NoneUnicodeInPath(String),
    #[error("{0} is a directory")]
    IsADirectoryError(String),
    #[error("Operation failed: {0}")]
    OperationFailed(String),
    #[error("Invalid connection URL {0}")]
    InvalidUri(String),
    #[error("{0} is not supported yet!")]
    NotSupported(String),
}

pub type Result<T> = std::result::Result<T, HdfsError>;

#[derive(Clone)]
pub struct WriteOptions {
    pub block_size: Option<c_int>,
    pub replication: Option<c_short>,
    pub overwrite: bool,
    pub create_parent: bool, //create_parent false is not supported, yet!
    pub buffer_size: c_int,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            block_size: None,
            replication: None,
            overwrite: false,
            create_parent: true,
            buffer_size: 0,
        }
    }
}

#[derive(Debug)]
pub struct FileStatus {
    pub path: String,
    pub length: usize,
    pub isdir: bool,
    pub permission: u16,
    pub owner: String,
    pub group: String,
    pub modification_time: u64,
    pub access_time: u64,
    pub replication: Option<u32>,
    pub blocksize: Option<u64>,
}

/// Strip a supported `scheme://` prefix, returning the remainder if matched.
fn strip_supported_scheme(uri: &str) -> Option<&str> {
    SUPPORTED_SCHEMES.iter().find_map(|s| uri.strip_prefix(s))
}

/// Strip scheme and authority from a libhdfs path, e.g.
/// `hdfs://host:port/foo` -> `/foo`. Unchanged if the scheme is unsupported.
fn strip_scheme_authority(path: &str) -> &str {
    let Some(after_authority) = strip_supported_scheme(path) else {
        return path;
    };
    match after_authority.find('/') {
        Some(i) => &after_authority[i..],
        None => "/",
    }
}

/// errno of the last libhdfs call on this thread. Read it in the same blocking task as
/// the call: libhdfs sets errno on the calling thread.
fn last_errno() -> Option<i32> {
    std::io::Error::last_os_error().raw_os_error()
}

impl FileStatus {
    pub fn from_hdfs_file_info(file_info: *const hdfsFileInfo) -> Result<Self> {
        unsafe {
            let path = CStr::from_ptr((*file_info).mName)
                .to_str()
                .ok()
                .map(|s| strip_scheme_authority(s).to_owned());

            let owner = CStr::from_ptr((*file_info).mOwner)
                .to_str()
                .ok()
                .map(|s| s.to_owned());

            let group = CStr::from_ptr((*file_info).mGroup)
                .to_str()
                .ok()
                .map(|s| s.to_owned());

            Ok(FileStatus {
                path: path.unwrap(),
                length: (*file_info).mSize as usize,
                isdir: (*file_info).mKind == tObjectKind::kObjectKindDirectory,
                permission: (*file_info).mPermissions as u16,
                owner: owner.unwrap(),
                group: group.unwrap(),
                modification_time: (*file_info).mLastMod as u64,
                access_time: (*file_info).mLastAccess as u64,
                replication: Some((*file_info).mReplication as u32),
                blocksize: Some((*file_info).mBlockSize as u64),
            })
        }
    }
}

pub struct FileReader {
    pub file: Arc<AtomicPtr<hdfsFile>>,
}

impl FileReader {
    pub fn new(file: *mut hdfsFile) -> Self {
        FileReader {
            file: Arc::new(AtomicPtr::new(file)),
        }
    }
    pub fn get_file_ptr(&self) -> *const hdfsFile {
        self.file.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct FileWriter {
    connection: Arc<Connection>,
    file: Arc<AtomicPtr<hdfsFile>>,
    closed: AtomicBool,
    io_runtime: Option<Handle>,
}

impl FileWriter {
    pub fn new(connection: Arc<Connection>, file: *mut hdfsFile, io_runtime: Option<Handle>) -> Self {
        FileWriter {
            connection,
            file: Arc::new(AtomicPtr::new(file)),
            closed: AtomicBool::new(false),
            io_runtime,
        }
    }
    pub fn get_file_ptr(&self) -> *const hdfsFile {
        self.file.load(Ordering::SeqCst)
    }

    pub async fn hdfs_write(&self, buf: Bytes) -> Result<()> {
        let file_ptr = self.get_file_ptr() as usize;
        let connection = Arc::clone(&self.connection);

        let res = self.spawn_blocking(move || {
            let buf_ptr = buf.as_ptr().cast::<c_void>();
            let buf_len = buf.len() as tSize;
            unsafe {
                native::hdfsWrite(
                    connection.get_conn_ptr(),
                    file_ptr as *const hdfsFile,
                    buf_ptr,
                    buf_len,
                )
            }
        })
        .await;

        if res.is_err() || res.unwrap() == -1 {
            Err(HdfsError::OperationFailed(
                "File write operation failed".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    pub async fn close_file(&self) -> Result<()> {
        let file_ptr = self.get_file_ptr() as usize;
        let connection = Arc::clone(&self.connection);

        let res = self.spawn_blocking(move || unsafe {
            native::hdfsCloseFile(connection.get_conn_ptr(), file_ptr as *const hdfsFile)
        })
        .await;

        if res.is_err() || res.unwrap() == -1 {
            return Err(HdfsError::OperationFailed(
                "File close operation failed".to_string(),
            ));
        }
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn spawn_blocking<F, R>(&self, f: F) -> task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        match &self.io_runtime {
            Some(handle) => handle.spawn_blocking(f),
            None => task::spawn_blocking(f),
        }
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::SeqCst) {
            unsafe {
                native::hdfsCloseFile(self.connection.get_conn_ptr(), self.get_file_ptr());
            }
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    pub ptr: AtomicPtr<hdfsFS>,
}

impl Connection {
    pub fn new(ptr: *const hdfsFS) -> Self {
        Connection {
            ptr: AtomicPtr::new(ptr.cast_mut()),
        }
    }

    fn get_conn_ptr(&self) -> *const hdfsFS {
        self.ptr.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct HopsClient {
    pub hdfs_internal: Arc<Vec<Arc<Connection>>>,
    next_conn_idx: AtomicUsize,
    io_runtime: Option<Handle>,
}

impl Drop for HopsClient {
    /// Disconnect from the HDFS filesystem.
    /// This can potentially cause problem if the disconnect fails.
    /// Yet there is no explicit close process exists in the lib.rs
    fn drop(&mut self) {
        for i in 0..MAX_CONNECTIONS {
            unsafe {
                let ret = native::hdfsDisconnect(self.hdfs_internal[i].get_conn_ptr());
                if ret != 0 {
                    eprintln!("hdfsDisconnect failed with error code: {}", ret);
                }
            }
        }
    }
}

impl HopsClient {
    /// Create a new HopsClient with the specified URL, optional config, and optional IO runtime
    pub fn new(
        url: &str,
        config: Option<HashMap<String, String>>,
        io_runtime: Option<Handle>,
    ) -> Result<Self> {
        let mut connections = Vec::with_capacity(MAX_CONNECTIONS);
        for _ in 0..MAX_CONNECTIONS {
            let fs = match &config {
                Some(cfg) => Self::hopsfs_connect_with_config(url, cfg)?,
                None => Self::hopsfs_connect_with_url(url)?,
            };
            let connection = Arc::new(Connection::new(fs));
            connections.push(connection);
        }
        Ok(HopsClient {
            hdfs_internal: Arc::new(connections),
            next_conn_idx: AtomicUsize::new(0),
            io_runtime,
        })
    }

    /// Create a new HopsClient using the specified URL
    #[deprecated(since = "1.1.0", note = "Use HopsClient::new() instead")]
    pub fn with_url(url: &str) -> Result<Self> {
        Self::new(url, None, None)
    }

    /// Create a new HopsClient using the specified URL and Hadoop configs
    #[deprecated(since = "1.1.0", note = "Use HopsClient::new() instead")]
    pub fn with_config(url: &str, config: HashMap<String, String>) -> Result<Self> {
        Self::new(url, Some(config), None)
    }

    pub fn get_connection(&self) -> Arc<Connection> {
        let curr_index = loop {
            let current = self.next_conn_idx.load(Ordering::SeqCst);
            let new = (current + 1) % MAX_CONNECTIONS;
            match self.next_conn_idx.compare_exchange(
                current,
                new,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(prev) => break prev,
                Err(_) => continue,
            }
        };
        Arc::clone(&self.hdfs_internal[curr_index])
    }

    fn hopsfs_connect_with_url(uri: &str) -> Result<*const hdfsFS> {
        let (host_str, port_u16) = extract_host_and_port(uri);

        let c_host = CString::new(host_str).expect("CString conversion failed");
        let c_port: c_ushort = port_u16;

        let max_retries = 3;
        let mut attempt = 0;
        while attempt < max_retries {
            unsafe {
                let fs = native::hdfsConnect(c_host.as_ptr(), c_port);
                if !fs.is_null() {
                    return Ok(fs);
                }
            }
            attempt += 1;
            thread::sleep(Duration::from_millis(500));
        }

        Err(HdfsError::OperationFailed(format!(
            "Connection to HopsFS failed after {} attempts! {}",
            max_retries,
            uri.to_string()
        )))
    }

    fn hopsfs_connect_with_config(
        url: &str,
        config: &HashMap<String, String>,
    ) -> Result<*const hdfsFS> {
        let (host_str, port_u16) = extract_host_and_port(url);
        let c_host = CString::new(host_str).expect("CString conversion failed");
        let c_port: c_ushort = port_u16;

        unsafe {
            let builder = native::hdfsNewBuilder();
            if builder.is_null() {
                return Err(HdfsError::OperationFailed(
                    "Failed to create HopsFS builder".to_string(),
                ));
            }

            native::hdfsBuilderSetNameNode(builder, c_host.as_ptr());
            native::hdfsBuilderSetNameNodePort(builder, c_port);
            native::hdfsBuilderSetForceNewInstance(builder);

            for (key, value) in config.iter() {
                let c_key = CString::new(key.as_str())
                    .map_err(|_| HdfsError::OperationFailed("Invalid config key".to_string()))?;
                let c_value = CString::new(value.as_str())
                    .map_err(|_| HdfsError::OperationFailed("Invalid config value".to_string()))?;
                native::hdfsBuilderConfSetStr(builder, c_key.as_ptr(), c_value.as_ptr());
            }

            let fs = native::hdfsBuilderConnect(builder);
            if fs.is_null() {
                return Err(HdfsError::OperationFailed(format!(
                    "Connection to HopsFS failed! {}",
                    url.to_string()
                )));
            }
            Ok(fs)
        }
    }

    pub async fn check_file_exists(&self, path: &str) -> Result<bool> {
        let connection = self.get_connection();
        let c_path = CString::new(path).unwrap();

        let res = self.spawn_blocking(move || unsafe {
            let result = native::hdfsExists(connection.get_conn_ptr(), c_path.as_ptr());
            result == 0
        })
        .await;

        if res.is_err() {
            return Err(HdfsError::OperationFailed(
                "Failed to check file existence".to_string(),
            ));
        }
        Ok(res.unwrap())
    }

    pub async fn get_file_info(&self, path: &str) -> Result<FileStatus> {
        let refined_path = CString::new(path).unwrap();
        let path_owned = path.to_string();
        let connection = self.get_connection();

        let file_status = self.spawn_blocking(move || unsafe {
            let path_info =
                native::hdfsGetPathInfo(connection.get_conn_ptr(), refined_path.as_ptr());

            if path_info.is_null() {
                return Err(HdfsError::FileNotFound(path_owned));
            }

            let owner = CStr::from_ptr((*path_info).mOwner)
                .to_str()
                .ok()
                .map(|s| s.to_owned());

            let group = CStr::from_ptr((*path_info).mGroup)
                .to_str()
                .ok()
                .map(|s| s.to_owned());

            let status = FileStatus {
                path: path_owned,
                length: (*path_info).mSize as usize,
                isdir: (*path_info).mKind == tObjectKind::kObjectKindDirectory,
                permission: (*path_info).mPermissions as u16,
                owner: owner.unwrap(),
                group: group.unwrap(),
                modification_time: (*path_info).mLastMod as u64,
                access_time: (*path_info).mLastAccess as u64,
                replication: Some((*path_info).mReplication as u32),
                blocksize: Some((*path_info).mBlockSize as u64),
            };

            native::hdfsFreeFileInfo(path_info, 1);

            Ok(status)
        })
        .await
        .map_err(|_| HdfsError::FileNotFound(path.to_string()))??;

        Ok(file_status)
    }

    pub async fn open_for_read(&self, path: &str) -> Result<FileReader> {
        if self.get_file_info(path).await.is_err() {
            Err(HdfsError::FileNotFound(path.to_string()))?
        }
        let c_path = CString::new(path)
            .map_err(|_| HdfsError::OperationFailed("Invalid path".to_string()))?;
        let connection = self.get_connection();

        let file_reader = self.spawn_blocking(move || unsafe {
            let hdfs_file = native::hdfsOpenFile(
                connection.get_conn_ptr(),
                c_path.as_ptr(),
                O_RDONLY,
                0,
                0,
                0,
            );

            if hdfs_file.is_null() {
                Err(HdfsError::OperationFailed(
                    "Failed to open file".to_string(),
                ))
            } else {
                Ok(FileReader::new(hdfs_file.cast_mut()))
            }
        })
        .await
        .map_err(|_| HdfsError::OperationFailed("Failed to open file".to_string()))??;

        Ok(file_reader)
    }

    /// Create a file for writing. Without `opts.overwrite` the namenode creates it only if
    /// the path does not exist, atomically, and an existing path is [HdfsError::AlreadyExists].
    pub async fn create(&self, path: &str, opts: WriteOptions) -> Result<FileWriter> {
        let c_path = CString::new(path)
            .map_err(|_| HdfsError::OperationFailed("Invalid path".to_string()))?;
        let connection = self.get_connection();
        let io_runtime = self.io_runtime.clone();
        let flags = if opts.overwrite {
            O_WRONLY
        } else {
            O_WRONLY | O_CREAT | O_EXCL
        };

        let result = self
            .spawn_blocking(move || unsafe {
                let file = native::hdfsOpenFile(
                    connection.get_conn_ptr(),
                    c_path.as_ptr(),
                    flags,
                    opts.buffer_size,
                    opts.replication.unwrap_or(0),
                    opts.block_size.unwrap_or(0),
                );
                if file.is_null() {
                    Err(last_errno())
                } else {
                    Ok(FileWriter::new(connection, file.cast_mut(), io_runtime))
                }
            })
            .await
            .map_err(|_| HdfsError::OperationFailed("Failed to create file".to_string()))?;

        match result {
            Ok(writer) => Ok(writer),
            Err(Some(libc::EEXIST)) => Err(HdfsError::AlreadyExists(path.to_string())),
            Err(errno) => Err(HdfsError::OperationFailed(format!(
                "Failed to create file {path}, errno {errno:?}"
            ))),
        }
    }

    /// Rename `from` to `to`. With `overwrite` an existing `to` is replaced. Without it the
    /// namenode renames only if `to` does not exist, atomically, and an existing `to` is
    /// [HdfsError::AlreadyExists]: of two callers renaming onto one path, exactly one wins.
    pub async fn rename(&self, from: &str, to: &str, overwrite: bool) -> Result<()> {
        let _from = CString::new(from).unwrap();
        let _to = CString::new(to).unwrap();
        let connection = self.get_connection();

        let res = self
            .spawn_blocking(move || unsafe {
                let ret = if overwrite {
                    native::hdfsRename(connection.get_conn_ptr(), _from.as_ptr(), _to.as_ptr())
                } else {
                    native::hdfsRename2(
                        connection.get_conn_ptr(),
                        _from.as_ptr(),
                        _to.as_ptr(),
                        native::HDFS_RENAME_NOREPLACE,
                    )
                };
                if ret == 0 { Ok(()) } else { Err(last_errno()) }
            })
            .await
            .map_err(|_| HdfsError::OperationFailed("rename failed!".to_string()))?;

        match res {
            Ok(()) => Ok(()),
            Err(Some(libc::EEXIST)) if !overwrite => Err(HdfsError::AlreadyExists(to.to_string())),
            Err(errno) => Err(HdfsError::OperationFailed(format!(
                "rename {from} to {to} failed, errno {errno:?}"
            ))),
        }
    }

    pub async fn delete(&self, path: &str, _recursive: bool) -> Result<bool> {
        let _path = CString::new(path).unwrap();
        let connection = self.get_connection();

        let res = self.spawn_blocking(move || unsafe {
            native::hdfsDelete(
                connection.get_conn_ptr(),
                _path.as_ptr(),
                _recursive as c_int,
            )
        })
        .await;

        if res.is_err() || res.unwrap() == -1 {
            return Err(HdfsError::OperationFailed(
                "Failed to delete file".to_string(),
            ));
        }

        Ok(true)
    }

    pub async fn list_directory(&self, prefix: &str) -> Result<Vec<FileStatus>> {
        let path_cstr =
            CString::new(prefix).map_err(|_| HdfsError::InvalidPath(prefix.to_string()))?;

        let mut num_entries: c_int = 0;
        let connection = self.get_connection();
        unsafe {
            let response = native::hdfsListDirectory(
                connection.get_conn_ptr(),
                path_cstr.as_ptr(),
                &mut num_entries,
            );
            if response.is_null() || num_entries == 0 {
                return Ok(vec![]);
            }

            let file_infos = std::slice::from_raw_parts(response, num_entries as usize);
            let mut objects = Vec::with_capacity(num_entries as usize);

            for info in file_infos.iter() {
                match FileStatus::from_hdfs_file_info(info) {
                    Ok(file_info) => objects.push(file_info),
                    Err(_e) => Err(HdfsError::OperationFailed(
                        "Listing directory content failed!".to_string(),
                    ))?,
                }
            }
            native::hdfsFreeFileInfo(response, num_entries);
            Ok(objects)
        }
    }

    pub async fn hdfs_copy(&self, src: &str, dst: &str, overwrite: bool) -> Result<()> {
        let dst_exists = self.check_file_exists(dst).await?;
        if dst_exists && !overwrite {
            Err(HdfsError::AlreadyExists(dst.to_string()))?
        }

        let src_exists = self.check_file_exists(src).await?;
        if !src_exists {
            Err(HdfsError::FileNotFound(src.to_string()))?
        }

        let src_cstr = CString::new(src).unwrap();
        let dst_cstr = CString::new(dst).unwrap();
        let connection = self.get_connection();

        let res = self.spawn_blocking(move || unsafe {
            let conn_ptr_usize = connection.get_conn_ptr();
            native::hdfsCopy(
                conn_ptr_usize,
                src_cstr.as_ptr(),
                conn_ptr_usize,
                dst_cstr.as_ptr(),
            )
        })
        .await;

        if matches!(res, Ok(0)) {
            Ok(())
        } else {
            Err(HdfsError::OperationFailed(
                "Failed to copy file".to_string(),
            ))
        }
    }

    pub async fn mkdir(&self, path: &str) -> Result<()> {
        let path_cstr = CString::new(path).unwrap();
        let connection = self.get_connection();

        let res = self.spawn_blocking(move || unsafe {
            native::hdfsCreateDirectory(connection.get_conn_ptr(), path_cstr.as_ptr())
        })
        .await;

        if res.is_err() || res.unwrap() == -1 {
            return Err(HdfsError::OperationFailed(
                "Failed to create directory".to_string(),
            ));
        }

        Ok(())
    }

    fn spawn_blocking<F, R>(&self, f: F) -> task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        match &self.io_runtime {
            Some(handle) => handle.spawn_blocking(f),
            None => task::spawn_blocking(f),
        }
    }
}

fn extract_host_and_port(uri: &str) -> (String, u16) {
    let default_port = 8020;

    let stripped_uri = strip_supported_scheme(uri).unwrap_or(uri);
    let authority = stripped_uri.split('/').next().unwrap_or("");

    let mut parts = authority.split(':');
    let host = parts.next().unwrap_or("").to_string();
    let port = parts
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(default_port);

    (host, port)
}

pub struct ReadRangeStream {
    connection: Arc<Connection>,
    file_pointer: Arc<AtomicPtr<hdfsFile>>,
    current: usize,
    end: usize,
}

impl ReadRangeStream {
    pub fn new(
        connection: Arc<Connection>,
        file_pointer: Arc<AtomicPtr<hdfsFile>>,
        start: usize,
        end: usize,
    ) -> Self {
        Self {
            connection,
            file_pointer,
            current: start,
            end,
        }
    }
}

impl Stream for ReadRangeStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current >= self.end {
            return Poll::Ready(None);
        }

        let current_end = std::cmp::min(self.current + DATA_BLOCK_SIZE, self.end);
        let length = current_end - self.current;
        let mut buffer = vec![0u8; length];

        unsafe {
            let result = native::hdfsPread(
                self.connection.get_conn_ptr(),
                self.file_pointer.load(Ordering::SeqCst),
                self.current as i64,
                buffer.as_mut_ptr() as *mut c_void,
                length as tSize,
            );
            if result <= 0 || (result as usize != length && current_end <= self.end) {
                return Poll::Ready(Some(Err(HdfsError::OperationFailed(format!(
                    "Failed to read {} bytes from {} with result value {}",
                    length, self.current, result
                )))));
            }
            self.current = current_end;
            let result_bytes = Bytes::copy_from_slice(&buffer[..result as usize]);
            Poll::Ready(Some(Ok(result_bytes)))
        }
    }
}

impl Drop for ReadRangeStream {
    fn drop(&mut self) {
        unsafe {
            native::hdfsCloseFile(
                self.connection.get_conn_ptr(),
                self.file_pointer.load(Ordering::SeqCst),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::extract_host_and_port;

    #[test]
    fn host_and_port_ignore_the_path() {
        assert_eq!(
            extract_host_and_port("hdfs://localhost:43313/tbl/_delta_log"),
            ("localhost".to_string(), 43313)
        );
        assert_eq!(
            extract_host_and_port("hdfs://nn.example:9000"),
            ("nn.example".to_string(), 9000)
        );
        assert_eq!(
            extract_host_and_port("hdfs://nn.example/tbl"),
            ("nn.example".to_string(), 8020)
        );
    }
}
