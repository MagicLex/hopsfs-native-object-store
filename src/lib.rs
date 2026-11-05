//! [object_store::ObjectStore] implementation for HopsFS/HDFS using libhdfs Go client bindings
//!
//! # Usage
//!
//! ```rust
//! use hopsfs_native_object_store::HdfsObjectStoreBuilder;
//! let store = HdfsObjectStoreBuilder::new()
//!     .with_url("hdfs://localhost:8020")
//!     .build()
//!     .unwrap();
//! ```
//!
mod client;
mod native;

use async_stream::try_stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
pub use client::HopsClient;
use futures::{
    stream::{BoxStream, StreamExt},
    FutureExt,
};
use object_store::path::Error::InvalidPath;
#[allow(deprecated)]
use object_store::{
    path::Path, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
    UploadPart,
};
use std::collections::VecDeque;
use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

pub type Client = HopsClient;

use crate::client::ReadRangeStream;
pub use crate::client::{FileStatus, FileWriter, HdfsError, WriteOptions};

fn generic_error(
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
) -> object_store::Error {
    object_store::Error::Generic {
        store: "HdfsObjectStore",
        source,
    }
}

/// Builder for creating an [HdfsObjectStore]
#[derive(Default)]
pub struct HdfsObjectStoreBuilder {
    url: Option<String>,
    config: HashMap<String, String>,
    io_runtime: Option<Handle>,
}

impl HdfsObjectStoreBuilder {
    /// Create a new [HdfsObjectStoreBuilder]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the URL to connect to. Can be the address of a single NameNode, or a logical NameService
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set configs to use for the client. The provided configs will override any found in the default config files loaded
    pub fn with_config(
        mut self,
        config: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.config = config
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    /// Use a dedicated tokio runtime for spawned tasks and IO operations
    pub fn with_io_runtime(mut self, runtime: Handle) -> Self {
        self.io_runtime = Some(runtime);
        self
    }

    /// Create the [HdfsObjectStore] instance from the provided settings
    pub fn build(self) -> Result<HdfsObjectStore> {
        let url = self.url.ok_or_else(|| generic_error("URL is required".into()))?;

        let client = if self.config.is_empty() {
            Client::new(&url, None, self.io_runtime.clone()).to_object_store_err()?
        } else {
            Client::new(&url, Some(self.config), self.io_runtime.clone()).to_object_store_err()?
        };

        Ok(HdfsObjectStore {
            client: Arc::new(client),
            io_runtime: self.io_runtime,
        })
    }
}

/// Interface for [Hadoop Distributed File System](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).
#[derive(Debug)]
pub struct HdfsObjectStore {
    client: Arc<Client>,
    io_runtime: Option<Handle>,
}

impl Clone for HdfsObjectStore {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            io_runtime: self.io_runtime.clone(),
        }
    }
}

impl HdfsObjectStore {
    /// Creates a new HdfsObjectStore from an existing [Client]
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use hopsfs_native_object_store::{Client, HdfsObjectStore};
    /// let client = Client::with_url("hdfs://127.0.0.1:8020").unwrap();
    /// let store = HdfsObjectStore::new(Arc::new(client));
    /// ```
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            io_runtime: None,
        }
    }

    /// Creates a new HdfsObjectStore using the specified URL
    ///
    /// Connect to a NameNode
    /// ```rust
    /// # use hopsfs_native_object_store::HdfsObjectStore;
    /// # fn main() -> object_store::Result<()> {
    /// let store = HdfsObjectStore::with_url("hdfs://127.0.0.1:8020")?;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(since = "1.1.0", note = "Use HdfsObjectStoreBuilder instead")]
    pub fn with_url(url: &str) -> Result<Self> {
        HdfsObjectStoreBuilder::new().with_url(url).build()
    }

    /// Creates a new HdfsObjectStore using the specified URL and Hadoop configs.
    ///
    /// Connect to a NameService
    /// ```rust
    /// # use hopsfs_native_object_store::HdfsObjectStore;
    /// # use std::collections::HashMap;
    /// # fn main() -> object_store::Result<()> {
    /// let config = HashMap::from([
    ///     ("dfs.ha.namenodes.ns".to_string(), "nn1,nn2".to_string()),
    ///     ("dfs.namenode.rpc-address.ns.nn1".to_string(), "nn1.example.com:9000".to_string()),
    ///     ("dfs.namenode.rpc-address.ns.nn2".to_string(), "nn2.example.com:9000".to_string()),
    /// ]);
    /// let store = HdfsObjectStore::with_config("hdfs://ns", config)?;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(since = "1.1.0", note = "Use HdfsObjectStoreBuilder instead")]
    pub fn with_config(url: &str, config: HashMap<String, String>) -> Result<Self> {
        HdfsObjectStoreBuilder::new()
            .with_url(url)
            .with_config(config)
            .build()
    }

    async fn internal_copy(&self, from: &Path, to: &Path, overwrite: bool) -> Result<()> {
        self.client
            .hdfs_copy(
                &make_absolute_file(from),
                &make_absolute_file(to),
                overwrite,
            )
            .await
            .to_object_store_err()
    }

    async fn open_tmp_file(&self, file_path: &str) -> Result<(FileWriter, String)> {
        let path_buf = PathBuf::from(file_path);

        let file_name = path_buf
            .file_name()
            .ok_or(HdfsError::InvalidPath(file_path.to_string()))
            .to_object_store_err()?
            .to_str()
            .ok_or(HdfsError::NoneUnicodeInPath(file_path.to_string()))
            .to_object_store_err()?
            .to_string();

        let tmp_file_path = path_buf
            .with_file_name(format!(".{}.tmp", file_name))
            .to_str()
            .ok_or(HdfsError::NoneUnicodeInPath(file_path.to_string()))
            .to_object_store_err()?
            .to_string();

        // Try to create a file with an incrementing index until we find one that doesn't exist yet
        let mut index = 1;
        loop {
            let path = format!("{}.{}", tmp_file_path, index);
            match self.client.create(&path, WriteOptions::default()).await {
                Ok(writer) => break Ok((writer, path)),
                Err(HdfsError::AlreadyExists(_)) => index += 1,
                Err(e) => break Err(e).to_object_store_err(),
            }
        }
    }
}

impl Display for HdfsObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HdfsObjectStore")
    }
}

impl From<Client> for HdfsObjectStore {
    fn from(value: Client) -> Self {
        Self::new(Arc::new(value))
    }
}

#[async_trait]
impl ObjectStore for HdfsObjectStore {
    /// Save the provided bytes to the specified location
    ///
    /// To make the operation atomic, we write to a temporary file `.{filename}.tmp.{i}` and rename
    /// on a successful write, where `i` is an integer that is incremented until a non-existent file
    /// is found.
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let overwrite = match opts.mode {
            PutMode::Create => false,
            PutMode::Overwrite => true,
            PutMode::Update(_) => {
                return Err(object_store::Error::NotSupported {
                    source: "Update mode not supported".to_string().into(),
                })
            }
        };

        let final_file_path = make_absolute_file(location);

        // If we're not overwriting, do an upfront check to see if the file already
        // exists. Otherwise, we have to write the whole file and try to rename before
        // finding out.
        let file_exists = self
            .client
            .check_file_exists(&final_file_path)
            .await
            .to_object_store_err()?;
        if !overwrite && file_exists {
            return Err(HdfsError::AlreadyExists(final_file_path)).to_object_store_err();
        }

        let (tmp_file, tmp_file_path) = self.open_tmp_file(&final_file_path).await?;

        let written = async {
            for bytes in payload {
                tmp_file.hdfs_write(bytes).await?;
            }
            tmp_file.close_file().await?;
            // Without overwrite the namenode renames only onto a missing path, so of two
            // writers creating the same path (a Delta log commit) exactly one wins.
            self.client
                .rename(&tmp_file_path, &final_file_path, overwrite)
                .await
        }
        .await;
        if let Err(e) = written {
            drop(tmp_file);
            let _ = self.client.delete(&tmp_file_path, false).await;
            return Err(e).to_object_store_err();
        }

        let e_tag = self.head(location).await?.e_tag;

        Ok(PutResult {
            e_tag,
            version: None,
        })
    }

    /// Create a multipart writer that writes to a temporary file in a background task, and renames
    /// to the final destination on complete.
    #[allow(deprecated)]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let final_file_path = make_absolute_file(location);

        let (tmp_file, tmp_file_path) = self.open_tmp_file(&final_file_path).await?;

        Ok(Box::new(HdfsMultipartWriter::new(
            Arc::clone(&self.client),
            Arc::new(tmp_file),
            &tmp_file_path,
            &final_file_path,
            self.io_runtime.clone(),
        )))
    }

    /// Reads data for the specified location.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let meta = self.head(location).await?;

        options.check_preconditions(&meta)?;

        let range = options
            .range
            .map(|r| match r {
                GetRange::Bounded(range) => range,
                GetRange::Offset(offset) => offset..meta.size,
                GetRange::Suffix(suffix) => meta.size.saturating_sub(suffix)..meta.size,
            })
            .unwrap_or(0..meta.size);

        let reader = self
            .client
            .open_for_read(&make_absolute_file(location))
            .await
            .to_object_store_err()?;

        let start: usize = range
            .start
            .try_into()
            .expect("unable to convert range.start to usize");
        let end: usize = range
            .end
            .try_into()
            .expect("unable to convert range.end to usize");

        let connection = self.client.get_connection();
        let stream = ReadRangeStream::new(connection, reader.file, start, end);
        let box_stream = stream.map(|b| b.to_object_store_err()).boxed();

        let payload = GetResultPayload::Stream(box_stream);

        Ok(GetResult {
            payload,
            meta,
            range,
            attributes: Default::default(),
        })
    }

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let status = self
            .client
            .get_file_info(&make_absolute_file(location))
            .await
            .to_object_store_err()?;

        if status.isdir {
            return Err(HdfsError::IsADirectoryError(
                "Head must be called on a file".to_string(),
            ))
            .to_object_store_err();
        }

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: DateTime::<Utc>::from_timestamp(status.modification_time as i64, 0)
                .unwrap(),
            size: status
                .length
                .try_into()
                .expect("unable to convert status.length to usize"),
            e_tag: Some(get_etag(&status)),
            version: None,
        })
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        let result = self
            .client
            .delete(&make_absolute_file(location), false)
            .await
            .to_object_store_err()?;

        if !result {
            Err(HdfsError::OperationFailed(
                "failed to delete object".to_string(),
            ))
            .to_object_store_err()?
        }

        Ok(())
    }

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    ///
    /// Note: the order of returned [`ObjectMeta`] is not guaranteed
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let start_prefix = prefix.map(make_absolute_dir).unwrap_or("".to_string());
        let client = Arc::clone(&self.client);

        try_stream! {
            let mut pending = VecDeque::new();
            let initial_objects: Vec<FileStatus> = client.list_directory(&start_prefix).await.to_object_store_err()?;
            pending.extend(initial_objects.into_iter());

            while let Some(object) = pending.pop_front() {
                if object.isdir {
                    let mut objects = client.list_directory(&object.path).await.to_object_store_err()?;
                    pending.extend(objects.drain(..));
                } else {
                    yield get_object_meta(&object)?;
                }
            }
        }.boxed()
    }

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let statuses = self
            .client
            .list_directory(&prefix.map(make_absolute_dir).unwrap_or("".to_string()))
            .await
            .to_object_store_err()?;

        let mut dirs: Vec<Path> = Vec::new();
        for status in statuses.iter().filter(|s| s.isdir) {
            dirs.push(Path::parse(&status.path)?)
        }

        let mut files: Vec<ObjectMeta> = Vec::new();
        for status in statuses.iter().filter(|s| !s.isdir) {
            files.push(get_object_meta(status)?)
        }

        Ok(ListResult {
            common_prefixes: dirs,
            objects: files,
        })
    }

    /// Renames a file. This operation is guaranteed to be atomic.
    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        // Make sure the parent directory exists
        let mut parent: Vec<_> = to.parts().collect();
        parent.pop();

        if !parent.is_empty() {
            let parent_path: Path = parent.into_iter().collect();
            self.client
                .mkdir(&make_absolute_dir(&parent_path))
                .await
                .to_object_store_err()?;
        }

        Ok(self
            .client
            .rename(&make_absolute_file(from), &make_absolute_file(to), true)
            .await
            .to_object_store_err()?)
    }

    /// Renames a file only if the destination doesn't exist. This operation is guaranteed
    /// to be atomic.
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(self
            .client
            .rename(&make_absolute_file(from), &make_absolute_file(to), false)
            .await
            .to_object_store_err()?)
    }

    /// Copy an object from one path to another in the same object store.
    ///
    /// If there exists an object at the destination, it will be overwritten.
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.internal_copy(from, to, true).await
    }

    /// Copy an object from one path to another, only if destination is empty.
    ///
    /// Will return an error if the destination already has an object.
    ///
    /// Performs an atomic operation if the underlying object storage supports it.
    /// If atomic operations are not supported by the underlying object storage (like S3)
    /// it will return an error.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.internal_copy(from, to, false).await
    }
}

trait HdfsErrorConvert<T> {
    fn to_object_store_err(self) -> Result<T>;
}

impl<T> HdfsErrorConvert<T> for client::Result<T> {
    fn to_object_store_err(self) -> Result<T> {
        self.map_err(|err| match err {
            HdfsError::FileNotFound(path) => object_store::Error::NotFound {
                path: path.clone(),
                source: Box::new(HdfsError::FileNotFound(path)),
            },
            HdfsError::AlreadyExists(path) => object_store::Error::AlreadyExists {
                path: path.clone(),
                source: Box::new(HdfsError::AlreadyExists(path)),
            },
            HdfsError::NoneUnicodeInPath(path) => object_store::Error::InvalidPath {
                source: InvalidPath {
                    path: PathBuf::from(path),
                },
            },
            HdfsError::InvalidPath(path) => object_store::Error::InvalidPath {
                source: InvalidPath {
                    path: PathBuf::from(path),
                },
            },
            HdfsError::IsADirectoryError(path) => object_store::Error::Precondition {
                path: path.clone(),
                source: Box::new(HdfsError::IsADirectoryError(path)),
            },
            _ => object_store::Error::Generic {
                store: "HdfsObjectStore",
                source: Box::new(err),
            },
        })
    }
}

type PartSender = mpsc::UnboundedSender<(oneshot::Sender<Result<()>>, PutPayload)>;

// Create a fake multipart writer the creates an uploader to a temp file as a background
// task, and submits new parts to be uploaded to a queue for this task.
// A once cell is used to track whether a part has finished writing or not.
// On completing, rename the file to the actual target.
struct HdfsMultipartWriter {
    client: Arc<Client>,
    sender: Option<(JoinHandle<Result<()>>, PartSender)>,
    tmp_filename: String,
    final_filename: String,
}

impl HdfsMultipartWriter {
    fn new(
        client: Arc<Client>,
        writer: Arc<FileWriter>,
        tmp_filename: &str,
        final_filename: &str,
        io_runtime: Option<Handle>,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        let writer_handle = Self::start_writer_task(Arc::clone(&writer), receiver, io_runtime);

        Self {
            client,
            sender: Some((writer_handle, sender)),
            tmp_filename: tmp_filename.to_string(),
            final_filename: final_filename.to_string(),
        }
    }

    fn start_writer_task(
        writer: Arc<FileWriter>,
        mut part_receiver: mpsc::UnboundedReceiver<(oneshot::Sender<Result<()>>, PutPayload)>,
        io_runtime: Option<Handle>,
    ) -> JoinHandle<Result<()>> {
        let future = async move {
            'outer: loop {
                match part_receiver.recv().await {
                    Some((sender, part)) => {
                        for bytes in part {
                            if let Err(e) = writer.hdfs_write(bytes).await.to_object_store_err() {
                                let _ = sender.send(Err(e));
                                break 'outer;
                            }
                        }
                        let _ = sender.send(Ok(()));
                    }
                    None => {
                        return writer.close_file().await.to_object_store_err();
                    }
                }
            }

            // If we've reached here, a write task failed so just return error's for all new parts that come in
            while let Some((sender, _)) = part_receiver.recv().await {
                let _ = sender.send(
                    Err(HdfsError::OperationFailed(
                        "Write failed during one of the parts".to_string(),
                    ))
                    .to_object_store_err(),
                );
            }
            Err(HdfsError::OperationFailed(
                "Write failed during one of the parts".to_string(),
            ))
            .to_object_store_err()
        };

        match io_runtime {
            Some(handle) => handle.spawn(future),
            None => tokio::task::spawn(future),
        }
    }
}

impl std::fmt::Debug for HdfsMultipartWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsMultipartWriter")
            .field("tmp_filename", &self.tmp_filename)
            .field("final_filename", &self.final_filename)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for HdfsMultipartWriter {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let (result_sender, result_receiver) = oneshot::channel();

        if let Some((_, payload_sender)) = self.sender.as_ref() {
            payload_sender.send((result_sender, payload)).unwrap();
        } else {
            result_sender
                .send(
                    Err(HdfsError::OperationFailed(
                        "Cannot put part after completing or aborting".to_string(),
                    ))
                    .to_object_store_err(),
                )
                .unwrap();
        }

        async { result_receiver.await.unwrap() }.boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // Drop the sender so the task knows no more data is coming
        if let Some((handle, sender)) = self.sender.take() {
            drop(sender);

            // Wait for the writer task to finish
            handle.await??;

            self.client
                .rename(&self.tmp_filename, &self.final_filename, true)
                .await
                .to_object_store_err()?;

            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        } else {
            Err(object_store::Error::NotSupported {
                source: "Cannot call abort or complete multiple times".into(),
            })
        }
    }

    async fn abort(&mut self) -> Result<()> {
        // Drop the sender so the task knows no more data is coming
        if let Some((handle, sender)) = self.sender.take() {
            drop(sender);

            // Wait for the writer task to finish
            handle.abort();

            self.client
                .delete(&self.tmp_filename, false)
                .await
                .to_object_store_err()?;

            Ok(())
        } else {
            Err(object_store::Error::NotSupported {
                source: "Cannot call abort or complete multiple times".into(),
            })
        }
    }
}

/// ObjectStore paths always remove the leading slash, so add it back
fn make_absolute_file(path: &Path) -> String {
    format!("/{}", path.as_ref())
}

fn make_absolute_dir(path: &Path) -> String {
    if path.parts().count() > 0 {
        format!("/{}/", path.as_ref())
    } else {
        "/".to_string()
    }
}

/// Generate an ETag from file status using modification time and size.
/// See: https://httpd.apache.org/docs/2.2/mod/core.html#fileetag
fn get_etag(status: &FileStatus) -> String {
    let size = status.length;
    let mtime = status.modification_time;
    format!("{mtime:x}-{size:x}")
}

fn get_object_meta(status: &FileStatus) -> Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: Path::parse(&status.path)?,
        last_modified: DateTime::<Utc>::from_timestamp(status.modification_time as i64, 0).unwrap(),
        size: status
            .length
            .try_into()
            .expect("unable to convert status.length to usize"),
        e_tag: Some(get_etag(status)),
        version: None,
    })
}
