use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::blocking::Client;
use serde::Deserialize;

use crate::formats::{
    CANONICAL_ROW_SIZE, OctreeIndex, RunMetadata, SERVING_ROW_SIZE, leaf_filename,
};

const METADATA_SERVER_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageRoot {
    Local(PathBuf),
    Gcs(GcsRoot),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcsRoot {
    bucket: String,
    prefix: String,
}

#[derive(Clone)]
pub struct StorageClient {
    inner: Arc<StorageClientInner>,
}

struct StorageClientInner {
    http: Client,
    token: Mutex<Option<CachedToken>>,
}

#[derive(Clone)]
struct CachedToken {
    value: String,
    expires_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct AccessTokenResponse {
    access_token: String,
    expires_in: i64,
}

#[derive(Deserialize)]
struct GcsListResponse {
    items: Option<Vec<GcsObject>>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct GcsObject {
    name: String,
    size: Option<String>,
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|value| value.to_str()) {
        Some("json") | Some("octree") => "application/json",
        _ => "application/octet-stream",
    }
}

fn ensure_row_multiple(size: u64, row_size: u64, label: &str) -> Result<u64> {
    if size % row_size != 0 {
        bail!("{label} size {size} is not a multiple of row size {row_size}");
    }
    Ok(size / row_size)
}

fn join_prefix(prefix: &str, relative: &str) -> String {
    match (prefix.trim_matches('/'), relative.trim_matches('/')) {
        ("", relative) => relative.to_string(),
        (prefix, "") => prefix.to_string(),
        (prefix, relative) => format!("{prefix}/{relative}"),
    }
}

fn object_url(bucket: &str, object: &str) -> String {
    format!(
        "https://storage.googleapis.com/storage/v1/b/{bucket}/o/{}",
        urlencoding::encode(object)
    )
}

fn upload_url(bucket: &str, object: &str) -> String {
    format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=media&name={}",
        urlencoding::encode(object)
    )
}

impl StorageRoot {
    pub fn parse(spec: &str) -> Result<Self> {
        if let Some(path) = spec.strip_prefix("gs://") {
            let (bucket, prefix) = path
                .split_once('/')
                .map_or((path, ""), |(bucket, prefix)| (bucket, prefix));
            if bucket.is_empty() {
                bail!("missing bucket in storage root {spec}");
            }
            return Ok(Self::Gcs(GcsRoot {
                bucket: bucket.to_string(),
                prefix: prefix.trim_matches('/').to_string(),
            }));
        }

        Ok(Self::Local(PathBuf::from(spec)))
    }

    pub fn join(&self, relative: &str) -> Self {
        match self {
            Self::Local(path) => Self::Local(path.join(relative)),
            Self::Gcs(root) => Self::Gcs(GcsRoot {
                bucket: root.bucket.clone(),
                prefix: join_prefix(&root.prefix, relative),
            }),
        }
    }

    pub fn as_local_path(&self) -> Option<&Path> {
        match self {
            Self::Local(path) => Some(path),
            Self::Gcs(_) => None,
        }
    }

    pub fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::Gcs(root) => {
                if root.prefix.is_empty() {
                    format!("gs://{}", root.bucket)
                } else {
                    format!("gs://{}/{}", root.bucket, root.prefix)
                }
            }
        }
    }
}

impl StorageClient {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            inner: Arc::new(StorageClientInner {
                http,
                token: Mutex::new(None),
            }),
        })
    }

    pub fn object_size(&self, root: &StorageRoot) -> Result<u64> {
        match root {
            StorageRoot::Local(path) => Ok(fs::metadata(path)
                .with_context(|| format!("failed to read metadata for {}", path.display()))?
                .len()),
            StorageRoot::Gcs(location) => {
                let response = self
                    .authorized_get(&object_url(&location.bucket, &location.prefix))
                    .send()
                    .with_context(|| format!("failed to read {}", root.display()))?;
                let response = response
                    .error_for_status()
                    .with_context(|| format!("failed to read {}", root.display()))?;
                let object: GcsObject = response
                    .json()
                    .with_context(|| format!("failed to parse {}", root.display()))?;
                object
                    .size
                    .ok_or_else(|| anyhow!("missing object size for {}", root.display()))?
                    .parse()
                    .with_context(|| format!("failed to parse object size for {}", root.display()))
            }
        }
    }

    pub fn read_bytes(&self, root: &StorageRoot) -> Result<Vec<u8>> {
        match root {
            StorageRoot::Local(path) => {
                fs::read(path).with_context(|| format!("failed to read {}", path.display()))
            }
            StorageRoot::Gcs(location) => {
                let response = self
                    .authorized_get(&format!(
                        "{}?alt=media",
                        object_url(&location.bucket, &location.prefix)
                    ))
                    .send()
                    .with_context(|| format!("failed to read {}", root.display()))?;
                let response = response
                    .error_for_status()
                    .with_context(|| format!("failed to read {}", root.display()))?;
                response
                    .bytes()
                    .map(|bytes| bytes.to_vec())
                    .with_context(|| format!("failed to decode {}", root.display()))
            }
        }
    }

    pub fn upload_directory(&self, local_root: &Path, remote_root: &StorageRoot) -> Result<()> {
        self.upload_directory_recursive(local_root, local_root, remote_root)
    }

    pub fn validate_run_layout(
        &self,
        root: &StorageRoot,
        metadata: &RunMetadata,
        index: &OctreeIndex,
    ) -> Result<()> {
        if metadata.octree_depth != index.depth {
            bail!(
                "metadata depth {} does not match index depth {}",
                metadata.octree_depth,
                index.depth
            );
        }
        if metadata.bounds != index.bounds {
            bail!("metadata bounds do not match index bounds");
        }

        let canonical_root = root.join(&metadata.canonical_directory);
        let serving_root = root.join(&metadata.serving_directory);

        let mut canonical_rows = 0;
        for part in &metadata.canonical_parts {
            canonical_rows += ensure_row_multiple(
                self.object_size(&canonical_root.join(part))?,
                CANONICAL_ROW_SIZE,
                "canonical object",
            )?;
        }

        let mut serving_rows = 0;
        let mut expected_files = BTreeSet::new();
        for morton in &index.leaves {
            let filename = leaf_filename(*morton);
            expected_files.insert(filename.clone());
            serving_rows += ensure_row_multiple(
                self.object_size(&serving_root.join(&filename))?,
                SERVING_ROW_SIZE,
                "serving object",
            )?;
        }

        let actual_files = self.list_filenames(&serving_root)?;
        if actual_files != expected_files {
            bail!("serving directory contents do not match index leaves");
        }
        if canonical_rows != serving_rows {
            bail!(
                "canonical rows {} do not match serving rows {}",
                canonical_rows,
                serving_rows
            );
        }
        if canonical_rows != metadata.counts.rows_in_bounds {
            bail!(
                "metadata rows_in_bounds {} does not match stored rows {}",
                metadata.counts.rows_in_bounds,
                canonical_rows
            );
        }

        Ok(())
    }

    fn authorized_get(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        self.inner
            .http
            .get(url)
            .bearer_auth(self.access_token().unwrap_or_default())
    }

    fn access_token(&self) -> Result<String> {
        let now = Utc::now();
        if let Some(token) = self
            .inner
            .token
            .lock()
            .expect("token mutex poisoned")
            .as_ref()
            .filter(|token| token.expires_at - ChronoDuration::seconds(60) > now)
            .cloned()
        {
            return Ok(token.value);
        }

        let token = self
            .fetch_metadata_token()
            .or_else(|_| self.fetch_gcloud_token())?;
        *self.inner.token.lock().expect("token mutex poisoned") = Some(token.clone());
        Ok(token.value)
    }

    fn fetch_metadata_token(&self) -> Result<CachedToken> {
        let response = self
            .inner
            .http
            .get(METADATA_SERVER_URL)
            .header("Metadata-Flavor", "Google")
            .timeout(Duration::from_secs(2))
            .send()
            .context("failed to contact metadata server")?;
        let response = response
            .error_for_status()
            .context("failed to fetch metadata server token")?;
        let token: AccessTokenResponse = response
            .json()
            .context("failed to parse metadata server token")?;
        Ok(CachedToken {
            value: token.access_token,
            expires_at: Utc::now() + ChronoDuration::seconds(token.expires_in),
        })
    }

    fn fetch_gcloud_token(&self) -> Result<CachedToken> {
        let output = Command::new("gcloud")
            .args(["auth", "print-access-token"])
            .output()
            .context("failed to execute gcloud auth print-access-token")?;
        if !output.status.success() {
            bail!(
                "gcloud auth print-access-token failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(CachedToken {
            value: String::from_utf8(output.stdout)
                .context("gcloud returned non-utf8 access token")?
                .trim()
                .to_string(),
            expires_at: Utc::now() + ChronoDuration::minutes(5),
        })
    }

    fn list_filenames(&self, root: &StorageRoot) -> Result<BTreeSet<String>> {
        match root {
            StorageRoot::Local(path) => {
                let mut files = BTreeSet::new();
                for entry in fs::read_dir(path)
                    .with_context(|| format!("failed to read {}", path.display()))?
                {
                    let entry =
                        entry.with_context(|| format!("failed to read {}", path.display()))?;
                    if entry
                        .file_type()
                        .with_context(|| format!("failed to stat {}", entry.path().display()))?
                        .is_file()
                    {
                        files.insert(entry.file_name().to_string_lossy().into_owned());
                    }
                }
                Ok(files)
            }
            StorageRoot::Gcs(location) => {
                let mut files = BTreeSet::new();
                let mut next_page_token = None::<String>;
                loop {
                    let mut url = format!(
                        "https://storage.googleapis.com/storage/v1/b/{}/o?prefix={}",
                        location.bucket,
                        urlencoding::encode(&join_prefix(&location.prefix, ""))
                    );
                    if let Some(token) = &next_page_token {
                        url.push_str("&pageToken=");
                        url.push_str(&urlencoding::encode(token));
                    }

                    let response = self
                        .authorized_get(&url)
                        .send()
                        .with_context(|| format!("failed to list {}", root.display()))?;
                    let response = response
                        .error_for_status()
                        .with_context(|| format!("failed to list {}", root.display()))?;
                    let listing: GcsListResponse = response
                        .json()
                        .with_context(|| format!("failed to parse {}", root.display()))?;

                    if let Some(items) = listing.items {
                        for item in items {
                            let filename = item.name.rsplit('/').next().unwrap_or("");
                            files.insert(filename.to_owned());
                        }
                    }

                    match listing.next_page_token {
                        Some(token) => next_page_token = Some(token),
                        None => return Ok(files),
                    }
                }
            }
        }
    }

    fn upload_directory_recursive(
        &self,
        local_root: &Path,
        current: &Path,
        remote_root: &StorageRoot,
    ) -> Result<()> {
        for entry in fs::read_dir(current)
            .with_context(|| format!("failed to read {}", current.display()))?
        {
            let entry = entry.with_context(|| format!("failed to read {}", current.display()))?;
            let path = entry.path();
            if entry
                .file_type()
                .with_context(|| format!("failed to stat {}", path.display()))?
                .is_dir()
            {
                self.upload_directory_recursive(local_root, &path, remote_root)?;
                continue;
            }

            let relative = path
                .strip_prefix(local_root)
                .with_context(|| format!("failed to relativize {}", path.display()))?
                .to_string_lossy()
                .replace('\\', "/");
            let bytes =
                fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
            self.write_bytes(&remote_root.join(&relative), &bytes, content_type(&path))?;
        }
        Ok(())
    }

    fn write_bytes(&self, root: &StorageRoot, bytes: &[u8], content_type: &str) -> Result<()> {
        match root {
            StorageRoot::Local(path) => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("failed to create {}", parent.display()))?;
                }
                fs::write(path, bytes)
                    .with_context(|| format!("failed to write {}", path.display()))
            }
            StorageRoot::Gcs(location) => {
                let response = self
                    .inner
                    .http
                    .post(upload_url(&location.bucket, &location.prefix))
                    .bearer_auth(self.access_token()?)
                    .header("content-type", content_type)
                    .body(bytes.to_vec())
                    .send()
                    .with_context(|| format!("failed to upload {}", root.display()))?;
                response
                    .error_for_status()
                    .with_context(|| format!("failed to upload {}", root.display()))?;
                Ok(())
            }
        }
    }
}
