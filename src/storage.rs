use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::blocking::Client;

use crate::formats::{
    CANONICAL_ROW_SIZE, OctreeIndex, SERVING_ROW_SIZE, SourceMetadata, leaf_filename,
    serving_directory,
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

struct AccessTokenResponse {
    access_token: String,
    expires_in: i64,
}

struct GcsListResponse {
    items: Option<Vec<GcsObject>>,
    next_page_token: Option<String>,
}

struct GcsObject {
    name: String,
    size: Option<String>,
}

enum JsonValue {
    Object(Vec<(String, JsonValue)>),
    Array(Vec<JsonValue>),
    String(String),
    Number(String),
    Bool,
    Null,
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|value| value.to_str()) {
        Some("octree") => "application/octet-stream",
        Some("txt") => "text/plain; charset=utf-8",
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

fn skip_json_whitespace(bytes: &[u8], index: &mut usize) {
    while matches!(bytes.get(*index), Some(b' ' | b'\n' | b'\r' | b'\t')) {
        *index += 1;
    }
}

fn expect_json_byte(bytes: &[u8], index: &mut usize, expected: u8) -> Result<()> {
    skip_json_whitespace(bytes, index);
    match bytes.get(*index) {
        Some(&actual) if actual == expected => {
            *index += 1;
            Ok(())
        }
        Some(&actual) => bail!(
            "expected JSON byte {:?}, found {:?}",
            expected as char,
            actual as char
        ),
        None => bail!("unexpected end of JSON input"),
    }
}

fn parse_json_string(bytes: &[u8], index: &mut usize) -> Result<String> {
    expect_json_byte(bytes, index, b'"')?;
    let mut value = String::new();
    while let Some(&byte) = bytes.get(*index) {
        *index += 1;
        match byte {
            b'"' => return Ok(value),
            b'\\' => {
                let escaped = *bytes
                    .get(*index)
                    .ok_or_else(|| anyhow!("unexpected end of JSON escape"))?;
                *index += 1;
                match escaped {
                    b'"' => value.push('"'),
                    b'\\' => value.push('\\'),
                    b'/' => value.push('/'),
                    b'b' => value.push('\u{0008}'),
                    b'f' => value.push('\u{000C}'),
                    b'n' => value.push('\n'),
                    b'r' => value.push('\r'),
                    b't' => value.push('\t'),
                    b'u' => {
                        let digits = bytes
                            .get(*index..*index + 4)
                            .ok_or_else(|| anyhow!("unexpected end of JSON unicode escape"))?;
                        *index += 4;
                        let hex = std::str::from_utf8(digits)
                            .context("JSON unicode escape is not utf-8")?;
                        let codepoint = u16::from_str_radix(hex, 16)
                            .context("failed to parse JSON unicode escape")?;
                        let ch = char::from_u32(codepoint as u32)
                            .ok_or_else(|| anyhow!("invalid JSON unicode escape"))?;
                        value.push(ch);
                    }
                    _ => bail!("unsupported JSON escape sequence"),
                }
            }
            _ => value.push(byte as char),
        }
    }
    bail!("unterminated JSON string")
}

fn parse_json_number(bytes: &[u8], index: &mut usize) -> Result<String> {
    skip_json_whitespace(bytes, index);
    let start = *index;
    while matches!(
        bytes.get(*index),
        Some(b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
    ) {
        *index += 1;
    }
    if *index == start {
        bail!("expected JSON number");
    }
    String::from_utf8(bytes[start..*index].to_vec()).context("JSON number is not utf-8")
}

fn parse_json_array(bytes: &[u8], index: &mut usize) -> Result<Vec<JsonValue>> {
    expect_json_byte(bytes, index, b'[')?;
    let mut values = Vec::new();
    loop {
        skip_json_whitespace(bytes, index);
        if matches!(bytes.get(*index), Some(b']')) {
            *index += 1;
            return Ok(values);
        }
        values.push(parse_json_value(bytes, index)?);
        skip_json_whitespace(bytes, index);
        match bytes.get(*index) {
            Some(b',') => *index += 1,
            Some(b']') => {
                *index += 1;
                return Ok(values);
            }
            Some(&byte) => bail!("unexpected JSON byte {:?} in array", byte as char),
            None => bail!("unexpected end of JSON array"),
        }
    }
}

fn parse_json_object(bytes: &[u8], index: &mut usize) -> Result<Vec<(String, JsonValue)>> {
    expect_json_byte(bytes, index, b'{')?;
    let mut fields = Vec::new();
    loop {
        skip_json_whitespace(bytes, index);
        if matches!(bytes.get(*index), Some(b'}')) {
            *index += 1;
            return Ok(fields);
        }
        let key = parse_json_string(bytes, index)?;
        expect_json_byte(bytes, index, b':')?;
        let value = parse_json_value(bytes, index)?;
        fields.push((key, value));
        skip_json_whitespace(bytes, index);
        match bytes.get(*index) {
            Some(b',') => *index += 1,
            Some(b'}') => {
                *index += 1;
                return Ok(fields);
            }
            Some(&byte) => bail!("unexpected JSON byte {:?} in object", byte as char),
            None => bail!("unexpected end of JSON object"),
        }
    }
}

fn parse_json_value(bytes: &[u8], index: &mut usize) -> Result<JsonValue> {
    skip_json_whitespace(bytes, index);
    match bytes.get(*index) {
        Some(b'"') => parse_json_string(bytes, index).map(JsonValue::String),
        Some(b'{') => parse_json_object(bytes, index).map(JsonValue::Object),
        Some(b'[') => parse_json_array(bytes, index).map(JsonValue::Array),
        Some(b't') if bytes.get(*index..*index + 4) == Some(b"true") => {
            *index += 4;
            Ok(JsonValue::Bool)
        }
        Some(b'f') if bytes.get(*index..*index + 5) == Some(b"false") => {
            *index += 5;
            Ok(JsonValue::Bool)
        }
        Some(b'n') if bytes.get(*index..*index + 4) == Some(b"null") => {
            *index += 4;
            Ok(JsonValue::Null)
        }
        Some(b'-' | b'0'..=b'9') => parse_json_number(bytes, index).map(JsonValue::Number),
        Some(&byte) => bail!("unexpected JSON byte {:?} at offset {}", byte as char, *index),
        None => bail!("unexpected end of JSON input"),
    }
}

fn parse_json(text: &str) -> Result<JsonValue> {
    let bytes = text.as_bytes();
    let mut index = 0;
    let value = parse_json_value(bytes, &mut index)?;
    skip_json_whitespace(bytes, &mut index);
    if index != bytes.len() {
        bail!("trailing characters after JSON value");
    }
    Ok(value)
}

fn json_field<'a>(fields: &'a [(String, JsonValue)], name: &str) -> Option<&'a JsonValue> {
    fields
        .iter()
        .find_map(|(key, value)| if key == name { Some(value) } else { None })
}

fn json_string(value: &JsonValue, label: &str) -> Result<String> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        _ => bail!("{label} is not a JSON string"),
    }
}

fn json_optional_string(value: Option<&JsonValue>, label: &str) -> Result<Option<String>> {
    match value {
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(JsonValue::Null) | None => Ok(None),
        Some(_) => bail!("{label} is not a JSON string"),
    }
}

fn json_i64(value: &JsonValue, label: &str) -> Result<i64> {
    match value {
        JsonValue::Number(value) => value.parse().with_context(|| format!("failed to parse {label}")),
        _ => bail!("{label} is not a JSON number"),
    }
}

fn parse_access_token_response(text: &str) -> Result<AccessTokenResponse> {
    let JsonValue::Object(fields) = parse_json(text)? else {
        bail!("access token response is not a JSON object");
    };
    Ok(AccessTokenResponse {
        access_token: json_string(
            json_field(&fields, "access_token")
                .ok_or_else(|| anyhow!("missing access_token"))?,
            "access_token",
        )?,
        expires_in: json_i64(
            json_field(&fields, "expires_in").ok_or_else(|| anyhow!("missing expires_in"))?,
            "expires_in",
        )?,
    })
}

fn parse_gcs_object(text: &str) -> Result<GcsObject> {
    let JsonValue::Object(fields) = parse_json(text)? else {
        bail!("GCS object response is not a JSON object");
    };
    Ok(GcsObject {
        name: json_string(
            json_field(&fields, "name").ok_or_else(|| anyhow!("missing name"))?,
            "name",
        )?,
        size: json_optional_string(json_field(&fields, "size"), "size")?,
    })
}

fn parse_gcs_list_response(text: &str) -> Result<GcsListResponse> {
    let JsonValue::Object(fields) = parse_json(text)? else {
        bail!("GCS list response is not a JSON object");
    };
    let items = match json_field(&fields, "items") {
        Some(JsonValue::Array(items)) => {
            let mut objects = Vec::with_capacity(items.len());
            for item in items {
                let JsonValue::Object(item_fields) = item else {
                    bail!("GCS list item is not a JSON object");
                };
                objects.push(GcsObject {
                    name: json_string(
                        json_field(item_fields, "name").ok_or_else(|| anyhow!("missing name"))?,
                        "name",
                    )?,
                    size: json_optional_string(json_field(item_fields, "size"), "size")?,
                });
            }
            Some(objects)
        }
        Some(JsonValue::Null) | None => None,
        Some(_) => bail!("items is not a JSON array"),
    };
    Ok(GcsListResponse {
        items,
        next_page_token: json_optional_string(json_field(&fields, "nextPageToken"), "nextPageToken")?,
    })
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
                let object = parse_gcs_object(
                    &response
                        .text()
                        .with_context(|| format!("failed to read {}", root.display()))?,
                )
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

    pub fn read_optional_bytes(&self, root: &StorageRoot) -> Result<Option<Vec<u8>>> {
        match root {
            StorageRoot::Local(path) => {
                if !path.exists() {
                    return Ok(None);
                }
                self.read_bytes(root).map(Some)
            }
            StorageRoot::Gcs(location) => {
                let response = self
                    .authorized_get(&format!(
                        "{}?alt=media",
                        object_url(&location.bucket, &location.prefix)
                    ))
                    .send()
                    .with_context(|| format!("failed to read {}", root.display()))?;
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                let response = response
                    .error_for_status()
                    .with_context(|| format!("failed to read {}", root.display()))?;
                response
                    .bytes()
                    .map(|bytes| Some(bytes.to_vec()))
                    .with_context(|| format!("failed to decode {}", root.display()))
            }
        }
    }

    pub fn upload_directory(&self, local_root: &Path, remote_root: &StorageRoot) -> Result<()> {
        self.upload_directory_recursive(local_root, local_root, remote_root)
    }

    pub fn list_relative_files_recursive(&self, root: &StorageRoot) -> Result<Vec<String>> {
        match root {
            StorageRoot::Local(path) => {
                if !path.exists() {
                    return Ok(Vec::new());
                }
                let mut files = Vec::new();
                self.collect_local_files(path, path, &mut files)?;
                files.sort();
                Ok(files)
            }
            StorageRoot::Gcs(location) => {
                let mut files = Vec::new();
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
                    let listing = parse_gcs_list_response(
                        &response
                            .text()
                            .with_context(|| format!("failed to read {}", root.display()))?,
                    )
                    .with_context(|| format!("failed to parse {}", root.display()))?;

                    if let Some(items) = listing.items {
                        for item in items {
                            let relative = item
                                .name
                                .strip_prefix(&join_prefix(&location.prefix, ""))
                                .unwrap_or(&item.name)
                                .trim_start_matches('/')
                                .to_string();
                            if !relative.is_empty() {
                                files.push(relative);
                            }
                        }
                    }

                    match listing.next_page_token {
                        Some(token) => next_page_token = Some(token),
                        None => {
                            files.sort();
                            return Ok(files);
                        }
                    }
                }
            }
        }
    }

    pub fn validate_canonical_layout(
        &self,
        root: &StorageRoot,
        metadata: &SourceMetadata,
    ) -> Result<u64> {
        let canonical_root = root.join(&metadata.canonical_directory);
        let mut canonical_rows = 0;
        for part in &metadata.canonical_parts {
            canonical_rows += ensure_row_multiple(
                self.object_size(&canonical_root.join(part))?,
                CANONICAL_ROW_SIZE,
                "canonical object",
            )?;
        }
        if canonical_rows != metadata.counts.rows_written {
            bail!(
                "metadata rows_written {} does not match stored rows {}",
                metadata.counts.rows_written,
                canonical_rows,
            );
        }
        Ok(canonical_rows)
    }

    pub fn validate_serving_layout(&self, root: &StorageRoot, index: &OctreeIndex) -> Result<u64> {
        let serving_root = root.join(&serving_directory(index.depth));
        let mut serving_rows = 0;
        for morton in &index.leaves {
            serving_rows += ensure_row_multiple(
                self.object_size(&serving_root.join(&leaf_filename(*morton)))?,
                SERVING_ROW_SIZE,
                "serving object",
            )?;
        }
        Ok(serving_rows)
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
        let token = parse_access_token_response(
            &response
                .text()
                .context("failed to read metadata server token")?,
        )
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

    fn collect_local_files(
        &self,
        root: &Path,
        current: &Path,
        files: &mut Vec<String>,
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
                self.collect_local_files(root, &path, files)?;
                continue;
            }

            let relative = path
                .strip_prefix(root)
                .with_context(|| format!("failed to relativize {}", path.display()))?
                .to_string_lossy()
                .replace('\\', "/");
            files.push(relative);
        }
        Ok(())
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
