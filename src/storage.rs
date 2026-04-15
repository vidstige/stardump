use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::formats::{CANONICAL_ROW_SIZE, PackedOctreeIndex, SourceMetadata};

fn ensure_row_multiple(size: u64, row_size: u64, label: &str) -> Result<u64> {
    if size % row_size != 0 {
        bail!("{label} size {size} is not a multiple of row size {row_size}");
    }
    Ok(size / row_size)
}

pub fn local_path(spec: &str) -> Result<PathBuf> {
    if spec.starts_with("gs://") {
        bail!("gs:// paths are no longer supported; mount the bucket and use a filesystem path")
    }
    Ok(PathBuf::from(spec))
}

pub fn read_optional_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    if !path.exists() {
        return Ok(None);
    }
    fs::read(path)
        .map(Some)
        .with_context(|| format!("failed to read {}", path.display()))
}

pub fn list_relative_files_recursive(root: &Path) -> Result<Vec<String>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    collect_local_files(root, root, &mut files)?;
    files.sort();
    Ok(files)
}

pub fn validate_canonical_layout(root: &Path, metadata: &SourceMetadata) -> Result<u64> {
    let canonical_root = root.join(&metadata.canonical_directory);
    let mut canonical_rows = 0;
    for part in &metadata.canonical_parts {
        let size = fs::metadata(canonical_root.join(part))
            .with_context(|| {
                format!(
                    "failed to read metadata for {}",
                    canonical_root.join(part).display()
                )
            })?
            .len();
        canonical_rows += ensure_row_multiple(size, CANONICAL_ROW_SIZE, "canonical object")?;
    }
    if canonical_rows != metadata.counts.rows_written {
        bail!(
            "metadata rows_written {} does not match stored rows {}",
            metadata.counts.rows_written,
            canonical_rows
        );
    }
    Ok(canonical_rows)
}

pub fn validate_packed_index_layout(root: &Path, index: &PackedOctreeIndex) -> Result<u64> {
    let path = root.join("index.octree");
    let size = fs::metadata(&path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?
        .len();
    if size != index.file_size() {
        bail!(
            "packed index size {} does not match expected {}",
            size,
            index.file_size()
        );
    }
    Ok(index.point_count)
}

fn collect_local_files(root: &Path, current: &Path, files: &mut Vec<String>) -> Result<()> {
    for entry in
        fs::read_dir(current).with_context(|| format!("failed to read {}", current.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", current.display()))?;
        let path = entry.path();
        if entry
            .file_type()
            .with_context(|| format!("failed to stat {}", path.display()))?
            .is_dir()
        {
            collect_local_files(root, &path, files)?;
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
