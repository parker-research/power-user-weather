use anyhow::Result;
use directories::ProjectDirs;
use log::debug;
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use url::Url;

/// Cache duration (1 hour)
const CACHE_TTL: Duration = Duration::from_secs(60 * 60);

/// Fetch a URL with 1-hour disk caching.
/// Returns the response body as a String.
pub async fn fetch_url_cached(url: &str) -> Result<String> {
    let cache_path = cache_file_path(url)?;

    // If cache exists and is fresh, return it.
    if let Some(contents) = read_if_fresh(&cache_path)? {
        debug!("Using cached response for URL: {}", url);
        return Ok(contents);
    }

    // Otherwise fetch from network.
    debug!("Fetching URL from API: {}", url);
    let client = Client::new();
    let response = client.get(url).send().await?;
    let response = response.error_for_status()?;
    let body = response.text().await?;

    // Write to cache
    write_cache(&cache_path, &body)?;

    Ok(body)
}

/// Build a cache file path for a URL.
fn cache_file_path(url: &str) -> Result<PathBuf> {
    let parsed = Url::parse(url)?;

    let proj_dirs = ProjectDirs::from("com", "example", "power-user-weather")
        .ok_or_else(|| anyhow::anyhow!("Could not determine cache directory"))?;

    let cache_dir = proj_dirs.cache_dir();
    fs::create_dir_all(cache_dir)?;

    // Create readable sanitized base name
    let mut base = format!(
        "{}_{}",
        parsed.host_str().unwrap_or("unknown"),
        parsed.path().replace('/', "_")
    );

    if let Some(query) = parsed.query() {
        base.push('_');
        base.push_str(query);
    }

    let sanitized = sanitize_filename::sanitize(&base);

    // Append SHA-256 hash of full URL
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    let hash = hex::encode(hasher.finalize());

    let sanitized_restricted_len = if sanitized.len() > 100 {
        &sanitized[..100]
    } else {
        &sanitized
    };

    let filename = format!("{}_{}.json", sanitized_restricted_len, &hash[..16]);
    Ok(cache_dir.join(filename))
}

/// Return file contents if cache exists and is still fresh.
fn read_if_fresh(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }

    let metadata = fs::metadata(path)?;
    let modified = metadata.modified()?;
    let age = SystemTime::now().duration_since(modified)?;

    if age < CACHE_TTL {
        let contents = fs::read_to_string(path)?;
        Ok(Some(contents))
    } else {
        debug!("Cached file exists but expired for file: {:?}", path);
        Ok(None)
    }
}

/// Write content to cache file.
fn write_cache(path: &Path, contents: &str) -> Result<()> {
    let mut file = fs::File::create(path)?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}
