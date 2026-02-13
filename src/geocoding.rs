use anyhow::{Context as _, Result};
use serde::Deserialize;

use crate::url_fetch::fetch_url_cached;

#[derive(Debug, Clone)]
pub struct Location {
    pub name: String,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Deserialize, Debug)]
struct GeocodingResult {
    results: Option<Vec<GeocodingLocation>>,
}

#[derive(Deserialize, Debug)]
struct GeocodingLocation {
    name: String,
    latitude: f64,
    longitude: f64,
    admin1: Option<String>,
    country: Option<String>,
}

pub async fn geocode_city(city: &str) -> Result<Location> {
    let url = format!(
        "https://geocoding-api.open-meteo.com/v1/search?name={}&count=1&language=en&format=json",
        urlencoding::encode(city)
    );

    let body = fetch_url_cached(&url)
        .await
        .context("Failed to fetch geocoding data")?;

    // Deserialize manually from the returned string.
    let response: GeocodingResult =
        serde_json::from_str(&body).context("Failed to parse geocoding response")?;

    let location = response
        .results
        .and_then(|mut r| r.pop())
        .context(format!("City '{}' not found", city))?;

    let full_name = format!(
        "{}, {}",
        location.name,
        location
            .admin1
            .or(location.country)
            .unwrap_or_else(|| "Unknown".to_string())
    );

    Ok(Location {
        name: full_name,
        lat: location.latitude,
        lon: location.longitude,
    })
}
