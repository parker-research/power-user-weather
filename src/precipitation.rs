use anyhow::{Context, Result};
use chrono::NaiveDate;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;

use crate::geocoding::Location;

#[derive(Debug, Clone, Copy)]
pub enum PrecipSource {
    HistoricalArchive,
    ForecastStandard,
    ForecastEnsemble,
}

impl fmt::Display for PrecipSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PrecipSource::HistoricalArchive => write!(f, "Historical Archive"),
            PrecipSource::ForecastStandard => write!(f, "Standard Forecast"),
            PrecipSource::ForecastEnsemble => write!(f, "Ensemble Forecast"),
        }
    }
}

#[derive(Debug)]
pub struct PrecipData {
    pub source: PrecipSource,
    pub data_type: String,
    pub daily_values: HashMap<NaiveDate, f64>,
    pub confidence_min: Option<HashMap<NaiveDate, f64>>,
    pub confidence_max: Option<HashMap<NaiveDate, f64>>,
}

#[derive(Deserialize, Debug)]
struct HistoricalResponse {
    daily: Option<DailyData>,
}

#[derive(Deserialize, Debug)]
struct ForecastResponse {
    daily: Option<DailyData>,
}

#[derive(Deserialize, Debug)]
struct DailyData {
    time: Vec<String>,
    precipitation_sum: Option<Vec<Option<f64>>>,
    precipitation: Option<Vec<Option<f64>>>,
    // Ensemble data
    precipitation_sum_mean: Option<Vec<Option<f64>>>,
    precipitation_sum_min: Option<Vec<Option<f64>>>,
    precipitation_sum_max: Option<Vec<Option<f64>>>,
}

/// Fetch historical archive data (observed/actual precipitation)
pub async fn fetch_historical(
    location: &Location,
    start_date: NaiveDate,
    end_date: NaiveDate,
    unit: &str,
    timezone: &str,
) -> Result<PrecipData> {
    let url = format!(
        "https://archive-api.open-meteo.com/v1/archive?\
         latitude={}&longitude={}&\
         start_date={}&end_date={}&\
         daily=precipitation_sum&\
         precipitation_unit={}&\
         timezone={}",
        location.lat, location.lon, start_date, end_date, unit, timezone
    );

    let response: HistoricalResponse = reqwest::get(&url)
        .await
        .context("Failed to fetch historical data")?
        .json()
        .await
        .context("Failed to parse historical response")?;

    let daily = response.daily.context("No daily data in response")?;
    let precip_values = daily
        .precipitation_sum
        .context("No precipitation_sum in response")?;

    let mut daily_values = HashMap::new();
    for (date_str, precip_opt) in daily.time.iter().zip(precip_values.iter()) {
        let date =
            NaiveDate::parse_from_str(date_str, "%Y-%m-%d").context("Failed to parse date")?;
        if let Some(precip) = precip_opt {
            daily_values.insert(date, *precip);
        } else {
            daily_values.insert(date, 0.0);
        }
    }

    Ok(PrecipData {
        source: PrecipSource::HistoricalArchive,
        data_type: "Observed".to_string(),
        daily_values,
        confidence_min: None,
        confidence_max: None,
    })
}

/// Fetch forecast data (predicted precipitation)
pub async fn fetch_forecast(
    location: &Location,
    start_date: NaiveDate,
    end_date: NaiveDate,
    unit: &str,
    timezone: &str,
    ensemble: bool,
) -> Result<PrecipData> {
    let base_url = if ensemble {
        "https://ensemble-api.open-meteo.com/v1/ensemble"
    } else {
        "https://api.open-meteo.com/v1/forecast"
    };

    let daily_params = if ensemble {
        "precipitation_sum_mean,precipitation_sum_min,precipitation_sum_max"
    } else {
        "precipitation_sum"
    };

    let url = format!(
        "{}?\
         latitude={}&longitude={}&\
         start_date={}&end_date={}&\
         daily={}&\
         precipitation_unit={}&\
         timezone={}",
        base_url, location.lat, location.lon, start_date, end_date, daily_params, unit, timezone
    );

    let response: ForecastResponse = reqwest::get(&url)
        .await
        .context("Failed to fetch forecast data")?
        .json()
        .await
        .context("Failed to parse forecast response")?;

    let daily = response.daily.context("No daily data in response")?;

    let mut daily_values = HashMap::new();
    let mut confidence_min = HashMap::new();
    let mut confidence_max = HashMap::new();

    if ensemble {
        // Process ensemble data with confidence intervals
        let precip_mean = daily
            .precipitation_sum_mean
            .context("No precipitation_sum_mean in ensemble response")?;
        let precip_min = daily
            .precipitation_sum_min
            .context("No precipitation_sum_min in ensemble response")?;
        let precip_max = daily
            .precipitation_sum_max
            .context("No precipitation_sum_max in ensemble response")?;

        for i in 0..daily.time.len() {
            let date = NaiveDate::parse_from_str(&daily.time[i], "%Y-%m-%d")
                .context("Failed to parse date")?;

            daily_values.insert(date, precip_mean[i].unwrap_or(0.0));
            confidence_min.insert(date, precip_min[i].unwrap_or(0.0));
            confidence_max.insert(date, precip_max[i].unwrap_or(0.0));
        }

        Ok(PrecipData {
            source: PrecipSource::ForecastEnsemble,
            data_type: "Predicted (Ensemble)".to_string(),
            daily_values,
            confidence_min: Some(confidence_min),
            confidence_max: Some(confidence_max),
        })
    } else {
        // Process standard forecast data
        let precip_values = daily
            .precipitation_sum
            .or(daily.precipitation)
            .context("No precipitation data in response")?;

        for (date_str, precip_opt) in daily.time.iter().zip(precip_values.iter()) {
            let date =
                NaiveDate::parse_from_str(date_str, "%Y-%m-%d").context("Failed to parse date")?;
            daily_values.insert(date, precip_opt.unwrap_or(0.0));
        }

        Ok(PrecipData {
            source: PrecipSource::ForecastStandard,
            data_type: "Predicted".to_string(),
            daily_values,
            confidence_min: None,
            confidence_max: None,
        })
    }
}
