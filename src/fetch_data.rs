use anyhow::{Context as _, Result};
use chrono::NaiveDate;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::{self, Display};

use crate::geocoding::Location;
use crate::models::ALL_DISTINCT_MODELS;
use crate::url_fetch::fetch_url_cached;

#[derive(Debug, Clone, Copy)]
pub enum WeatherDataSource {
    HistoricalArchive,
    ForecastStandard,
    ForecastEnsemble,
}

impl fmt::Display for WeatherDataSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WeatherDataSource::HistoricalArchive => write!(f, "Historical Archive"),
            WeatherDataSource::ForecastStandard => write!(f, "Standard Forecast"),
            WeatherDataSource::ForecastEnsemble => write!(f, "Ensemble Forecast"),
        }
    }
}

#[derive(Deserialize, Debug)]
struct DailyDataResponseFullResponse {
    // Many other fields here, but we use this struct to extract only the one we want.
    daily: Option<DailyDataRawColumnarFormat>,
}

#[derive(Debug, Deserialize)]
struct DailyDataRawColumnarFormat {
    time: Vec<String>,

    /// The keys of these rows are distinct for each measure for each model.
    /// For example, `rain_sum_cma_grapes_global`, `rain_sum_ecmwf_ifs025`, etc.
    #[serde(flatten)]
    data_fields: HashMap<String, Vec<Option<f64>>>,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MeasureAndModel {
    pub measure: String,
    pub model: String,
}

#[derive(Debug)]
pub struct DailyDataColumnarFormat {
    pub time: Vec<String>,

    pub data_fields: HashMap<MeasureAndModel, Vec<Option<f64>>>,
}

#[derive(Debug, Clone)]
pub enum PrecipitationUnit {
    Millimeters,
    Inches,
}

impl Display for PrecipitationUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Millimeters => write!(f, "mm"),
            Self::Inches => write!(f, "inch"),
        }
    }
}

impl From<PrecipitationUnit> for String {
    /// Default to mm
    fn from(value: PrecipitationUnit) -> Self {
        value.to_string()
    }
}

impl TryFrom<&str> for PrecipitationUnit {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "inch" => Ok(Self::Inches),
            "mm" => Ok(Self::Millimeters),
            _ => anyhow::bail!("Invalid precipitation unit: {}", value),
        }
    }
}

fn response_key_to_measure_and_model(key: String) -> Result<MeasureAndModel> {
    // Model is whichever ALL_DISTINCT_MODELS value the key ends with.
    // Critical assumption: ALL_DISTINCT_MODELS is sorted by length descending.
    // Critical to ensure we match the longest substring.
    let model: String = ALL_DISTINCT_MODELS
        .iter()
        .find(|possible_model| key.ends_with(*possible_model))
        .map(|m| m.to_string())
        .ok_or_else(|| anyhow::anyhow!("No matching model for field: {}", key))?;

    // Remove "_{model}" from the end of the key.
    let measure = key
        .strip_suffix(&format!("_{}", model))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Key does not contain expected separator before model: {}",
                key
            )
        })?
        .to_string();

    Ok(MeasureAndModel { measure, model })
}

fn decode_response_to_daily_data_columnar_format(
    response: String,
) -> Result<DailyDataColumnarFormat> {
    let response: DailyDataResponseFullResponse =
        serde_json::from_str(&response).context("Failed to parse weather data response")?;

    let response: DailyDataRawColumnarFormat = response
        .daily
        .ok_or_else(|| anyhow::anyhow!("No daily data in response"))?;

    let better_data_fields = response
        .data_fields
        .into_iter()
        .map(|(key, value)| {
            response_key_to_measure_and_model(key)
                .map(|measure_and_model| (measure_and_model, value))
        })
        .collect::<Result<_, _>>()?;

    Ok(DailyDataColumnarFormat {
        time: response.time,
        data_fields: better_data_fields,
    })
}

/// Fetch daily weather data into a Daily Data Columnar Format.
pub async fn fetch_weather_data(
    url_base: &str,
    location: &Location,
    start_date: NaiveDate,
    end_date: NaiveDate,
    precipitation_unit: PrecipitationUnit,
    timezone: &str,
    models: &Vec<&str>,
    daily_measures: &Vec<&str>,
) -> Result<DailyDataColumnarFormat> {
    let url = format!(
        "https://{url_base}?\
         latitude={}&longitude={}&\
         start_date={}&end_date={}&\
         daily={}&\
         precipitation_unit={}&\
         timezone={}&models={}",
        location.lat,
        location.lon,
        start_date,
        end_date,
        daily_measures.join(","),
        precipitation_unit,
        timezone,
        models.join(",")
    );

    let response: String = fetch_url_cached(&url)
        .await
        .context("Failed to fetch data")?;

    let daily = decode_response_to_daily_data_columnar_format(response)?;

    Ok(daily)
}

/// Fetch all summable precipitation measures for all models.
pub async fn fetch_all_summable_precipitation_data(
    weather_data_source: WeatherDataSource,
    location: &Location,
    start_date: NaiveDate,
    end_date: NaiveDate,
    precipitation_unit: PrecipitationUnit,
    timezone: &str,
) -> Result<DailyDataColumnarFormat> {
    let url_base = match weather_data_source {
        WeatherDataSource::HistoricalArchive => "archive-api.open-meteo.com/v1/archive",
        WeatherDataSource::ForecastStandard => "api.open-meteo.com/v1/forecast",
        WeatherDataSource::ForecastEnsemble => "ensemble-api.open-meteo.com/v1/ensemble",
    };

    let models = Vec::from(crate::models::models_for_weather_data_source(
        weather_data_source,
    ));
    let daily_measures = Vec::from(
        crate::models::daily_summable_precipitation_measures_for_weather_data_source(
            weather_data_source,
        ),
    );

    fetch_weather_data(
        url_base,
        location,
        start_date,
        end_date,
        precipitation_unit,
        timezone,
        &models,
        &daily_measures,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MODELS: [&str; 5] = [
        "meteoswiss_icon_seamless",
        "italia_meteo_arpae_icon_2i",
        "kma_gdps",
        "kma_ldps",
        "kma_seamless",
    ];

    const TEST_MEASURES: [&str; 5] = [
        "rain_sum",
        "showers_sum",
        "snowfall_sum",
        "precipitation_sum",
        "precipitation_hours",
    ];

    #[test]
    fn parses_some_measure_model_combinations() {
        // Note: icon_seamless is a substring of certain other ones.
        let result =
            response_key_to_measure_and_model("rain_sum_meteoswiss_icon_seamless".to_string())
                .expect("Expected valid parse");
        assert_eq!(result.measure, "rain_sum");
        assert_eq!(result.model, "meteoswiss_icon_seamless");
    }

    #[test]
    fn parses_all_measure_model_combinations() {
        for measure in TEST_MEASURES {
            for model in TEST_MODELS {
                let key = format!("{}_{}", measure, model);

                let result =
                    response_key_to_measure_and_model(key.clone()).expect("Expected valid parse");

                assert_eq!(result.measure, measure);
                assert_eq!(result.model, model);
            }
        }
    }

    #[test]
    fn errors_when_no_model_matches() {
        let key = "rain_sum_unknown_model".to_string();
        let result = response_key_to_measure_and_model(key);

        assert!(result.is_err());
    }

    #[test]
    fn errors_when_separator_missing() {
        // Ends with a valid model but missing underscore separator.
        // rain_sum AND meteoswiss_icon_seamless
        let key = "rain_summeteoswiss_icon_seamless".to_string();
        let result = response_key_to_measure_and_model(key);

        assert!(result.is_err());
    }

    #[test]
    fn selects_full_model_when_models_overlap() {
        // Add an overlapping case to ensure longest match works correctly
        // (Only meaningful if ALL_DISTINCT_MODELS ordering changes)
        let key = "rain_sum_kma_gdps".to_string();

        let result = response_key_to_measure_and_model(key).expect("Expected valid parse");

        assert_eq!(result.measure, "rain_sum");
        assert_eq!(result.model, "kma_gdps");
    }

    #[test]
    fn measure_can_contain_underscores() {
        let key = "precipitation_hours_kma_ldps".to_string();

        let result = response_key_to_measure_and_model(key).expect("Expected valid parse");

        assert_eq!(result.measure, "precipitation_hours");
        assert_eq!(result.model, "kma_ldps");
    }

    #[test]
    fn parse_response_all_floats() {
        let response_json = r#"
{
    "latitude": 40.710335,
    "longitude": -73.99308,
    "generationtime_ms": 1.6531944274902344,
    "utc_offset_seconds": 0,
    "timezone": "GMT",
    "timezone_abbreviation": "GMT",
    "elevation": 51.0,
    "daily_units": {
        "time": "iso8601",
        "rain_sum_best_match": "mm",
        "showers_sum_best_match": "mm"
    },
    "daily": {
        "time": [
            "2026-02-13",
            "2026-02-14",
            "2026-02-15",
            "2026-02-16",
            "2026-02-17",
            "2026-02-18",
            "2026-02-19",
            "2026-02-20",
            "2026-02-21"
        ],
        "rain_sum_best_match": [
            0.00,
            0.50,
            0.00,
            0.10,
            0.00,
            0.30,
            0.30,
            2.60,
            0.70
        ],
        "showers_sum_best_match": [
            0.00,
            0.00,
            0.00,
            0.00,
            0.00,
            0.00,
            0.00,
            0.00,
            0.00
        ]
    }
}
    "#;

        // Confirm this is valid JSON.
        let _: serde_json::Value =
            serde_json::from_str(response_json).expect("Failed to parse JSON");

        let decode = decode_response_to_daily_data_columnar_format(response_json.to_string());

        assert!(decode.is_ok());

        let expected_time = vec![
            "2026-02-13",
            "2026-02-14",
            "2026-02-15",
            "2026-02-16",
            "2026-02-17",
            "2026-02-18",
            "2026-02-19",
            "2026-02-20",
            "2026-02-21",
        ];

        assert_eq!(decode.unwrap().time, expected_time);
    }

    #[test]
    fn parse_response_mixed_nulls_and_floats() {
        let response_json = r#"
{
    "latitude": 40.710335,
    "longitude": -73.99308,
    "generationtime_ms": 1.6531944274902344,
    "utc_offset_seconds": 0,
    "timezone": "GMT",
    "timezone_abbreviation": "GMT",
    "elevation": 51.0,
    "daily_units": {
        "time": "iso8601",
        "rain_sum_best_match": "mm",
        "showers_sum_best_match": "mm"
    },
    "daily": {
        "time": [
            "2026-02-13",
            "2026-02-14",
            "2026-02-15",
            "2026-02-16",
            "2026-02-17",
            "2026-02-18",
            "2026-02-19",
            "2026-02-20",
            "2026-02-21"
        ],
        "rain_sum_best_match": [
            0.00,
            0.50,
            0.00,
            0.10,
            0.00,
            null,
            null,
            null,
            null
        ],
        "showers_sum_best_match": [
            0.00,
            0.00,
            0.00,
            0.00,
            0.00,
            0.00,
            0.00,
            0.00,
            null
        ]
    }
}
    "#;

        // Confirm this is valid JSON.
        let _: serde_json::Value =
            serde_json::from_str(response_json).expect("Failed to parse JSON");

        let decode = decode_response_to_daily_data_columnar_format(response_json.to_string());

        assert!(decode.is_ok());

        let expected_time = vec![
            "2026-02-13",
            "2026-02-14",
            "2026-02-15",
            "2026-02-16",
            "2026-02-17",
            "2026-02-18",
            "2026-02-19",
            "2026-02-20",
            "2026-02-21",
        ];

        assert_eq!(decode.unwrap().time, expected_time);
    }
}
