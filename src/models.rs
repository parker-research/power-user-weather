use once_cell::sync::Lazy;

use crate::fetch_data::WeatherDataSource;
use std::collections::BTreeSet;

const ARCHIVE_MODELS: [&'static str; 8] = [
    "best_match",
    "ecmwf_ifs",
    "ecmwf_ifs_analysis_long_window",
    "era5_seamless",
    "era5",
    "era5_land",
    "era5_ensemble",
    "cerra",
];

const ARCHIVE_DAILY_SUMMABLE_PRECIPITATION_MEASURES: [&'static str; 4] = [
    "rain_sum",
    "snowfall_sum",
    "precipitation_sum",
    "precipitation_hours",
];

const FORECAST_MODELS: [&'static str; 48] = [
    "best_match",
    "ecmwf_ifs",
    "ecmwf_ifs025",
    "ecmwf_aifs025_single",
    "cma_grapes_global",
    "bom_access_global",
    "icon_seamless",
    "icon_global",
    "icon_eu",
    "icon_d2",
    "metno_seamless",
    "metno_nordic",
    "dmi_harmonie_arome_europe",
    "dmi_seamless",
    "knmi_harmonie_arome_netherlands",
    "knmi_harmonie_arome_europe",
    "knmi_seamless",
    "gem_hrdps_west",
    "gem_hrdps_continental",
    "gem_regional",
    "gem_global",
    "gem_seamless",
    "ncep_hgefs025_ensemble_mean",
    "ncep_aigfs025",
    "gfs_graphcast025",
    "ncep_nam_conus",
    "ncep_nbm_conus",
    "gfs_hrrr",
    "gfs_global",
    "gfs_seamless",
    "jma_seamless",
    "jma_msm",
    "jma_gsm",
    "meteofrance_seamless",
    "meteofrance_arpege_world",
    "meteofrance_arpege_europe",
    "meteofrance_arome_france",
    "meteofrance_arome_france_hd",
    "ukmo_seamless",
    "ukmo_global_deterministic_10km",
    "ukmo_uk_deterministic_2km",
    "meteoswiss_icon_ch2",
    "meteoswiss_icon_ch1",
    "meteoswiss_icon_seamless",
    "italia_meteo_arpae_icon_2i",
    "kma_gdps",
    "kma_ldps",
    "kma_seamless",
];

const FORECAST_DAILY_SUMMABLE_PRECIPITATION_MEASURES: [&'static str; 5] = [
    "rain_sum",
    "showers_sum",
    "snowfall_sum",
    "precipitation_sum",
    "precipitation_hours",
];

const ENSEMBLE_MODELS: [&'static str; 16] = [
    "icon_seamless_eps",
    "icon_global_eps",
    "icon_eu_eps",
    "icon_d2_eps",
    "meteoswiss_icon_ch1_ensemble",
    "meteoswiss_icon_ch2_ensemble",
    "ncep_aigefs025",
    "ncep_gefs025",
    "ncep_gefs05",
    "ncep_gefs_seamless",
    "bom_access_global_ensemble",
    "gem_global_ensemble",
    "ecmwf_ifs025_ensemble",
    "ecmwf_aifs025_ensemble",
    "ukmo_global_ensemble_20km",
    "ukmo_uk_ensemble_2km",
];

const ENSEMBLE_DAILY_SUMMABLE_PRECIPITATION_MEASURES: [&'static str; 4] = [
    "rain_sum",
    "snowfall_sum",
    "precipitation_sum",
    "precipitation_hours",
];

pub static ALL_DISTINCT_MODELS: Lazy<Vec<&'static str>> = Lazy::new(|| {
    let mut seen = BTreeSet::new();

    for &model in ARCHIVE_MODELS
        .iter()
        .chain(FORECAST_MODELS.iter())
        .chain(ENSEMBLE_MODELS.iter())
    {
        seen.insert(model);
    }

    // Now, sort by length descending. Critical to ensure we match the longest substring.
    let mut seen: Vec<&'static str> = seen.into_iter().collect();
    seen.sort_by_key(|s| std::cmp::Reverse(s.len()));

    seen
});

pub fn models_for_weather_data_source(
    weather_data_source: WeatherDataSource,
) -> &'static [&'static str] {
    match weather_data_source {
        WeatherDataSource::HistoricalArchive => &ARCHIVE_MODELS,
        WeatherDataSource::ForecastStandard => &FORECAST_MODELS,
        WeatherDataSource::ForecastEnsemble => &ENSEMBLE_MODELS,
    }
}

pub fn daily_summable_precipitation_measures_for_weather_data_source(
    weather_data_source: WeatherDataSource,
) -> &'static [&'static str] {
    match weather_data_source {
        WeatherDataSource::HistoricalArchive => &ARCHIVE_DAILY_SUMMABLE_PRECIPITATION_MEASURES,
        WeatherDataSource::ForecastStandard => &FORECAST_DAILY_SUMMABLE_PRECIPITATION_MEASURES,
        WeatherDataSource::ForecastEnsemble => &ENSEMBLE_DAILY_SUMMABLE_PRECIPITATION_MEASURES,
    }
}
