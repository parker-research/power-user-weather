use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use colored::Colorize;
use log::debug;
use polars::prelude::*;
use std::collections::{BTreeSet, HashMap};

mod fetch_data;
mod geocoding;
mod models;
mod url_fetch;

use fetch_data::{DailyDataColumnarFormat, MeasureAndModel, WeatherDataSource};
use geocoding::Location;

#[derive(Parser, Debug)]
#[command(name = "power-user-weather")]
#[command(about = "Analyze and compare precipitation data from multiple sources", long_about = None)]
struct Cli {
    /// City name (e.g., "Seattle, WA" or "New York")
    #[arg(short, long, group = "location")]
    city: Option<String>,

    /// Latitude (use with --lon)
    #[arg(long, requires = "lon", group = "location", allow_hyphen_values = true)]
    lat: Option<f64>,

    /// Longitude (use with --lat)
    #[arg(long, requires = "lat", allow_hyphen_values = true)]
    lon: Option<f64>,

    /// Start date (YYYY-MM-DD)
    #[arg(short, long)]
    start: String,

    /// End date (YYYY-MM-DD)
    #[arg(short, long)]
    end: String,

    /// Precipitation unit (mm or inch)
    #[arg(short = 'u', long, default_value = "mm")]
    unit: String,

    /// Time zone (e.g., "America/New_York", "UTC")
    #[arg(short = 'z', long, default_value = "UTC")]
    timezone: String,

    /// Include ensemble forecast models (provides confidence intervals)
    #[arg(long, default_value = "true")]
    ensemble: bool,

    /// Fetch historical archive data
    #[arg(long, default_value = "true")]
    historical: bool,

    /// Fetch forecast data
    #[arg(long, default_value = "true")]
    forecast: bool,

    /// Show detailed daily breakdown
    #[arg(short, long)]
    verbose: bool,
}

struct DataSourceResult {
    source: WeatherDataSource,
    data: DailyDataColumnarFormat,
}

/// Aggregate data by summing values across the time period for each measure-model combination
fn aggregate_data(data: &DailyDataColumnarFormat) -> HashMap<MeasureAndModel, f64> {
    let mut aggregated = HashMap::new();

    for (measure_and_model, values) in &data.data_fields {
        let sum: f64 = values.iter().filter_map(|v| *v).sum();
        aggregated.insert(
            MeasureAndModel {
                measure: measure_and_model.measure.clone(),
                model: measure_and_model.model.clone(),
            },
            sum,
        );
    }

    aggregated
}

/// Build a table showing measures as columns only, with each model as a separate row using polars
fn build_model_measure_table(aggregated_data: &HashMap<MeasureAndModel, f64>) -> Result<String> {
    // Create DataFrame.
    let df = df!(
        "Measure" => aggregated_data.keys().map(|k| k.measure.clone()).collect::<Vec<_>>(),
        "Model" => aggregated_data.keys().map(|k| k.model.clone()).collect::<Vec<_>>(),
        "Value" => aggregated_data.values().copied().collect::<Vec<_>>()
    )?;

    // De-duplicate then sort:
    let measure_values: Vec<_> = aggregated_data
        .keys()
        .map(|k| k.measure.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let df = df
        .lazy()
        .pivot(
            Selector::ByName {
                names: [PlSmallStr::from("Measure")].into(),
                strict: true,
            },
            Arc::new(df!("" => &measure_values)?),
            Selector::ByName {
                names: [PlSmallStr::from("Model")].into(),
                strict: true,
            },
            Selector::ByName {
                names: [PlSmallStr::from("Value")].into(),
                strict: true,
            },
            Expr::Agg(AggExpr::Item {
                input: Arc::new(Expr::Element),
                allow_empty: true,
            }),
            true,
            "|".into(),
        )
        .collect()?;

    // Format the output
    Ok(format!("{}", df))
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    debug!("Starting parsing arguments");

    let cli = Cli::parse();

    // Parse dates
    let start_date = NaiveDate::parse_from_str(&cli.start, "%Y-%m-%d")
        .context("Invalid start date format. Use YYYY-MM-DD")?;
    let end_date = NaiveDate::parse_from_str(&cli.end, "%Y-%m-%d")
        .context("Invalid end date format. Use YYYY-MM-DD")?;

    if end_date < start_date {
        anyhow::bail!("End date must be after start date");
    }

    // Parse precipitation unit
    let precipitation_unit = fetch_data::PrecipitationUnit::try_from(cli.unit.as_str())
        .context("Invalid precipitation unit")?;

    // Get location
    let location = if let Some(city) = cli.city {
        println!("{}", format!("🌍 Geocoding '{}'...", city).cyan());
        geocoding::geocode_city(&city).await?
    } else if let (Some(lat), Some(lon)) = (cli.lat, cli.lon) {
        Location {
            name: format!("Lat: {:.4}, Lon: {:.4}", lat, lon),
            lat,
            lon,
        }
    } else {
        anyhow::bail!("Must specify either --city or both --lat and --lon");
    };

    println!("{}", format!("📍 Location: {}", location.name).green());
    println!(
        "{}",
        format!("📅 Period: {} to {}", start_date, end_date).green()
    );
    println!();

    // Determine what data to fetch
    let now = chrono::Utc::now().date_naive();
    let is_historical = end_date < now;
    let is_forecast = start_date <= now + chrono::Duration::days(16);
    let is_mixed = start_date < now && end_date >= now;

    // Collect all precipitation data
    let mut all_data: Vec<DataSourceResult> = Vec::new();

    // Fetch historical data
    if cli.historical && (is_historical || is_mixed) {
        println!("{}", "📊 Fetching historical data...".yellow());
        let hist_end = if is_mixed {
            now - chrono::Duration::days(1)
        } else {
            end_date
        };

        match fetch_data::fetch_all_summable_precipitation_data(
            WeatherDataSource::HistoricalArchive,
            &location,
            start_date,
            hist_end,
            precipitation_unit.clone(),
            &cli.timezone,
        )
        .await
        {
            Ok(data) => {
                println!("  ✓ Historical archive data retrieved");
                all_data.push(DataSourceResult {
                    source: WeatherDataSource::HistoricalArchive,
                    data,
                });
            }
            Err(e) => println!("  ⚠ Historical data error: {:#}", e),
        }
    }

    // Fetch forecast data
    if cli.forecast && is_forecast {
        println!("{}", "🔮 Fetching forecast data...".yellow());
        let forecast_start = if is_mixed { now } else { start_date };
        let forecast_end = if end_date > now + chrono::Duration::days(16) {
            now + chrono::Duration::days(16)
        } else {
            end_date
        };

        // Standard forecast
        match fetch_data::fetch_all_summable_precipitation_data(
            WeatherDataSource::ForecastStandard,
            &location,
            forecast_start,
            forecast_end,
            precipitation_unit.clone(),
            &cli.timezone,
        )
        .await
        {
            Ok(data) => {
                println!("  ✓ Standard forecast data retrieved");
                all_data.push(DataSourceResult {
                    source: WeatherDataSource::ForecastStandard,
                    data,
                });
            }
            Err(e) => println!("  ⚠ Forecast data error: {:#}", e),
        }

        // Ensemble forecast (for confidence intervals)
        if cli.ensemble {
            match fetch_data::fetch_all_summable_precipitation_data(
                WeatherDataSource::ForecastEnsemble,
                &location,
                forecast_start,
                forecast_end,
                precipitation_unit.clone(),
                &cli.timezone,
            )
            .await
            {
                Ok(data) => {
                    println!("  ✓ Ensemble forecast data retrieved");
                    all_data.push(DataSourceResult {
                        source: WeatherDataSource::ForecastEnsemble,
                        data,
                    });
                }
                Err(e) => println!("  ⚠ Ensemble forecast error: {:#}", e),
            }
        }
    }

    if all_data.is_empty() {
        anyhow::bail!("No data retrieved from any source");
    }

    println!();

    // Display results for each data source
    for result in &all_data {
        println!("{}", "═".repeat(100).bright_blue());
        println!(
            "{}",
            format!("{} - PRECIPITATION BY MODEL AND MEASURE", result.source)
                .bright_blue()
                .bold()
        );
        println!("{}", "═".repeat(100).bright_blue());
        println!();

        let aggregated = aggregate_data(&result.data);
        let table = build_model_measure_table(&aggregated)?;
        println!("{}", table);
        println!();
    }

    // Optional: Detailed daily breakdown if verbose
    if cli.verbose {
        println!("{}", "═".repeat(100).bright_blue());
        println!("{}", "DETAILED DAILY BREAKDOWN".bright_blue().bold());
        println!("{}", "═".repeat(100).bright_blue());
        println!();

        for result in &all_data {
            println!("{}", format!("Source: {}", result.source).yellow().bold());
            println!();

            // Group by date
            let mut date_data: HashMap<String, Vec<(String, String, Option<f64>)>> = HashMap::new();

            for (measure_and_model, values) in &result.data.data_fields {
                for (i, date) in result.data.time.iter().enumerate() {
                    if i < values.len() {
                        date_data.entry(date.clone()).or_default().push((
                            measure_and_model.model.clone(),
                            measure_and_model.measure.clone(),
                            values[i],
                        ));
                    }
                }
            }

            let mut dates: Vec<_> = date_data.keys().collect();
            dates.sort();

            for date in dates {
                println!("  Date: {}", date.bright_cyan());
                if let Some(entries) = date_data.get(date) {
                    for (model, measure, value) in entries {
                        println!(
                            "    {} - {}: {} {}",
                            model,
                            measure,
                            value.map_or("".to_string(), |v| format!("{:.1}", v)),
                            cli.unit
                        );
                    }
                }
                println!();
            }
        }
    }

    println!("{}", "✨ Analysis complete!".green().bold());

    Ok(())
}
