use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use colored::Colorize;
use tabled::{Table, Tabled, settings::Style};

mod geocoding;
mod precipitation;

use geocoding::Location;
use precipitation::PrecipData;

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

#[derive(Tabled)]
struct SummaryRow {
    #[tabled(rename = "Source")]
    source: String,
    #[tabled(rename = "Type")]
    data_type: String,
    #[tabled(rename = "Total Precip")]
    total: String,
    #[tabled(rename = "Days")]
    days: usize,
    #[tabled(rename = "Avg/Day")]
    avg_per_day: String,
    #[tabled(rename = "Max Day")]
    max_day: String,
    #[tabled(rename = "Confidence")]
    confidence: String,
}

#[derive(Tabled)]
struct DailyRow {
    #[tabled(rename = "Date")]
    date: String,
    #[tabled(rename = "Source")]
    source: String,
    #[tabled(rename = "Precipitation")]
    precipitation: String,
    #[tabled(rename = "Min")]
    min: String,
    #[tabled(rename = "Max")]
    max: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Parse dates
    let start_date = NaiveDate::parse_from_str(&cli.start, "%Y-%m-%d")
        .context("Invalid start date format. Use YYYY-MM-DD")?;
    let end_date = NaiveDate::parse_from_str(&cli.end, "%Y-%m-%d")
        .context("Invalid end date format. Use YYYY-MM-DD")?;

    if end_date < start_date {
        anyhow::bail!("End date must be after start date");
    }

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
    let mut all_data: Vec<PrecipData> = Vec::new();

    // Fetch historical data
    if cli.historical && (is_historical || is_mixed) {
        println!("{}", "📊 Fetching historical data...".yellow());
        let hist_end = if is_mixed {
            now - chrono::Duration::days(1)
        } else {
            end_date
        };

        match precipitation::fetch_historical(
            &location,
            start_date,
            hist_end,
            &cli.unit,
            &cli.timezone,
        )
        .await
        {
            Ok(data) => {
                println!("  ✓ Historical archive data retrieved");
                all_data.push(data);
            }
            Err(e) => println!("  ⚠ Historical data error: {}", e),
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
        match precipitation::fetch_forecast(
            &location,
            forecast_start,
            forecast_end,
            &cli.unit,
            &cli.timezone,
            false,
        )
        .await
        {
            Ok(data) => {
                println!("  ✓ Standard forecast data retrieved");
                all_data.push(data);
            }
            Err(e) => println!("  ⚠ Forecast data error: {}", e),
        }

        // Ensemble forecast (for confidence intervals)
        if cli.ensemble {
            match precipitation::fetch_forecast(
                &location,
                forecast_start,
                forecast_end,
                &cli.unit,
                &cli.timezone,
                true,
            )
            .await
            {
                Ok(data) => {
                    println!("  ✓ Ensemble forecast data retrieved");
                    all_data.push(data);
                }
                Err(e) => println!("  ⚠ Ensemble forecast error: {}", e),
            }
        }
    }

    if all_data.is_empty() {
        anyhow::bail!("No data retrieved from any source");
    }

    println!();
    println!("{}", "═".repeat(80).bright_blue());
    println!("{}", "PRECIPITATION SUMMARY".bright_blue().bold());
    println!("{}", "═".repeat(80).bright_blue());
    println!();

    // Build summary table
    let mut summary_rows = Vec::new();
    for data in &all_data {
        let total: f64 = data.daily_values.values().sum();
        let avg = if !data.daily_values.is_empty() {
            total / data.daily_values.len() as f64
        } else {
            0.0
        };
        let max = data.daily_values.values().cloned().fold(0.0f64, f64::max);

        let confidence = if let (Some(min_vals), Some(max_vals)) =
            (&data.confidence_min, &data.confidence_max)
        {
            let total_min: f64 = min_vals.values().sum();
            let total_max: f64 = max_vals.values().sum();
            format!("{:.1}-{:.1} {}", total_min, total_max, cli.unit)
        } else {
            "N/A".to_string()
        };

        summary_rows.push(SummaryRow {
            source: data.source.to_string(),
            data_type: data.data_type.clone(),
            total: format!("{:.1} {}", total, cli.unit),
            days: data.daily_values.len(),
            avg_per_day: format!("{:.2} {}", avg, cli.unit),
            max_day: format!("{:.1} {}", max, cli.unit),
            confidence,
        });
    }

    let summary_table = Table::new(summary_rows).with(Style::modern()).to_string();
    println!("{}", summary_table);

    // Detailed daily breakdown if verbose
    if cli.verbose {
        println!();
        println!("{}", "═".repeat(80).bright_blue());
        println!("{}", "DAILY BREAKDOWN".bright_blue().bold());
        println!("{}", "═".repeat(80).bright_blue());
        println!();

        let mut daily_rows = Vec::new();
        for data in &all_data {
            let mut dates: Vec<_> = data.daily_values.keys().collect();
            dates.sort();

            for date in dates {
                let precip = data.daily_values[date];
                let min = data
                    .confidence_min
                    .as_ref()
                    .and_then(|m| m.get(date))
                    .map(|v| format!("{:.1}", v))
                    .unwrap_or_else(|| "-".to_string());
                let max = data
                    .confidence_max
                    .as_ref()
                    .and_then(|m| m.get(date))
                    .map(|v| format!("{:.1}", v))
                    .unwrap_or_else(|| "-".to_string());

                daily_rows.push(DailyRow {
                    date: date.to_string(),
                    source: data.source.to_string(),
                    precipitation: format!("{:.1} {}", precip, cli.unit),
                    min,
                    max,
                });
            }
        }

        let daily_table = Table::new(daily_rows).with(Style::modern()).to_string();
        println!("{}", daily_table);
    }

    println!();
    println!("{}", "✨ Analysis complete!".green().bold());

    Ok(())
}
