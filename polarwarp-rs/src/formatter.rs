use polars::prelude::*;
use num_format::{Locale, ToFormattedString};
use crate::types::Result;

/// Format numeric values with commas
pub fn format_with_commas(value: f64) -> String {
    if value.fract() == 0.0 {
        // Integer value
        return value.abs() as u64
            .to_formatted_string(&Locale::en);
    } else {
        // Float value, format with 2 decimal places
        let formatted = format!("{:.2}", value);
        let parts: Vec<&str> = formatted.split('.').collect();
        
        if parts.len() == 2 {
            let int_part = parts[0].parse::<i64>().unwrap_or(0)
                .to_formatted_string(&Locale::en);
            return format!("{}.{}", int_part, parts[1]);
        }
        
        // Fallback to simple formatting
        return formatted;
    }
}

/// Format a DataFrame for display by converting numeric columns to strings with commas
pub fn format_dataframe_for_display(df: &DataFrame, columns_to_format: &[&str]) -> Result<DataFrame> {
    let mut formatted_df = df.clone();
    
    for col_name in columns_to_format {
        if let Ok(col) = df.column(col_name) {
            if col.dtype().is_numeric() {
                let formatted_series = col.f64()?
                    .into_iter()
                    .map(|opt_val| opt_val.map(format_with_commas))
                    .collect::<StringChunked>()
                    .into_series()
                    .rename(col_name);
                
                formatted_df.with_column(formatted_series)?;
            }
        }
    }
    
    Ok(formatted_df)
}

/// Get the default list of columns to format
pub fn default_columns_to_format() -> Vec<&'static str> {
    vec![
        "mean_lat_us",
        "med._lat_us",
        "90%_lat_us",
        "95%_lat_us",
        "99%_lat_us",
        "max_lat_us",
        "avg_obj_KB",
        "ops_/_sec",
        "xput_MBps",
        "count",
        "tot_ops_/_sec",
        "total_xput_MBps",
        "tot_count",
    ]
}

/// Print DataFrame in a tabular format
pub fn print_dataframe(df: &DataFrame) {
    // Use Polars built-in pretty print
    println!("{}", df);
}
