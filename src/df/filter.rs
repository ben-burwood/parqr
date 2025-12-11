use polars::prelude::*;
use polars::{frame::DataFrame, prelude::DataType};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FilterType {
    Equals,
    Contains,
}

impl std::fmt::Display for FilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterType::Equals => write!(f, "Equals"),
            FilterType::Contains => write!(f, "Contains"),
        }
    }
}

pub fn filter_dataframe(
    dataframe: &DataFrame,
    column_name: &str,
    filter_type: FilterType,
    filter_value: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let lazy_df = dataframe.clone().lazy();

    let filter_expr = match filter_type {
        FilterType::Equals => col(column_name)
            .cast(DataType::String)
            .eq(lit(filter_value)),
        FilterType::Contains => col(column_name)
            .cast(DataType::String)
            .str()
            .contains(lit(filter_value), false),
    };

    let filtered_df = lazy_df
        .filter(filter_expr)
        .collect()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    Ok(filtered_df)
}
