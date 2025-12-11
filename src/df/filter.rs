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

#[derive(Debug, Clone)]
pub struct FilterCondition {
    pub filter_type: FilterType,
    pub column_name: String,
    pub filter_value: String,
}

pub fn filter_dataframe(
    dataframe: &DataFrame,
    filters: &[FilterCondition],
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut lazy_df = dataframe.clone().lazy();

    if !filters.is_empty() {
        let mut filter_exprs = Vec::new();
        for filter in filters {
            let expr = match filter.filter_type {
                FilterType::Equals => col(&filter.column_name)
                    .cast(DataType::String)
                    .eq(lit(filter.filter_value.clone())),
                FilterType::Contains => col(&filter.column_name)
                    .cast(DataType::String)
                    .str()
                    .contains(lit(filter.filter_value.clone()), false),
            };
            filter_exprs.push(expr);
        }
        let combined = filter_exprs.into_iter().reduce(|a, b| a.and(b)).unwrap();
        lazy_df = lazy_df.filter(combined);
    }

    let filtered_df = lazy_df
        .collect()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    Ok(filtered_df)
}
