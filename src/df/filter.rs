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
