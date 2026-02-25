#[derive(PartialEq, Clone, Copy)]
pub enum FileType {
    Csv,
    Parquet,
}

impl FileType {
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "csv" => Some(FileType::Csv),
            "parquet" => Some(FileType::Parquet),
            _ => None,
        }
    }
}
