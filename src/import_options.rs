use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Clone, Parser)]
pub struct ImportOptions {
    #[arg(short = 'p', long, help_heading = "Data Source")]
    /// Search for the data source file here instead of from the import profile
    pub path_override: Option<PathBuf>,
    #[arg(
        short = 'd',
        long,
        default_value = "retain",
        help_heading = "Data Source"
    )]
    pub deletion: DataSourceDeletion,
    /// Do not merge results from the temporary table to the target table
    #[arg(long, requires = "no_drop", help_heading = "Developer")]
    pub no_merge: bool,
    /// Do not drop the temporary table after each table mapper execution
    #[arg(long, help_heading = "Developer")]
    pub no_drop: bool,
    /// Do not merge duplicate columns
    #[arg(long, help_heading = "Developer")]
    pub no_duplicate_optimization: bool,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum DataSourceDeletion {
    /// Retain the data source file
    Retain,
    /// Delete the data source file
    Delete,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            path_override: None,
            deletion: DataSourceDeletion::Retain,
            no_merge: false,
            no_drop: false,
            no_duplicate_optimization: false,
        }
    }
}
