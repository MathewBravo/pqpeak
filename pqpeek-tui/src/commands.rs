use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli { 
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Shows statistics of provided file
    Stats {file: String},
    
    /// Shows statistics of provided file
    VerboseStats {file: String}
}
