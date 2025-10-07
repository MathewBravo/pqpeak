use clap::Parser;

use crate::commands::Cli;

mod commands;
mod stats;

fn main() {
    let cli = Cli::parse();
    
    match &cli.command {
        commands::Commands::Stats { file } => {
            stats::print_stats(file);
        },
        commands::Commands::VerboseStats { file } => println!("Getting verbose stats for {}", file),
    }

    /* match pqpeek_core::stats::get_statistics("../order_items.parquet") {
        Ok(stats) => println!("{:#?}", stats),
        Err(err) => println!("{}", err),
    }
    match pqpeek_core::stats::get_statistics("../Combined_Flights_2020.parquet") {
        Ok(stats) => println!("{:#?}", stats),
        Err(err) => println!("{}", err),
    } */
}
