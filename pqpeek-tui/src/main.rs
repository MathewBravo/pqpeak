fn main() {
    match pqpeek_core::stats::get_quick_stats("../order_items.parquet") {
        Ok(stats) => println!("{:#?}", stats),
        Err(err) => println!("{}", err),
    }
    match pqpeek_core::stats::get_quick_stats("../Combined_Flights_2020.parquet") {
        Ok(stats) => println!("{:#?}", stats),
        Err(err) => println!("{}", err),
    }
}
