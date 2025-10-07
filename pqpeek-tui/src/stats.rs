use std::path::Path;

use pqpeek_core::stats::ParquetFileStatistics;

pub fn print_stats(path: &str){
    let path_obj = Path::new(path);

    let filename = path_obj
        .file_name()
        .map(|f| f.to_string_lossy())
        .unwrap_or_else(|| "(unknown file)".into());
    
    match pqpeek_core::stats::get_statistics(path) {
        Ok(stats) => {
            println!("Stats for {}:\n", filename);
            println!("============================ File Statistics ============================");
            println!("File Name:\t\t\t\t{}", filename);
            println!("Row Count:\t\t\t\t{}", stats.num_rows);
            println!("Parquet Version:\t\t\t{}", stats.version);
            if let Some(creator) = stats.created_by {
                println!("Created By:\t\t\t\t{}", creator);
            }
            println!();
            println!("=========================== Column Statistics ===========================");
            for col in stats.col_descriptions{
                println!("Column Name:\t\t\t\t{}", col.name);
                println!("Physical Type:\t\t\t\t{}", col.phys_type);
                if let Some(lt) = col.logical_type{
                    println!("Logical Type:\t\t\t\t{:?}", lt);
                }
                if let Some(p) = col.percision{
                    println!("Percision:\t\t\t\t{}", p);
                }
                if let Some(s) = col.scale{
                    println!("Scale:\t\t\t\t\t{}", s);
                }
                println!("Nullable:\t\t\t\t{}", col.nullable);
                if let Some(nc) = col.nc {
                    println!("Null Count:\t\t\t\t{}", nc);
                }else{
                    println!("Null Count:\t\t\t\tNone");
                }
                println!();
            }

        },
        Err(err) => println!("{}", err),
    } 
}