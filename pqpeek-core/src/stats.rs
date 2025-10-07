use std::io::Error;

use parquet::{basic::{LogicalType, Repetition, Type}, file::{reader::FileReader, statistics::Statistics}};

use crate::io::get_reader_from_file;

#[derive(Debug)]
pub struct ParquetFileStatistics{
    pub version: i32,
    pub num_rows: i64,
    pub created_by: Option<String>,
    pub col_descriptions: Vec<ColumnDescriptors>,
}

// !TODO Add Nullable info
#[derive(Debug)]
pub struct ColumnDescriptors{
    name: String,
    phys_type: Type,
    logical_type: Option<LogicalType>,
    percision: Option<i32>,
    scale: Option<i32>,
    nullable: bool,
    nc: Option<u64>
}


pub struct VerboseParquetFileStatistics;

pub fn get_statistics(path: &str) -> Result<ParquetFileStatistics, Error>{
    let reader = get_reader_from_file(path)?;
    let meta = reader.metadata();
    
    let schema = meta.file_metadata().schema_descr();
    
    let mut cds: Vec<ColumnDescriptors> = Vec::new();
    
    for i in 0..schema.num_columns(){
        let col = schema.column(i);
        
        let nullable = col.max_rep_level() > 0 || col.self_type().get_basic_info().repetition() != Repetition::REQUIRED;
        
        let decimal = match col.physical_type() {
            Type::FLOAT => true,
            Type::DOUBLE => true,
            _ => false,
        };
        


        
        let mut null_count: u64 = 0;
        let mut no_nulls = true;

        for rg_idx in 0..meta.num_row_groups(){
            let col_chunk = meta.row_group(rg_idx).column(i);
            match col_chunk.statistics() {
                Some(stats) => match stats.null_count_opt() {
                    Some(n) => null_count += n,
                    None => { no_nulls = false; }
                },
                None => { no_nulls = false; }
            }
        }
        
        let cd = ColumnDescriptors { 
                name: col.name().to_string(), 
                phys_type: col.physical_type(), 
                logical_type: col.logical_type(), 
                percision: if decimal{
                    Some(col.type_precision())
                }else{
                    None
                }, 
                scale: if decimal{
                    Some(col.type_scale())
                }else{
                    None
                }, 
                nullable,
                nc: if no_nulls {Some(null_count)} else {None},
        };

        cds.push(cd);
    }

    let quick_stats = ParquetFileStatistics{
        version: meta.file_metadata().version(),
        num_rows: meta.file_metadata().num_rows(),
        created_by: meta.file_metadata().created_by().map(|s| s.to_string()),
        col_descriptions: cds,
    };
    
    Ok(quick_stats)
}