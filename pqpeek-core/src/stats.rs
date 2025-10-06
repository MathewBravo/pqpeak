use std::io::Error;

use parquet::{basic::{LogicalType, Repetition, Type}, file::{metadata::KeyValue, reader::FileReader}};

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
    percision: i32,
    scale: i32,
    nullable: bool,
}

pub struct VerboseParquetFileStatistics;

pub fn get_quick_stats(path: &str) -> Result<ParquetFileStatistics, Error>{
    let reader = get_reader_from_file(path)?;
    let file_metadata = reader.metadata().file_metadata();
    
    let schema = file_metadata.schema_descr();
    
    let mut cd: Vec<ColumnDescriptors> = Vec::new();
    
    for i in 0..schema.num_columns(){
        let col = schema.column(i);
        
        let nullable = col.max_rep_level() > 0 || col.self_type().get_basic_info().repetition() != Repetition::REQUIRED;
        

        cd.push(
            ColumnDescriptors { 
                name: col.name().to_string(), 
                phys_type: col.physical_type(), 
                logical_type: col.logical_type(), 
                percision: col.type_precision(), 
                scale: col.type_scale(), 
                nullable,
            }
        );
        
    }

    let quick_stats = ParquetFileStatistics{
        version: file_metadata.version(),
        num_rows: file_metadata.num_rows(),
        created_by: file_metadata.created_by().map(|s| s.to_string()),
        col_descriptions: cd,
    };
    
    Ok(quick_stats)
}