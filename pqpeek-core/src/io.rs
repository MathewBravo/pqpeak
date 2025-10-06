use std::{fs::File, io::Error, path::Path};
use parquet::file::reader::{FileReader, SerializedFileReader};

pub fn get_reader_from_file(path: &str) -> Result<SerializedFileReader<File>, Error>{
    let path = Path::new(path);
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    Ok(reader)
}
