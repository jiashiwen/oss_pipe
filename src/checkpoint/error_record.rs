use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::{LineWriter, Write},
    str::FromStr,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorRecord {
    source_bucket: String,
    source_prefix: String,
    source_path: String,
}

impl FromStr for ErrorRecord {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let r = serde_json::from_str::<Self>(s)?;
        Ok(r)
    }
}

impl ErrorRecord {
    fn save_json_to_file(&self, file: &str) -> Result<()> {
        let file_opt = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(file)?;
        let json = serde_json::to_string(self)?;
        let mut f = LineWriter::new(file_opt);

        f.write(json.as_bytes())?;
        f.write(b"\n")?;
        f.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::ErrorRecord;

    //cargo test checkpoint::error_record::test::test_error_record -- --nocapture
    #[test]
    fn test_error_record() {
        let record1 = ErrorRecord {
            source_bucket: "bucket1".to_string(),
            source_prefix: "prefix1/".to_string(),
            source_path: "path1/path/file1".to_string(),
        };

        record1.save_json_to_file("/tmp/error_record");

        let record2 = ErrorRecord {
            source_bucket: "bucket1".to_string(),
            source_prefix: "prefix1/".to_string(),
            source_path: "path1/path/file1".to_string(),
        };
        record2.save_json_to_file("/tmp/error_record");
    }
}
