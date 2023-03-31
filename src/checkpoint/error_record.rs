use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::{LineWriter, Write},
    path::Path,
    str::FromStr,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Record {
    pub key: String,
    pub offset: usize,
}

impl FromStr for Record {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let r = serde_json::from_str::<Self>(s)?;
        Ok(r)
    }
}

impl Record {
    pub fn save_json_to_file(&self, file: &str) -> Result<()> {
        // 获取文件路径，若不存在则创建路径
        let store_path = Path::new(file);
        let path = std::path::Path::new(store_path);
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        };
        let file_opt = OpenOptions::new()
            .append(true)
            .create(true)
            .write(true)
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
    use super::Record;

    //cargo test checkpoint::error_record::test::test_error_record -- --nocapture
    #[test]
    fn test_error_record() {
        let record1 = Record {
            key: "test/test1/ttt".to_string(),
            offset: 65,
        };

        let r = record1.save_json_to_file("err_dir/error_record");
        println!("r is {:?}", r);

        let record2 = Record {
            key: "test/test2/tt222".to_string(),
            offset: 77,
        };
        record2.save_json_to_file("err_dir/error_record");
    }
}
