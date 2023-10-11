use std::{fs::File, io::Write, str::FromStr};

use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Record {
    pub key: String,
    pub offset: usize,
    pub line_num: usize,
}

impl FromStr for Record {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let r = serde_json::from_str::<Self>(s)?;
        Ok(r)
    }
}

impl Record {
    pub fn save_json_to_file(&self, file: &mut File) -> Result<()> {
        let mut json = serde_json::to_string(self)?;
        json.push_str("\n");
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{fs::OpenOptions, io::Write, path::Path};

    use super::Record;

    //cargo test checkpoint::record::test::test_error_record -- --nocapture
    #[test]
    fn test_error_record() {
        let file_name = "/tmp/err_dir/error_record";
        let path = Path::new(file_name);
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p).unwrap();
        };

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_name)
            .unwrap();

        for i in 0..100 {
            let mut key = "tt/ttt/tttt".to_string();
            key.push_str(i.to_string().as_str());
            let offset = 3214 + i;
            let offset_usize: usize = offset.try_into().unwrap();
            let record = Record {
                key,
                offset: offset_usize,
                line_num: 1,
            };
            let _ = record.save_json_to_file(&mut file);
        }

        let record1 = Record {
            key: "test/test1/ttt".to_string(),
            offset: 65,
            line_num: 1,
        };

        let r = record1.save_json_to_file(&mut file);
        println!("r is {:?}", r);

        let record2 = Record {
            key: "test/test2/tt222".to_string(),
            offset: 77,
            line_num: 1,
        };
        let _ = record2.save_json_to_file(&mut file);

        file.flush().unwrap();
    }
}
