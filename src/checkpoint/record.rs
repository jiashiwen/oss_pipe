use std::{
    fs::File,
    io::Write,
    str::FromStr,
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::{Error, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ListedRecord {
    pub key: String,
    pub offset: usize,
    pub line_num: u64,
}

impl FromStr for ListedRecord {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let r = serde_json::from_str::<Self>(s)?;
        Ok(r)
    }
}

impl ListedRecord {
    pub fn save_json_to_file(&self, file: &mut File) -> Result<()> {
        let mut json = serde_json::to_string(self)?;
        json.push_str("\n");
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Opt {
    PUT,
    REMOVE,
    UNKOWN,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilePosition {
    pub offset: usize,
    pub line_num: u64,
}

impl Default for FilePosition {
    fn default() -> Self {
        Self {
            offset: 0,
            line_num: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RecordDescription {
    pub source_key: String,
    pub target_key: String,
    pub list_file_path: String,
    pub list_file_position: FilePosition,
    pub option: Opt,
}

impl FromStr for RecordDescription {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let r = serde_json::from_str::<Self>(s)?;
        Ok(r)
    }
}

impl RecordDescription {
    pub fn handle_error(
        &self,
        err_counter: &Arc<AtomicUsize>,
        offset_map: &Arc<DashMap<String, FilePosition>>,
        save_to: &mut File,
        file_position_key: &str,
    ) {
        offset_map.insert(
            file_position_key.to_string(),
            self.list_file_position.clone(),
        );

        err_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = self.save_json_to_file(save_to);
    }

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
    use super::ListedRecord;
    use std::{fs::OpenOptions, io::Write, path::Path};

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
            let record = ListedRecord {
                key,
                offset: offset_usize,
                line_num: 1,
            };
            let _ = record.save_json_to_file(&mut file);
        }

        let record1 = ListedRecord {
            key: "test/test1/ttt".to_string(),
            offset: 65,
            line_num: 1,
        };

        let r = record1.save_json_to_file(&mut file);
        println!("r is {:?}", r);

        let record2 = ListedRecord {
            key: "test/test2/tt222".to_string(),
            offset: 77,
            line_num: 1,
        };
        let _ = record2.save_json_to_file(&mut file);

        file.flush().unwrap();
    }
}
