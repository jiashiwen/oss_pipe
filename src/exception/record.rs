use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::checkpoint::ListedRecord;

use anyhow::{Error, Result};

use std::{
    fs::File,
    io::Write,
    str::FromStr,
    sync::{atomic::AtomicUsize, Arc},
};

pub fn save_error_record(counter: &Arc<AtomicUsize>, record: ListedRecord, err_file: &mut File) {
    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let _ = record.save_json_to_file(err_file);
}

// 描述错误记录，用于retry或日志
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrRecord {
    pub source: String,
    pub target: String,
    pub list_file_offset: usize,
    pub list_file_line_num: usize,
    pub list_file_path: String,
}

impl FromStr for ErrRecord {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let r = serde_json::from_str::<Self>(s)?;
        Ok(r)
    }
}

impl ErrRecord {
    pub fn save(&self, error_conter: &Arc<AtomicUsize>, err_file: &mut File) {
        error_conter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = self.save_json_to_file(err_file);
    }
    pub fn save_json_to_file(&self, file: &mut File) -> Result<()> {
        let mut json = serde_json::to_string(self)?;
        json.push_str("\n");
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}

pub fn process_error(
    error_conter: &Arc<AtomicUsize>,
    e: Error,
    record: ErrRecord,
    err_file: &mut File,
    offset_key: &str,
    line_key: &str,
    offset_map: &Arc<DashMap<String, usize>>,
) {
    log::error!("{}", e);
    offset_map.insert(offset_key.to_string(), record.list_file_offset);
    offset_map.insert(line_key.to_string(), record.list_file_line_num);
    record.save(error_conter, err_file);
}
