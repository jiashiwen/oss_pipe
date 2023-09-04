use std::{
    fs::{self, File},
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::{anyhow, Error, Result};
use dashmap::DashMap;
use tokio::runtime::Runtime;

use crate::{checkpoint::Record, exception::save_error_record, s3::OSSDescription};

pub fn err_process(
    error_conter: &Arc<AtomicUsize>,
    e: Error,
    record: Record,
    err_file: &mut File,
    offset_key: &str,
    line_key: &str,
    offset_map: &Arc<DashMap<String, usize>>,
) {
    log::error!("{}", e);
    save_error_record(&error_conter, record.clone(), err_file);
    offset_map.insert(offset_key.to_string(), record.offset);
    offset_map.insert(line_key.to_string(), record.line_num);
}
