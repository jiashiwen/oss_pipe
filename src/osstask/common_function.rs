use std::{
    fs::File,
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::Error;
use dashmap::DashMap;

use crate::{checkpoint::ListedRecord, exception::save_error_record};

pub fn err_process(
    error_conter: &Arc<AtomicUsize>,
    e: Error,
    record: ListedRecord,
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
