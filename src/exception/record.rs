use crate::checkpoint::Record;
use anyhow::Error;
use std::sync::{atomic::AtomicUsize, Arc};

pub fn record_exception(e: Error, counter: &Arc<AtomicUsize>, record: Record, err_file: &str) {
    log::error!("{}", e);
    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let _ = record.save_json_to_file(err_file);
}
