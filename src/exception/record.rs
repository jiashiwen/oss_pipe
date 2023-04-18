use crate::checkpoint::Record;

use std::{
    fs::File,
    sync::{atomic::AtomicUsize, Arc},
};

pub fn save_error_record(counter: &Arc<AtomicUsize>, record: Record, err_file: &mut File) {
    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let _ = record.save_json_to_file(err_file);
}
