use std::sync::Arc;

use once_cell::sync::Lazy;
use rocksdb::{DBWithThreadMode, MultiThreaded};

use super::rocksdb::init_rocksdb;

pub static GLOBAL_ROCKSDB: Lazy<Arc<DBWithThreadMode<MultiThreaded>>> = Lazy::new(|| {
    let rocksdb = match init_rocksdb("oss_pipe_rocksdb") {
        Ok(db) => db,
        Err(err) => panic!("{}", err),
    };
    Arc::new(rocksdb)
});
