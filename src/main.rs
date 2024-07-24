use crate::logger::init_log;

use logger::tracing_init;
use serde::{Deserialize, Serialize};
mod checkers;
pub mod checkpoint;
mod cmd;
mod commons;
mod configure;
mod exception;
mod interact;
mod logger;
mod s3;
mod tasks;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Test {
    pub t1: String,
    pub t2: u64,
}

fn main() {
    // init_log();
    tracing_init();
    // let db = GLOBAL_ROCKSDB.clone();
    // let cf = db.cf_handle(CF_TASK_STATUS).unwrap();

    // let mut wb = WriteBatchWithTransaction::<false>::default();
    // for i in 0..100 {
    //     let k = "test".to_owned() + &i.to_string();
    //     let v = Test {
    //         t1: k.clone(),
    //         t2: i,
    //     };
    //     let encoded: Vec<u8> = bincode::serialize(&v).unwrap();

    //     wb.put_cf(&cf, k.as_bytes(), encoded);
    // }
    // let _ = db.write(wb);
    // let iter = db.iterator_cf(&cf, IteratorMode::Start);
    // for item in iter {
    //     if let Ok(kv) = item {
    //         let decoded: Test = bincode::deserialize(&kv.1.to_vec()).unwrap();
    //         println!(
    //             "{:?}:{:?}",
    //             String::from_utf8(kv.0.to_vec()).unwrap(),
    //             decoded
    //         )
    //     }
    // }
    cmd::run_app();
}
