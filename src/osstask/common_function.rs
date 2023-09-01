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

// 获取对象列表
// pub fn generate_object_list(
//     oss_desc: &OSSDescription,
//     meta_dir: &str,
//     rt: &Runtime,
//     object_list_file: String,
//     bach_size: i32,
// ) -> Result<usize> {
//     // 预清理meta目录
//     let _ = fs::remove_dir_all(meta_dir);
//     let mut interrupted = false;
//     let mut total = 0;

//     rt.block_on(async {
//         let client_source = match oss_desc.gen_oss_client() {
//             Result::Ok(c) => c,
//             Err(e) => {
//                 log::error!("{}", e);
//                 interrupted = true;
//                 return;
//             }
//         };
//         match client_source
//             .append_all_object_list_to_file(
//                 oss_desc.bucket.clone(),
//                 oss_desc.prefix.clone(),
//                 bach_size,
//                 object_list_file.clone(),
//             )
//             .await
//         {
//             Ok(size) => total = size,
//             Err(e) => {
//                 log::error!("{}", e);
//                 interrupted = true;
//                 return;
//             }
//         };
//     });

//     if interrupted {
//         return Err(anyhow!("get object list error"));
//     }
//     Ok(total)
// }
