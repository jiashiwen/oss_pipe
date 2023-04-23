use crate::{checkpoint::Record, s3::OSSDescription};
use anyhow::Result;
use aws_sdk_s3::types::ByteStream;
use dashmap::DashMap;
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

use super::{gen_file_path, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX};

#[derive(Debug, Clone)]
pub struct UpLoad {
    pub local_path: String,
    pub target: OSSDescription,
    pub error_conter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub meta_dir: String,
    // pub filter: Option<String>,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunck: usize,
}

impl UpLoad {
    pub async fn exec(&self, records: Vec<Record>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        offset_key.push_str(&subffix);
        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_t = self.target.gen_oss_client()?;
        for record in records {
            let s_file_name = gen_file_path(self.local_path.as_str(), &record.key.as_str(), "");

            // 判断源文件是否存在
            let s_path = Path::new(s_file_name.as_str());
            if !s_path.exists() {
                self.offset_map.insert(offset_key.clone(), record.offset);
                continue;
            }

            let mut s_file = OpenOptions::new().read(true).open(s_file_name.as_str())?;

            let mut target_key = "".to_string();
            if let Some(s) = self.target.prefix.clone() {
                target_key.push_str(&s);
            };
            target_key.push_str(&record.key);

            // 目标object存在则不推送
            if self.target_exist_skip {
                let target_obj_exists = c_t
                    .object_exists(self.target.bucket.as_str(), target_key.as_str())
                    .await;
                match target_obj_exists {
                    Ok(b) => {
                        if b {
                            self.offset_map.insert(offset_key.clone(), record.offset);
                            continue;
                        }
                    }
                    Err(e) => {
                        log::error!("{}", e);
                        self.error_conter
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = record.save_json_to_file(&mut error_file);
                        self.offset_map.insert(offset_key.clone(), record.offset);
                    }
                }
            }

            let content_len: usize = match match s_file.metadata() {
                Ok(m) => m.len(),
                Err(e) => {
                    log::error!("{}", e);
                    self.error_conter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = record.save_json_to_file(&mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            }
            .try_into()
            {
                Ok(l) => l,
                Err(e) => {
                    log::error!("{}", e);
                    self.error_conter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = record.save_json_to_file(&mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            };

            // 大文件走 multi part upload 分支
            if let Err(e) = match content_len > self.large_file_size {
                true => {
                    c_t.multipart_upload_local_file(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        &mut s_file,
                        self.multi_part_chunck,
                    )
                    .await
                }
                false => {
                    let mut body = vec![];
                    if let Err(e) = s_file.read_to_end(&mut body) {
                        log::error!("{}", e);
                        self.error_conter
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = record.save_json_to_file(&mut error_file);
                        self.offset_map.insert(offset_key.clone(), record.offset);
                        continue;
                    };
                    c_t.upload_object_bytes(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        None,
                        ByteStream::from(body),
                    )
                    .await
                }
            } {
                log::error!("{}", e);
                self.error_conter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = record.save_json_to_file(&mut error_file);
                self.offset_map.insert(offset_key.clone(), record.offset);
            };

            self.offset_map.insert(offset_key.clone(), record.offset);
        }
        self.offset_map.remove(&offset_key);
        let _ = error_file.flush();
        match error_file.metadata() {
            Ok(meta) => {
                if meta.len() == 0 {
                    let _ = fs::remove_file(error_file_name.as_str());
                }
            }
            Err(_) => {}
        };

        Ok(())
    }
}
