use crate::{
    checkpoint::Record,
    exception::save_error_record,
    s3::{
        aws_s3::{byte_stream_multi_partes_to_file, byte_stream_to_file},
        OSSDescription,
    },
};
use anyhow::Result;
use aws_sdk_s3::error::GetObjectErrorKind;
use dashmap::DashMap;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

use super::{gen_file_path, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX};

#[derive(Debug, Clone)]
pub struct DownLoad {
    pub local_path: String,
    pub source: OSSDescription,
    pub error_conter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
}

impl DownLoad {
    pub async fn exec(&self, records: Vec<Record>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        offset_key.push_str(&subffix);
        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);
        // 先写首行日志，避免错误漏记
        self.offset_map
            .insert(offset_key.clone(), records[0].offset);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;
        for record in records {
            let resp = match c_s
                .get_object(&self.source.bucket.as_str(), record.key.as_str())
                .await
            {
                core::result::Result::Ok(b) => b,
                Err(e) => {
                    log::error!("{}", e);
                    // 源端文件不存在按传输成功处理
                    match e.into_service_error().kind {
                        GetObjectErrorKind::InvalidObjectState(_)
                        | GetObjectErrorKind::Unhandled(_) => {
                            save_error_record(&self.error_conter, record.clone(), &mut error_file);
                        }
                        GetObjectErrorKind::NoSuchKey(_) => {}
                        _ => {}
                    }

                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            };
            let t_file_name = gen_file_path(self.local_path.as_str(), &record.key.as_str(), "");

            let t_path = Path::new(t_file_name.as_str());
            // 目标object存在则不下载
            if self.target_exist_skip {
                if t_path.exists() {
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            }

            if let Some(p) = t_path.parent() {
                if let Err(e) = std::fs::create_dir_all(p) {
                    log::error!("{}", e);
                    save_error_record(&self.error_conter, record.clone(), &mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                };
            };

            let mut t_file = match OpenOptions::new()
                .truncate(true)
                .create(true)
                .write(true)
                .open(t_file_name.as_str())
            {
                Ok(p) => p,
                Err(e) => {
                    log::error!("{}", e);
                    save_error_record(&self.error_conter, record.clone(), &mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            };

            let s_len: usize = match resp.content_length().try_into() {
                Ok(len) => len,
                Err(e) => {
                    log::error!("{}", e);
                    save_error_record(&self.error_conter, record.clone(), &mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            };

            // 大文件走 multi part upload 分支
            match s_len > self.large_file_size {
                true => {
                    if let Err(e) =
                        byte_stream_multi_partes_to_file(resp, &mut t_file, self.multi_part_chunk)
                            .await
                    {
                        log::error!("{}", e);
                        save_error_record(&self.error_conter, record.clone(), &mut error_file);
                        self.offset_map.insert(offset_key.clone(), record.offset);
                        continue;
                    };
                }
                false => {
                    if let Err(e) = byte_stream_to_file(resp.body, &mut t_file).await {
                        log::error!("{}", e);
                        save_error_record(&self.error_conter, record.clone(), &mut error_file);
                        self.offset_map.insert(offset_key.clone(), record.offset);
                        continue;
                    };
                }
            }

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
