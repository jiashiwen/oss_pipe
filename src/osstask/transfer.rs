use crate::{checkpoint::Record, exception::save_error_record, s3::OSSDescription};
use anyhow::Result;
use aws_sdk_s3::error::GetObjectErrorKind;
use dashmap::DashMap;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
};

use super::{gen_file_path, CURRENT_LINE_PREFIX, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX};

#[derive(Debug, Clone)]
pub struct Transfer {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub error_conter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
    pub begin_line_number: usize,
}

impl Transfer {
    pub async fn exec(&self, records: Vec<Record>) -> Result<()> {
        let mut line_num = self.begin_line_number;
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        offset_key.push_str(&subffix);
        let mut current_line_key = CURRENT_LINE_PREFIX.to_string();
        current_line_key.push_str(&self.begin_line_number.to_string());

        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        // 先写首行日志，避免错误漏记
        self.offset_map
            .insert(offset_key.clone(), records[0].offset);
        // 与记录当前行数
        let num = TryInto::<usize>::try_into(line_num).unwrap();
        self.offset_map.insert(current_line_key.clone(), num);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;
        let c_t = self.target.gen_oss_client()?;
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

            let mut target_key = match self.target.prefix.clone() {
                Some(s) => s,
                None => "".to_string(),
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

            let content_len = match usize::try_from(resp.content_length()) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    self.error_conter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = record.save_json_to_file(&mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            };

            let expr = match resp.expires() {
                Some(datetime) => Some(*datetime),
                None => None,
            };

            // 大文件走 multi part upload 分支
            if let Err(e) = match content_len > self.large_file_size {
                true => {
                    c_t.multipart_upload_byte_stream(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        expr,
                        content_len,
                        self.multi_part_chunk,
                        resp.body,
                    )
                    .await
                }
                false => {
                    c_t.upload_object_bytes(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        expr,
                        resp.body,
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

            line_num += 1;
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
