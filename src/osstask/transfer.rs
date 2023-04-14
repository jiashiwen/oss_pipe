use crate::{checkpoint::Record, exception::record_exception, s3::OSSDescription};
use anyhow::Result;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

use super::{gen_file_path, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX};

#[derive(Debug, Clone)]
pub struct Transfer {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub error_conter: Arc<AtomicUsize>,
    pub meta_dir: String,
    pub prefix: Option<String>,
    // pub filter: Option<String>,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunck: usize,
}

impl Transfer {
    // todo
    // 报错写error file ok
    // key filter 正则表达式支持
    // 根据 target prefix 拼接新路径 ok
    // 增加taget 目标存在则不推送参数 ok，
    pub async fn exec(&self, records: Vec<Record>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let offset_log_file_name = gen_file_path(&self.meta_dir, OFFSET_EXEC_PREFIX, &subffix);
        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        let path = Path::new(offset_log_file_name.as_str());
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        };

        let mut offset_log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(offset_log_file_name.as_str())?;

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
                    record_exception(e, &self.error_conter, record, &mut error_file);
                    continue;
                }
            };

            let mut target_key = "".to_string();
            if let Some(s) = self.prefix.clone() {
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
                            continue;
                        }
                    }
                    Err(e) => {
                        log::error!("{}", e);
                        self.error_conter
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = record.save_json_to_file(&mut error_file);
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
                    continue;
                }
            };

            // 大文件走 multi part upload 分支
            if let Err(e) = match content_len > self.large_file_size {
                true => {
                    c_t.multipart_upload_byte_stream(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        content_len,
                        self.multi_part_chunck,
                        resp.body,
                    )
                    .await
                }
                false => {
                    c_t.upload_object_bytes(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        resp.body,
                    )
                    .await
                }
            } {
                log::error!("{}", e);
                self.error_conter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = record.save_json_to_file(&mut error_file);
            };

            let _ = offset_log_file.write_all(record.offset.to_string().as_bytes());
            let _ = offset_log_file.write_all("\n".as_bytes());
        }
        let _ = offset_log_file.flush();
        let _ = error_file.flush();
        match error_file.metadata() {
            Ok(meta) => {
                if meta.len() == 0 {
                    let _ = fs::remove_file(error_file_name.as_str());
                }
            }
            Err(_) => {}
        };

        let _ = fs::remove_file(offset_log_file_name.as_str());
        Ok(())
    }
}
