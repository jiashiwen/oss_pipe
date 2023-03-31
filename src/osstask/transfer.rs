use crate::{checkpoint::Record, exception::record_exception, s3::OSSDescription};
use anyhow::Result;
use std::sync::{atomic::AtomicUsize, Arc};

#[derive(Debug, Clone)]
pub struct Transfer {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub error_conter: Arc<AtomicUsize>,
    pub error_file: String,
    pub prefix: Option<String>,
    // pub filter: Option<String>,
    pub target_exist_skip: bool,
}

impl Transfer {
    // todo 报错写error file ok
    // key filter 正则表达式支持
    // 根据 target prefix 拼接新路径 ok
    // 增加taget 目标存在则不推送参数，
    pub async fn exec(&self, records: Vec<Record>) -> Result<()> {
        let c_s = self.source.gen_oss_client()?;
        let c_t = self.target.gen_oss_client()?;
        for record in records {
            let bytes = match c_s
                .get_object_bytes(&self.source.bucket.as_str(), record.key.as_str())
                .await
            {
                core::result::Result::Ok(b) => b,
                Err(e) => {
                    record_exception(e, &self.error_conter, record, &self.error_file);
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
                        let _ = record.save_json_to_file(&self.error_file);
                    }
                }
            }

            if let Err(e) = c_t
                .upload_object_bytes(self.target.bucket.as_str(), target_key.as_str(), bytes)
                .await
            {
                log::error!("{}", e);
                self.error_conter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = record.save_json_to_file(&self.error_file);
            };
        }
        Ok(())
    }
}
