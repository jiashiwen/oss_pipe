use crate::{checkpoint::Record, exception::save_error_record, s3::OSSDescription};
use anyhow::Result;
use aws_sdk_s3::{error::GetObjectErrorKind, output::GetObjectOutput};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
};

use super::{gen_file_path, COMPARE_OBJECT_DIFF_PREFIX, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ObjectDiff {
    NotExists(DiffNotExists),
    ContentLenthDiff(DiffContentLenth),
    ExpiresDiff(DiffExpires),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffNotExists {
    key: String,
    source_exists: bool,
    target_exists: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffExpires {
    key: String,
    source_expires: Option<DateTime>,
    target_expires: Option<DateTime>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DateTime {
    seconds: i64,
    subsecond_nanos: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffContentLenth {
    key: String,
    source_content_len: i64,
    target_content_len: i64,
}

impl ObjectDiff {
    pub fn save_json_to_file(&self, file: &mut File) -> Result<()> {
        // 获取文件路径，若不存在则创建路径
        let mut json = serde_json::to_string(self)?;
        json.push_str("\n");
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OssCompare {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub error_conter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub meta_dir: String,
    pub exprirs_diff_scope: i64,
    // pub filter: Option<String>,
}

impl OssCompare {
    // todo
    // key filter 正则表达式支持
    pub async fn compare(&self, records: Vec<Record>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);
        let diff_file_name = gen_file_path(&self.meta_dir, COMPARE_OBJECT_DIFF_PREFIX, &subffix);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;
        let mut diff_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;
        let c_t = self.target.gen_oss_client()?;
        for record in records {
            let mut s_exists = false;
            let mut t_exists = false;
            let mut resp_s = GetObjectOutput::builder().build();
            let mut resp_t = GetObjectOutput::builder().build();
            match c_s
                .get_object(&self.source.bucket.as_str(), record.key.as_str())
                .await
            {
                Err(e) => {
                    log::error!("{}", e);
                    // 源端文件不存在按传输成功处理
                    match e.into_service_error().kind {
                        GetObjectErrorKind::InvalidObjectState(_)
                        | GetObjectErrorKind::Unhandled(_) => {
                            save_error_record(&self.error_conter, record.clone(), &mut error_file);
                            self.offset_map.insert(offset_key.clone(), record.offset);
                            continue;
                        }
                        GetObjectErrorKind::NoSuchKey(_) => {
                            s_exists = true;
                        }
                        _ => {}
                    }
                }
                Ok(r) => resp_s = r,
            };

            let mut target_key = "".to_string();
            if let Some(s) = self.target.prefix.clone() {
                target_key.push_str(&s);
            };
            target_key.push_str(&record.key);

            match c_t
                .get_object(&self.target.bucket.as_str(), target_key.as_str())
                .await
            {
                core::result::Result::Ok(t) => resp_t = t,
                Err(e) => {
                    log::error!("{}", e);
                    // 源端文件不存在按传输成功处理
                    match e.into_service_error().kind {
                        GetObjectErrorKind::InvalidObjectState(_)
                        | GetObjectErrorKind::Unhandled(_) => {
                            save_error_record(&self.error_conter, record.clone(), &mut error_file);
                            self.offset_map.insert(offset_key.clone(), record.offset);
                            continue;
                        }
                        GetObjectErrorKind::NoSuchKey(_) => {
                            t_exists = true;
                        }
                        _ => {}
                    }

                    self.offset_map.insert(offset_key.clone(), record.offset);
                }
            };

            if !s_exists.eq(&t_exists) {
                let diff = ObjectDiff::NotExists(DiffNotExists {
                    key: record.key,
                    source_exists: s_exists,
                    target_exists: t_exists,
                });
                let _ = diff.save_json_to_file(&mut diff_file);
                continue;
            }

            let content_len_s = resp_s.content_length();
            let content_len_t = resp_t.content_length();

            if !content_len_s.eq(&content_len_t) {
                let diff = ObjectDiff::ContentLenthDiff(DiffContentLenth {
                    key: record.key,
                    source_content_len: content_len_s,
                    target_content_len: content_len_t,
                });
                let _ = diff.save_json_to_file(&mut diff_file);
                continue;
            }

            let expr_s = match resp_s.expires() {
                Some(datetime) => Some(*datetime),
                None => None,
            };

            let expr_t = match resp_t.expires() {
                Some(datetime) => Some(*datetime),
                None => None,
            };

            if !expr_s.eq(&expr_t) {
                let mut s_second = 0;
                let mut t_second = 0;
                let s_data = match expr_s {
                    Some(d) => {
                        s_second = d.secs();
                        Some(DateTime {
                            seconds: d.secs(),
                            subsecond_nanos: d.subsec_nanos(),
                        })
                    }
                    None => None,
                };
                let t_data = match expr_t {
                    Some(d) => {
                        t_second = d.secs();
                        Some(DateTime {
                            seconds: d.secs(),
                            subsecond_nanos: d.subsec_nanos(),
                        })
                    }
                    None => None,
                };
                if s_data.is_none() || t_data.is_none() {
                    let diff = ObjectDiff::ExpiresDiff(DiffExpires {
                        key: record.key,
                        source_expires: s_data,
                        target_expires: t_data,
                    });
                    let _ = diff.save_json_to_file(&mut diff_file);
                    continue;
                }

                if i64::abs(s_second - t_second) > self.exprirs_diff_scope {
                    let diff = ObjectDiff::ExpiresDiff(DiffExpires {
                        key: record.key,
                        source_expires: s_data,
                        target_expires: t_data,
                    });
                    let _ = diff.save_json_to_file(&mut diff_file);
                };
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
        match diff_file.metadata() {
            Ok(meta) => {
                if meta.len() == 0 {
                    let _ = fs::remove_file(diff_file_name.as_str());
                }
            }
            Err(_) => {}
        };

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        commons::struct_to_json_string,
        osstask::osscompare::{DateTime, DiffExpires, ObjectDiff},
    };

    //cargo test osstask::osscompare::test::test_object_diff -- --nocapture
    #[test]
    fn test_object_diff() {
        let abs = i64::abs(990 - 870);
        println!("{}", abs);

        let expires_diff = DiffExpires {
            key: "abc".to_string(),
            source_expires: Some(DateTime {
                seconds: 1212139921,
                subsecond_nanos: 132132,
            }),
            target_expires: Some(DateTime {
                seconds: 1212138821,
                subsecond_nanos: 132132,
            }),
        };

        let diff = ObjectDiff::ExpiresDiff(expires_diff);
        let diff_str = struct_to_json_string(&diff);

        println!("{}", diff_str.unwrap());
    }
}
