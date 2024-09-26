use super::task_actions::CompareExecutor;
use super::task_actions::CompareTaskActions;
use super::CompareCheckOption;
use super::CompareTaskAttributes;
use super::Diff;
use super::DiffExists;
use super::ObjectDiff;
use super::COMPARE_RESULT_PREFIX;
use super::OFFSET_PREFIX;
use super::{gen_file_path, DiffContent, DiffLength};
use crate::checkpoint::FileDescription;
use crate::commons::RegexFilter;
use crate::{
    checkpoint::{FilePosition, ListedRecord},
    s3::{oss_client::OssClient, OSSDescription},
};
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{
    fs::{self, OpenOptions},
    io::Write,
};
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TaskCompareOss2Local {
    pub source: OSSDescription,
    pub target: String,
    pub check_option: CompareCheckOption,
    pub attributes: CompareTaskAttributes,
}

#[async_trait]
impl CompareTaskActions for TaskCompareOss2Local {
    async fn gen_list_file(&self, object_list_file: &str) -> Result<FileDescription> {
        let client_source = self.source.gen_oss_client()?;
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        client_source
            .append_object_list_to_file(
                self.source.bucket.clone(),
                self.source.prefix.clone(),
                self.attributes.objects_per_batch,
                object_list_file,
                regex_filter,
                self.attributes.last_modify_filter,
            )
            .await
    }

    fn gen_compare_executor(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
    ) -> Arc<dyn CompareExecutor + Send + Sync> {
        let comparator = Oss2LocalRecordsComparator {
            source: self.source.clone(),
            target: self.target.clone(),
            stop_mark,
            err_occur,
            offset_map,
            check_option: self.check_option.clone(),
            attributes: self.attributes.clone(),
        };
        Arc::new(comparator)
    }
}

#[derive(Debug, Clone)]
pub struct Oss2LocalRecordsComparator {
    pub source: OSSDescription,
    pub target: String,
    pub stop_mark: Arc<AtomicBool>,
    pub err_occur: Arc<AtomicBool>,
    // pub semaphore: Arc<Semaphore>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub check_option: CompareCheckOption,
    pub attributes: CompareTaskAttributes,
}

#[async_trait]
impl CompareExecutor for Oss2LocalRecordsComparator {
    async fn compare_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let compare_result_file_name =
            gen_file_path(&self.attributes.meta_dir, COMPARE_RESULT_PREFIX, &subffix);

        let mut compare_result_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(compare_result_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }
            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );

            let t_key = gen_file_path(self.target.as_str(), record.key.as_str(), "");

            match self.compare_listed_record(&record, &c_s, &t_key).await {
                Ok(r) => {
                    if let Some(diff) = r {
                        let _ = diff.save_json_to_file(&mut compare_result_file);
                    }
                }
                Err(e) => {
                    self.stop_mark
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    log::error!("{:?}", e);
                }
            };
        }

        let _ = compare_result_file.flush();
        self.offset_map.remove(&offset_key);
        if let Ok(m) = compare_result_file.metadata() {
            if m.len().eq(&0) {
                let _ = fs::remove_file(compare_result_file_name.as_str());
            }
        };

        Ok(())
    }

    fn error_occur(&self) {
        self.err_occur
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.stop_mark
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Oss2LocalRecordsComparator {
    pub async fn compare_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let compare_result_file_name =
            gen_file_path(&self.attributes.meta_dir, COMPARE_RESULT_PREFIX, &subffix);

        let mut compare_result_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(compare_result_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }
            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );

            let t_key = gen_file_path(self.target.as_str(), record.key.as_str(), "");

            match self.compare_listed_record(&record, &c_s, &t_key).await {
                Ok(r) => {
                    if let Some(diff) = r {
                        let _ = diff.save_json_to_file(&mut compare_result_file);
                    }
                }
                Err(e) => {
                    self.stop_mark
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    log::error!("{:?}", e);
                }
            };
        }

        let _ = compare_result_file.flush();
        self.offset_map.remove(&offset_key);
        if let Ok(m) = compare_result_file.metadata() {
            if m.len().eq(&0) {
                let _ = fs::remove_file(compare_result_file_name.as_str());
            }
        };

        Ok(())
    }

    async fn compare_listed_record(
        &self,
        record: &ListedRecord,
        source: &OssClient,
        target_key: &str,
    ) -> Result<Option<ObjectDiff>> {
        let mut s_exists = false;
        let mut obj_s = GetObjectOutput::builder().build();
        match source
            .get_object(&self.source.bucket.as_str(), record.key.as_str())
            .await
        {
            core::result::Result::Ok(o) => {
                s_exists = true;
                obj_s = o;
            }
            Err(e) => {
                let service_err = e.into_service_error();
                match service_err.is_no_such_key() {
                    true => {}
                    false => return Err(service_err.into()),
                }
            }
        };
        let t_path = Path::new(target_key);
        let t_exists = t_path.exists();

        if !s_exists.eq(&t_exists) {
            let diff = ObjectDiff {
                source: record.key.to_string(),
                target: target_key.to_string(),
                diff: Diff::ExistsDiff(DiffExists {
                    source_exists: s_exists,
                    target_exists: t_exists,
                }),
            };

            return Ok(Some(diff));
        }

        if !s_exists && !t_exists {
            return Ok(None);
        }

        if self.check_option.check_content_length() {
            if let Some(diff) = self.compare_content_len(record, &obj_s, &target_key)? {
                return Ok(Some(diff));
            }
        }

        if self.check_option.check_content() {
            if let Some(diff) = self.compare_content(record, obj_s, &target_key).await? {
                return Ok(Some(diff));
            }
        }

        Ok(None)
    }

    fn compare_content_len(
        &self,
        record: &ListedRecord,
        s_obj: &GetObjectOutput,
        target_key: &str,
    ) -> Result<Option<ObjectDiff>> {
        let len_s = match s_obj.content_length() {
            Some(l) => i128::from(l),
            None => return Err(anyhow!("content length is None")),
        };
        let t_file = File::open(target_key)?;
        let len_t = i128::from(t_file.metadata()?.len());
        if !len_s.eq(&len_t) {
            let diff = ObjectDiff {
                source: record.key.clone(),
                target: target_key.to_string(),
                diff: Diff::LengthDiff(DiffLength {
                    source_content_len: len_s,
                    target_content_len: len_t,
                }),
            };
            return Ok(Some(diff));
        }
        Ok(None)
    }

    async fn compare_content(
        &self,
        record: &ListedRecord,
        s_obj: GetObjectOutput,
        target_key: &str,
    ) -> Result<Option<ObjectDiff>> {
        let buffer_size = 1048577;

        let s_len = match s_obj.content_length() {
            Some(l) => TryInto::<usize>::try_into(l)?,
            None => return Err(anyhow!("content length is None")),
        };

        let mut left = s_len.clone();
        let mut reader_s = s_obj.body.into_async_read();
        let mut t_file = File::open(target_key)?;

        loop {
            let mut buf_s = vec![0; buffer_size];
            let mut buf_t = vec![0; buffer_size];
            if left > buffer_size {
                let _ = reader_s.read_exact(&mut buf_s).await?;
                let _ = t_file.read(&mut buf_t)?;
                left -= buffer_size;
            } else {
                buf_s = vec![0; left];
                buf_t = vec![0; left];
                let _ = reader_s.read_exact(&mut buf_s).await?;
                let _ = t_file.read(&mut buf_t)?;
                break;
            }
            if !buf_s.eq(&buf_t) {
                for (idx, byte) in buf_s.iter().enumerate() {
                    if !byte.eq(&buf_t[idx]) {
                        let diff = DiffContent {
                            stream_position: s_len - left + idx,
                            source_byte: *byte,
                            target_byte: buf_t[idx],
                        };

                        let obj_diff: ObjectDiff = ObjectDiff {
                            source: record.key.clone(),
                            target: target_key.to_string(),
                            diff: Diff::ContentDiff(diff),
                        };
                        return Ok(Some(obj_diff));
                    }
                }
            }
        }
        Ok(None)
    }
}
