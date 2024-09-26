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
use crate::checkpoint::{FilePosition, ListedRecord};
use crate::commons::scan_folder_files_to_file;
use crate::commons::RegexFilter;
use anyhow::Result;
use async_trait::async_trait;
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
use tokio::sync::Semaphore;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TaskCompareLocal2Local {
    pub source: String,
    pub target: String,
    pub check_option: CompareCheckOption,
    pub attributes: CompareTaskAttributes,
}

#[async_trait]
impl CompareTaskActions for TaskCompareLocal2Local {
    async fn gen_list_file(&self, object_list_file: &str) -> Result<FileDescription> {
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        scan_folder_files_to_file(
            self.source.as_str(),
            &object_list_file,
            regex_filter,
            self.attributes.last_modify_filter.clone(),
        )
    }

    fn gen_compare_executor(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
    ) -> Arc<dyn CompareExecutor + Send + Sync> {
        let comparator = Local2LocalRecordsComparator {
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

    // async fn listed_records_comparator(
    //     &self,
    //     joinset: &mut JoinSet<()>,
    //     records: Vec<ListedRecord>,
    //     stop_mark: Arc<AtomicBool>,
    //     offset_map: Arc<DashMap<String, FilePosition>>,
    // ) {
    //     let comparator = Local2LocalRecordsComparator {
    //         source: self.source.clone(),
    //         target: self.target.clone(),
    //         stop_mark: stop_mark.clone(),
    //         offset_map,
    //         check_option: self.check_option.clone(),
    //         attributes: self.attributes.clone(),
    //     };

    //     joinset.spawn(async move {
    //         if let Err(e) = comparator.compare_listed_records(records).await {
    //             stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
    //             log::error!("{:?}", e);
    //         };
    //     });
    // }
}

#[derive(Debug, Clone)]
pub struct Local2LocalRecordsComparator {
    pub source: String,
    pub target: String,
    pub stop_mark: Arc<AtomicBool>,
    pub err_occur: Arc<AtomicBool>,
    // pub semaphore: Arc<Semaphore>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: CompareTaskAttributes,
    pub check_option: CompareCheckOption,
}

#[async_trait]
impl CompareExecutor for Local2LocalRecordsComparator {
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
            let s_key = gen_file_path(self.source.as_str(), record.key.as_str(), "");
            let t_key = gen_file_path(self.target.as_str(), record.key.as_str(), "");

            match self.compare_listed_record(&record, &s_key, &t_key).await {
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

impl Local2LocalRecordsComparator {
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
            let s_key = gen_file_path(self.source.as_str(), record.key.as_str(), "");
            let t_key = gen_file_path(self.target.as_str(), record.key.as_str(), "");

            match self.compare_listed_record(&record, &s_key, &t_key).await {
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
        source_key: &str,
        target_key: &str,
    ) -> Result<Option<ObjectDiff>> {
        let s_path = Path::new(source_key);
        let t_path = Path::new(target_key);

        let s_exists = s_path.exists();
        let t_exists = t_path.exists();

        if !s_exists.eq(&t_exists) {
            let diff = ObjectDiff {
                source: source_key.to_string(),
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
            if let Some(diff) = self.compare_content_len(record, source_key, target_key)? {
                return Ok(Some(diff));
            }
        }

        if self.check_option.check_content() {
            if let Some(diff) = self.compare_content(record, source_key, target_key)? {
                return Ok(Some(diff));
            }
        }

        Ok(None)
    }

    fn compare_content_len(
        &self,
        record: &ListedRecord,
        source_key: &str,
        target_key: &str,
    ) -> Result<Option<ObjectDiff>> {
        let s_file = File::open(source_key)?;
        let t_file = File::open(target_key)?;
        let len_s = i128::from(s_file.metadata()?.len());
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

    fn compare_content(
        &self,
        record: &ListedRecord,
        source_key: &str,
        target_key: &str,
    ) -> Result<Option<ObjectDiff>> {
        let buffer_size = 1048577;
        let mut s_file = File::open(source_key)?;
        let mut t_file = File::open(target_key)?;
        let s_len = TryInto::<usize>::try_into(s_file.metadata()?.len())?;

        let mut left = s_len;

        loop {
            let mut buf_s = vec![0; buffer_size];
            let mut buf_t = vec![0; buffer_size];

            let read_count_s = s_file.read(&mut buf_s)?;
            let read_count_t = t_file.read(&mut buf_t)?;
            let buf_c_s = &buf_s[..read_count_s];
            let buf_c_t = &buf_t[..read_count_t];
            left -= read_count_s;

            if !buf_c_s.eq(buf_c_t) {
                for (idx, byte) in buf_c_s.iter().enumerate() {
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

            if read_count_s != buffer_size {
                break;
            }
        }
        Ok(None)
    }
}
