use crate::{
    checkpoint::{FilePosition, ListedRecord, Opt, RecordDescription},
    commons::{json_to_struct, read_lines},
    s3::{
        aws_s3::{oss_download_to_file, OssClient},
        OSSDescription,
    },
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::error::GetObjectErrorKind;
use dashmap::DashMap;

use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::{runtime::Runtime, task::JoinSet};
use walkdir::WalkDir;

use super::{
    gen_file_path, task_actions::TaskActionsFromOss, TaskType, TransferTaskAttributes,
    ERROR_RECORD_PREFIX, OFFSET_PREFIX,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct DownloadTask {
    pub source: OSSDescription,
    pub local_path: String,
    pub task_attributes: TransferTaskAttributes,
}

impl Default for DownloadTask {
    fn default() -> Self {
        Self {
            source: OSSDescription::default(),
            local_path: "/tmp".to_string(),
            task_attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TaskActionsFromOss for DownloadTask {
    fn task_type(&self) -> TaskType {
        TaskType::Download
    }

    fn error_record_retry(&self) -> Result<()> {
        // 遍历错误记录
        // 每个错误文件重新处理
        for entry in WalkDir::new(self.task_attributes.meta_dir.as_str())
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir() && e.file_name().to_str().is_some())
        {
            let file_name = match entry.file_name().to_str() {
                Some(name) => name,
                None => {
                    continue;
                }
            };

            if !file_name.starts_with(ERROR_RECORD_PREFIX) {
                continue;
            };

            if let Some(p) = entry.path().to_str() {
                if let Ok(lines) = read_lines(p) {
                    let mut record_vec = vec![];
                    for line in lines {
                        match line {
                            Ok(content) => {
                                let record = match json_to_struct::<ListedRecord>(content.as_str())
                                {
                                    Ok(r) => r,
                                    Err(e) => {
                                        log::error!("{}", e);
                                        continue;
                                    }
                                };
                                record_vec.push(record);
                            }
                            Err(e) => {
                                log::error!("{}", e);
                                continue;
                            }
                        }
                    }

                    if record_vec.len() > 0 {
                        let download = DownLoadRecordsExecutor {
                            target: self.local_path.clone(),
                            source: self.source.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            meta_dir: self.task_attributes.meta_dir.clone(),
                            target_exist_skip: self.task_attributes.target_exists_skip,
                            large_file_size: self.task_attributes.large_file_size,
                            multi_part_chunk: self.task_attributes.multi_part_chunk,
                            list_file_path: p.to_string(),
                        };
                        let _ = download.exec(record_vec);
                    }
                }

                let _ = fs::remove_file(p);
            }
        }

        Ok(())
    }

    fn generate_object_list(
        &self,
        rt: &Runtime,
        last_modify_timestamp: i64,
        object_list_file: &str,
    ) -> Result<usize> {
        let mut interrupted = false;
        let mut total_lines = 0;

        rt.block_on(async {
            let client_source = match self.source.gen_oss_client() {
                Result::Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    interrupted = true;
                    return;
                }
            };

            // 若为持续同步模式，且 last_modify_timestamp 大于 0，则将 last_modify 属性大于last_modify_timestamp变量的对象加入执行列表
            let total_rs = match last_modify_timestamp > 0 {
                true => {
                    client_source
                        .append_last_modify_greater_object_to_file(
                            self.source.bucket.clone(),
                            self.source.prefix.clone(),
                            self.task_attributes.bach_size,
                            object_list_file.to_string(),
                            last_modify_timestamp,
                        )
                        .await
                }
                false => {
                    client_source
                        .append_all_object_list_to_file(
                            self.source.bucket.clone(),
                            self.source.prefix.clone(),
                            self.task_attributes.bach_size,
                            object_list_file.to_string(),
                        )
                        .await
                }
            };
            match total_rs {
                Ok(size) => total_lines = size,
                Err(e) => {
                    log::error!("{}", e);
                    interrupted = true;
                    return;
                }
            }
        });

        if interrupted {
            return Err(anyhow!("get object list error"));
        }
        Ok(total_lines)
    }

    async fn records_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        records: Vec<ListedRecord>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    ) {
        let download = DownLoadRecordsExecutor {
            target: self.local_path.clone(),
            source: self.source.clone(),
            err_counter,
            offset_map,
            meta_dir: self.task_attributes.meta_dir.clone(),
            target_exist_skip: false,
            large_file_size: self.task_attributes.large_file_size,
            multi_part_chunk: self.task_attributes.multi_part_chunk,
            list_file_path: list_file,
        };

        joinset.spawn(async move {
            if let Err(e) = download.exec(records).await {
                download
                    .err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }
}

#[derive(Debug, Clone)]
pub struct DownLoadRecordsExecutor {
    pub target: String,
    pub source: OSSDescription,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
    pub list_file_path: String,
}

impl DownLoadRecordsExecutor {
    pub async fn exec(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);
        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);
        // 先写首行日志，避免错误漏记
        self.offset_map.insert(
            offset_key.clone(),
            FilePosition {
                offset: records[0].offset,
                line_num: records[0].line_num,
            },
        );

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;
        for record in records {
            let t_file_name = gen_file_path(self.target.as_str(), &record.key.as_str(), "");
            if let Err(e) = self
                .record_handler(offset_key.as_str(), &record, &c_s, t_file_name.as_str())
                .await
            {
                let record_desc = RecordDescription {
                    source_key: record.key.clone(),
                    target_key: t_file_name.clone(),
                    list_file_path: self.list_file_path.clone(),
                    list_file_position: FilePosition {
                        offset: record.offset,
                        line_num: record.line_num,
                    },
                    option: Opt::PUT,
                };
                record_desc.handle_error(
                    &self.err_counter,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
                log::error!("{}", e);
            }
            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );
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

    async fn record_handler(
        &self,
        offset_key: &str,
        record: &ListedRecord,
        source_oss_client: &OssClient,
        target_file: &str,
    ) -> Result<()> {
        let t_path = Path::new(target_file);
        if let Some(p) = t_path.parent() {
            std::fs::create_dir_all(p)?;
        };

        // 目标object存在则不下载
        if self.target_exist_skip {
            if t_path.exists() {
                self.offset_map.insert(
                    offset_key.to_string(),
                    FilePosition {
                        offset: record.offset,
                        line_num: record.line_num,
                    },
                );
                return Ok(());
            }
        }

        let resp = match source_oss_client
            .get_object(&self.source.bucket.as_str(), record.key.as_str())
            .await
        {
            core::result::Result::Ok(resp) => resp,
            Err(e) => {
                // 源端文件不存在按传输成功处理
                let service_err = e.into_service_error();
                match service_err.kind {
                    GetObjectErrorKind::NoSuchKey(_) => {
                        return Ok(());
                    }
                    _ => {
                        return Err(service_err.into());
                    }
                }
            }
        };

        let mut t_file = OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(target_file)?;

        oss_download_to_file(
            resp,
            &mut t_file,
            self.large_file_size,
            self.multi_part_chunk,
        )
        .await
    }
}
