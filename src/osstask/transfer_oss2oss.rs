use super::{
    gen_file_path,
    task_actions::{TaskActionsFromOss, TransferTaskActions},
    TaskAttributes, TaskType, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX,
};
use crate::{
    checkpoint::{FilePosition, ListedRecord, Opt, RecordDescription},
    commons::{json_to_struct, read_lines},
    s3::{aws_s3::OssClient, OSSDescription},
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::error::GetObjectErrorKind;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::{runtime::Runtime, task::JoinSet};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferOss2Oss {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub task_attributes: TaskAttributes,
}

impl Default for TransferOss2Oss {
    fn default() -> Self {
        Self {
            source: OSSDescription::default(),
            target: OSSDescription::default(),
            task_attributes: TaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferOss2Oss {
    // 错误记录重试
    fn error_record_retry(&self) -> Result<()> {
        // 遍历错误记录
        for entry in WalkDir::new(self.task_attributes.meta_dir.as_str())
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir() && e.file_name().to_str().is_some())
        {
            let file_name = entry.file_name().to_str().unwrap();

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
                        let copy = TransferRecordsExecutor {
                            source: self.source.clone(),
                            target: self.target.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            meta_dir: self.task_attributes.meta_dir.clone(),
                            target_exist_skip: self.task_attributes.target_exists_skip,
                            large_file_size: self.task_attributes.large_file_size,
                            multi_part_chunk: self.task_attributes.multi_part_chunk,
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            list_file_path: p.to_string(),
                        };
                        let _ = copy.exec_listed_records(record_vec);
                    }
                }

                let _ = fs::remove_file(p);
            }
        }

        Ok(())
    }
    // 记录执行器
    async fn records_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        records: Vec<ListedRecord>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    ) {
        let transfer = TransferRecordsExecutor {
            source: self.source.clone(),
            target: self.target.clone(),
            err_counter,
            offset_map,
            meta_dir: self.task_attributes.meta_dir.clone(),
            target_exist_skip: false,
            large_file_size: self.task_attributes.large_file_size,
            multi_part_chunk: self.task_attributes.multi_part_chunk,
            list_file_path: list_file,
        };

        joinset.spawn(async move {
            if let Err(e) = transfer.exec_listed_records(records).await {
                transfer
                    .err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }

    // 生成对象列表
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
}

#[derive(Debug, Clone)]
pub struct TransferRecordsExecutor {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
    pub list_file_path: String,
}

impl TransferRecordsExecutor {
    pub async fn exec_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
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
        let c_t = self.target.gen_oss_client()?;
        for record in records {
            let mut target_key = match self.target.prefix.clone() {
                Some(s) => s,
                None => "".to_string(),
            };
            target_key.push_str(&record.key);

            if let Err(e) = self
                .listed_record_handler(&offset_key, &record, &c_s, &c_t, &target_key)
                .await
            {
                let recorddesc = RecordDescription {
                    source_key: record.key.clone(),
                    target_key: target_key.clone(),
                    list_file_path: self.list_file_path.clone(),
                    list_file_position: FilePosition {
                        offset: record.offset,
                        line_num: record.line_num,
                    },
                    option: Opt::PUT,
                };
                recorddesc.error_handler(
                    anyhow!("{}", e),
                    &self.err_counter,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
            }

            // 插入文件offset记录
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

    async fn listed_record_handler(
        &self,
        offset_key: &str,
        record: &ListedRecord,
        source_oss: &OssClient,
        target_oss: &OssClient,
        target_key: &str,
    ) -> Result<()> {
        let resp = match source_oss
            .get_object(&self.source.bucket.as_str(), record.key.as_str())
            .await
        {
            core::result::Result::Ok(resp) => resp,
            Err(e) => {
                // 源端文件不存在按传输成功处理
                let service_err = e.into_service_error();
                match service_err.kind {
                    // GetObjectErrorKind::InvalidObjectState(_)
                    // | GetObjectErrorKind::Unhandled(_) => {
                    //     save_error_record(&self.err_counter, record.clone(), &mut error_file);
                    // }
                    GetObjectErrorKind::NoSuchKey(_) => {
                        self.offset_map.insert(
                            offset_key.to_string(),
                            FilePosition {
                                offset: record.offset,
                                line_num: record.line_num,
                            },
                        );
                        return Ok(());
                    }
                    _ => {
                        return Err(service_err.into());
                    }
                }
            }
        };

        // 目标object存在则不推送
        if self.target_exist_skip {
            let target_obj_exists = target_oss
                .object_exists(self.target.bucket.as_str(), target_key)
                .await?;
            if target_obj_exists {
                return Ok(());
            }
        }
        let content_len = usize::try_from(resp.content_length())?;

        let expr = match resp.expires() {
            Some(datetime) => Some(*datetime),
            None => None,
        };

        // 大文件走 multi part upload 分支
        match content_len > self.large_file_size {
            true => {
                target_oss
                    .multipart_upload_byte_stream(
                        self.target.bucket.as_str(),
                        target_key,
                        expr,
                        content_len,
                        self.multi_part_chunk,
                        resp.body,
                    )
                    .await
            }
            false => {
                target_oss
                    .upload_object_bytes(self.target.bucket.as_str(), target_key, expr, resp.body)
                    .await
            }
        }
    }
}
