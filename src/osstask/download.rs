use crate::{
    checkpoint::{FilePosition, ListedRecord, Opt, RecordDescription},
    commons::{json_to_struct, read_lines},
    exception::save_error_record,
    s3::{
        aws_s3::{byte_stream_multi_partes_to_file, byte_stream_to_file},
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
    err_process, gen_file_path, task_actions::TaskActionsFromOss, TaskAttributes, TaskType,
    CURRENT_LINE_PREFIX, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct DownloadTask {
    pub source: OSSDescription,
    pub local_path: String,
    pub task_attributes: TaskAttributes,
}

impl Default for DownloadTask {
    fn default() -> Self {
        Self {
            source: OSSDescription::default(),
            local_path: "/tmp".to_string(),
            task_attributes: TaskAttributes::default(),
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
                            local_path: self.local_path.clone(),
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
        // 预清理meta目录
        // let _ = fs::remove_dir_all(self.task_attributes.meta_dir.as_str());
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
            local_path: self.local_path.clone(),
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
    pub local_path: String,
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
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        let mut current_line_key = CURRENT_LINE_PREFIX.to_string();
        offset_key.push_str(&subffix);
        current_line_key.push_str(&records[0].offset.to_string());
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
                            save_error_record(&self.err_counter, record.clone(), &mut error_file);
                        }
                        GetObjectErrorKind::NoSuchKey(_) => {}
                        _ => {}
                    }

                    self.offset_map.insert(
                        offset_key.clone(),
                        FilePosition {
                            offset: record.offset,
                            line_num: record.line_num,
                        },
                    );
                    continue;
                }
            };
            let t_file_name = gen_file_path(self.local_path.as_str(), &record.key.as_str(), "");
            let t_path = Path::new(t_file_name.as_str());

            // 目标object存在则不下载
            if self.target_exist_skip {
                if t_path.exists() {
                    self.offset_map.insert(
                        offset_key.clone(),
                        FilePosition {
                            offset: record.offset,
                            line_num: record.line_num,
                        },
                    );
                    continue;
                }
            }

            if let Some(p) = t_path.parent() {
                if let Err(e) = std::fs::create_dir_all(p) {
                    log::error!("{}", e);
                    save_error_record(&self.err_counter, record.clone(), &mut error_file);
                    self.offset_map.insert(
                        offset_key.clone(),
                        FilePosition {
                            offset: record.offset,
                            line_num: record.line_num,
                        },
                    );
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
                    record_desc.error_handler(
                        anyhow!("{}", e),
                        &self.err_counter,
                        &self.offset_map,
                        &mut error_file,
                        offset_key.as_str(),
                    );
                    continue;
                }
            };

            let s_len: usize = match resp.content_length().try_into() {
                Ok(len) => len,
                Err(e) => {
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
                    record_desc.error_handler(
                        anyhow!("{}", e),
                        &self.err_counter,
                        &self.offset_map,
                        &mut error_file,
                        offset_key.as_str(),
                    );
                    continue;
                }
            };

            // 大文件走 multi part download 分支
            match s_len > self.large_file_size {
                true => {
                    if let Err(e) =
                        byte_stream_multi_partes_to_file(resp, &mut t_file, self.multi_part_chunk)
                            .await
                    {
                        // err_process(
                        //     &self.err_counter,
                        //     anyhow!(e.to_string()),
                        //     record,
                        //     &mut error_file,
                        //     offset_key.as_str(),
                        //     current_line_key.as_str(),
                        //     &self.offset_map,
                        // );
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
                        record_desc.error_handler(
                            anyhow!("{}", e),
                            &self.err_counter,
                            &self.offset_map,
                            &mut error_file,
                            offset_key.as_str(),
                        );
                        continue;
                    };
                }
                false => {
                    if let Err(e) = byte_stream_to_file(resp.body, &mut t_file).await {
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
                        record_desc.error_handler(
                            anyhow!("{}", e),
                            &self.err_counter,
                            &self.offset_map,
                            &mut error_file,
                            offset_key.as_str(),
                        );
                        continue;
                    };
                }
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
        self.offset_map.remove(&current_line_key);
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
