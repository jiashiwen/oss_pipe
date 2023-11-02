use super::task_actions::TransferTaskActions;
use super::{gen_file_path, TransferTaskAttributes, ERROR_RECORD_PREFIX, OFFSET_PREFIX};
use crate::checkpoint::ListedRecord;
use crate::checkpoint::{FilePosition, Opt, RecordDescription};
use crate::commons::{copy_file, scan_folder_files_to_file};
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferLocal2Local {
    pub source: String,
    pub target: String,
    pub attributes: TransferTaskAttributes,
}

#[async_trait]
impl TransferTaskActions for TransferLocal2Local {
    fn error_record_retry(&self) -> Result<()> {
        Ok(())
    }
    async fn records_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        records: Vec<ListedRecord>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    ) {
        let copy = Local2LocalExecutor {
            source: self.source.clone(),
            target: self.target.clone(),
            err_counter,
            offset_map,
            meta_dir: self.attributes.meta_dir.clone(),
            target_exist_skip: false,
            large_file_size: self.attributes.large_file_size,
            multi_part_chunk: self.attributes.multi_part_chunk,
            list_file_path: list_file,
        };

        joinset.spawn(async move {
            if let Err(e) = copy.exec_listed_records(records).await {
                copy.err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }

    async fn generate_object_list(
        &self,
        _last_modify_timestamp: i64,
        object_list_file: &str,
    ) -> Result<usize> {
        // let mut interrupted = false;
        // let mut total_lines = 0;

        // 遍历目录并生成文件列表
        // let total_rs = scan_folder_files_to_file(self.source.as_str(), &object_list_file)?;
        scan_folder_files_to_file(self.source.as_str(), &object_list_file)
    }
}

#[derive(Debug, Clone)]
pub struct Local2LocalExecutor {
    pub source: String,
    pub target: String,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
    pub list_file_path: String,
}

impl Local2LocalExecutor {
    // Todo
    // 如果在批次处理开始前出现报错则整批数据都不执行，需要有逻辑执行错误记录
    pub async fn exec_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
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

        for record in records {
            let s_file_name = gen_file_path(self.source.as_str(), record.key.as_str(), "");
            let t_file_name = gen_file_path(self.target.as_str(), record.key.as_str(), "");

            if let Err(e) = self
                .listed_record_handler(
                    offset_key.as_str(),
                    &record,
                    s_file_name.as_str(),
                    t_file_name.as_str(),
                )
                .await
            {
                // 记录错误记录
                let recorddesc = RecordDescription {
                    source_key: s_file_name,
                    target_key: t_file_name,
                    list_file_path: self.list_file_path.clone(),
                    list_file_position: FilePosition {
                        offset: record.offset,
                        line_num: record.line_num,
                    },
                    option: Opt::PUT,
                };
                recorddesc.handle_error(
                    // anyhow!("{}", e),
                    &self.err_counter,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
                log::error!("{}", e);
            };

            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );
        }

        let _ = error_file.flush();
        match error_file.metadata() {
            Ok(meta) => {
                if meta.len() == 0 {
                    let _ = fs::remove_file(error_file_name.as_str());
                }
            }
            Err(_) => {}
        };
        self.offset_map.remove(&offset_key);

        Ok(())
    }

    async fn listed_record_handler(
        &self,
        offset_key: &str,
        record: &ListedRecord,
        source_file: &str,
        target_file: &str,
    ) -> Result<()> {
        // 判断源文件是否存在，若不存判定为成功传输
        let s_path = Path::new(source_file);
        if !s_path.exists() {
            self.offset_map.insert(
                offset_key.to_string(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );
            return Ok(());
        }

        let t_path = Path::new(target_file);
        if let Some(p) = t_path.parent() {
            std::fs::create_dir_all(p)?
        };

        // 目标object存在则不推送
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

        copy_file(
            source_file,
            target_file,
            self.large_file_size,
            self.multi_part_chunk,
        )
    }

    pub async fn exec_record_descriptions(&self, records: Vec<RecordDescription>) -> Result<()> {
        let subffix = records[0].list_file_position.offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);
        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        for record in records {
            if let Err(e) = self.record_description_handler(&record).await {
                record.handle_error(
                    // anyhow!("{}", e),
                    &self.err_counter,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
                log::error!("{}", e);
            }
        }

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

    pub async fn record_description_handler(&self, record: &RecordDescription) -> Result<()> {
        match record.option {
            Opt::PUT => {
                copy_file(
                    &record.source_key,
                    &record.target_key,
                    self.large_file_size,
                    self.multi_part_chunk,
                )?;
            }
            Opt::REMOVE => fs::remove_file(record.target_key.as_str())?,
            Opt::UNKOWN => return Err(anyhow!("unknow option")),
        };
        Ok(())
    }
}
