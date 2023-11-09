use super::task_actions::TaskActionsFromLocal;
use super::{gen_file_path, TransferTaskAttributes, ERROR_RECORD_PREFIX, OFFSET_PREFIX};
use crate::checkpoint::ListedRecord;
use crate::checkpoint::{FilePosition, Opt, RecordDescription};
use crate::commons::{
    copy_file, json_to_struct, read_lines, scan_folder_files_to_file, Modified, ModifyType,
    NotifyWatcher, PathType,
};
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TaskLocal2Local {
    pub source: String,
    pub target: String,
    pub task_attributes: TransferTaskAttributes,
}

impl Default for TaskLocal2Local {
    fn default() -> Self {
        Self {
            source: "/tmp/source".to_string(),
            target: "/tmp/target".to_string(),
            task_attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TaskActionsFromLocal for TaskLocal2Local {
    // fn task_type(&self) -> TaskType {
    //     TaskType::Upload
    // }
    // 错误记录重试
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
                                let record =
                                    match json_to_struct::<RecordDescription>(content.as_str()) {
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
                        let upload = LocalToLocal {
                            source: self.source.clone(),
                            target: self.target.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            meta_dir: self.task_attributes.meta_dir.clone(),
                            target_exist_skip: self.task_attributes.target_exists_skip,
                            large_file_size: self.task_attributes.large_file_size,
                            multi_part_chunk: self.task_attributes.multi_part_chunk,
                            list_file_path: p.to_string(),
                        };
                        let _ = upload.exec_record_descriptions(record_vec);
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
        offset_map: Arc<DashMap<std::string::String, FilePosition>>,
        list_file: String,
    ) {
        let copy = LocalToLocal {
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
            if let Err(e) = copy.exec_listed_records(records).await {
                copy.err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }

    // 生成对象列表
    fn generate_object_list(&self, rt: &Runtime, object_list_file: &str) -> Result<usize> {
        let mut interrupted = false;
        let mut total_lines = 0;

        rt.block_on(async {
            // 遍历目录并生成文件列表
            let total_rs = scan_folder_files_to_file(&self.source, &object_list_file);
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

    fn gen_watcher(&self) -> notify::Result<NotifyWatcher> {
        let watcher = NotifyWatcher::new(&self.source)?;
        Ok(watcher)
    }

    // Todo
    // 抽象
    async fn execute_increment(
        &self,
        notify_file: &str,
        notify_file_size: Arc<AtomicU64>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        snapshot_stop_mark: Arc<AtomicBool>,
    ) {
        let mut offset = 0;
        let mut line_num = 0;

        let subffix = offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let error_file_name = gen_file_path(
            &self.task_attributes.meta_dir,
            ERROR_RECORD_PREFIX,
            &subffix,
        );

        loop {
            if notify_file_size.load(Ordering::SeqCst).le(&offset) {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
            if self
                .task_attributes
                .max_errors
                .le(&err_counter.load(std::sync::atomic::Ordering::Relaxed))
            {
                snapshot_stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }

            let mut file = match File::open(notify_file) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            if let Err(e) = file.seek(SeekFrom::Start(offset)) {
                log::error!("{}", e);
                continue;
            };

            let mut error_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(error_file_name.as_str())
                .unwrap();

            let lines = BufReader::new(file).lines();
            let mut offset_usize: usize = TryInto::<usize>::try_into(offset).unwrap();
            let mut records = vec![];
            for line in lines {
                line_num += 1;
                if let Result::Ok(key) = line {
                    // Modifed 解析
                    offset_usize += key.len();
                    match self
                        .modified_str_to_record_description(
                            &key,
                            notify_file,
                            offset_usize,
                            line_num,
                        )
                        .await
                    {
                        Ok(r) => records.push(r),
                        Err(e) => {
                            let r = RecordDescription {
                                source_key: "".to_string(),
                                target_key: "".to_string(),
                                list_file_path: notify_file.to_string(),
                                list_file_position: FilePosition {
                                    offset: offset_usize,
                                    line_num,
                                },
                                option: Opt::UNKOWN,
                            };
                            r.handle_error(
                                &err_counter,
                                &offset_map,
                                &mut error_file,
                                offset_key.as_str(),
                            );
                            log::error!("{}", e);
                        }
                    }
                }
            }

            let copy = LocalToLocal {
                source: self.source.clone(),
                target: self.target.clone(),
                err_counter: Arc::clone(&err_counter),
                offset_map: Arc::clone(&offset_map),
                meta_dir: self.task_attributes.meta_dir.clone(),
                target_exist_skip: false,
                large_file_size: self.task_attributes.large_file_size,
                multi_part_chunk: self.task_attributes.multi_part_chunk,
                list_file_path: notify_file.to_string(),
            };

            if records.len() > 0 {
                let _ = copy.exec_record_descriptions(records).await;
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
            offset = notify_file_size.load(Ordering::SeqCst);
            let offset_usize = TryInto::<usize>::try_into(offset).unwrap();
            let position = FilePosition {
                offset: offset_usize,
                line_num,
            };

            offset_map.remove(&offset_key);
            offset_key = OFFSET_PREFIX.to_string();
            offset_key.push_str(&offset.to_string());
            offset_map.insert(offset_key.clone(), position);
        }
    }
}

impl TaskLocal2Local {
    async fn modified_str_to_record_description(
        &self,
        modified_str: &str,
        list_file_path: &str,
        offset: usize,
        line_num: usize,
    ) -> Result<RecordDescription> {
        let modified = from_str::<Modified>(modified_str)?;
        let mut target_path = modified.path.clone();
        match self.source.ends_with("/") {
            true => target_path.drain(..self.source.len()),
            false => target_path.drain(..self.source.len() + 1),
        };

        match self.target.ends_with("/") {
            true => {
                target_path.insert_str(0, &self.target);
            }
            false => {
                let target_prefix = self.target.clone() + "/";
                target_path.insert_str(0, &target_prefix);
            }
        }

        if PathType::File.eq(&modified.path_type) {
            match modified.modify_type {
                ModifyType::Create | ModifyType::Modify => {
                    let record = RecordDescription {
                        source_key: modified.path.clone(),
                        target_key: target_path,
                        list_file_path: list_file_path.to_string(),
                        list_file_position: FilePosition { offset, line_num },
                        option: Opt::PUT,
                    };
                    return Ok(record);
                }
                ModifyType::Delete => {
                    let record = RecordDescription {
                        source_key: modified.path.clone(),
                        target_key: target_path,
                        list_file_path: list_file_path.to_string(),
                        list_file_position: FilePosition { offset, line_num },
                        option: Opt::REMOVE,
                    };
                    return Ok(record);
                }
                ModifyType::Unkown => Err(anyhow!("Unkown modify type")),
            }
        } else {
            return Err(anyhow!("Unkown modify type"));
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalToLocal {
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

impl LocalToLocal {
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
        let _ = fs::remove_file(offset_key.as_str());

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
        return match record.option {
            Opt::PUT => {
                // 判断源文件是否存储，若不存在则直接返回成功
                let s_path = Path::new(&record.source_key);
                if !s_path.exists() {
                    return Ok(());
                }
                copy_file(
                    &record.source_key,
                    &record.target_key,
                    self.large_file_size,
                    self.multi_part_chunk,
                )
            }
            Opt::REMOVE => match fs::remove_file(record.target_key.as_str()) {
                Ok(_) => Ok(()),
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => Ok(()),
                    _ => Err(anyhow!("{}", e)),
                },
            },
            Opt::UNKOWN => return Err(anyhow!("unknow option")),
        };
    }
}
