use super::task_actions::TaskActionsFromLocal;
use super::TaskType;
use super::TransferTaskAttributes;
use super::OFFSET_PREFIX;
use super::{gen_file_path, ERROR_RECORD_PREFIX};
use crate::checkpoint::{FilePosition, Opt, RecordDescription};
use crate::commons::{
    json_to_struct, read_lines, scan_folder_files_to_file, Modified, ModifyType, NotifyWatcher,
    PathType,
};
use crate::s3::aws_s3::OssClient;
use crate::{checkpoint::ListedRecord, s3::OSSDescription};
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::from_str;
use std::fs::File;
use std::io::{self, BufRead, Seek, SeekFrom};
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
pub struct UploadTask {
    pub source: String,
    pub target: OSSDescription,
    pub task_attributes: TransferTaskAttributes,
}

impl Default for UploadTask {
    fn default() -> Self {
        Self {
            target: OSSDescription::default(),
            source: "/tmp".to_string(),
            task_attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TaskActionsFromLocal for UploadTask {
    fn task_type(&self) -> TaskType {
        TaskType::Upload
    }
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
                        let upload = UpLoadExecutor {
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
        let upload = UpLoadExecutor {
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
            if let Err(e) = upload.exec_listed_records(records).await {
                upload
                    .err_counter
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
            let total_rs = scan_folder_files_to_file(self.source.as_str(), &object_list_file);
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
        let watcher = NotifyWatcher::new(self.source.as_str())?;
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
        let client = match self.target.gen_oss_client() {
            Ok(c) => c,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };

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

        // Todo
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

            let lines = io::BufReader::new(file).lines();
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

            let executer = UpLoadExecutor {
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
                let _ = executer.exec_record_descriptions(records).await;
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
            println!("{}", offset);
        }
    }
}

impl UploadTask {
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
        if let Some(prefix) = self.target.prefix.clone() {
            target_path.insert_str(0, &prefix);
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
pub struct UpLoadExecutor {
    pub source: String,
    pub target: OSSDescription,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
    pub list_file_path: String,
}

impl UpLoadExecutor {
    pub async fn exec_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        // 先写首行日志，避免错误漏记
        let positon: FilePosition = FilePosition {
            offset: records[0].offset,
            line_num: records[0].line_num,
        };
        self.offset_map.insert(offset_key.clone(), positon);

        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);
        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let target_oss_client = self.target.gen_oss_client()?;
        for record in records {
            let source_file_path = gen_file_path(self.source.as_str(), &record.key.as_str(), "");
            let mut target_key = "".to_string();
            if let Some(s) = self.target.prefix.clone() {
                target_key.push_str(&s);
            };
            target_key.push_str(&record.key);

            if let Err(e) = self
                .listed_record_handler(
                    &offset_key,
                    &record,
                    &source_file_path,
                    &target_oss_client,
                    &target_key,
                )
                .await
            {
                let record_desc = RecordDescription {
                    source_key: source_file_path.clone(),
                    target_key: target_key.clone(),
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
                if 0.eq(&meta.len()) {
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
        source_file: &str,
        target_oss: &OssClient,
        target_key: &str,
    ) -> Result<()> {
        // 判断源文件是否存在
        let s_path = Path::new(source_file);
        if !s_path.exists() {
            let positon = FilePosition {
                offset: record.offset,
                line_num: record.line_num,
            };
            self.offset_map.insert(offset_key.to_string(), positon);
            return Ok(());
        }

        // 目标object存在则不推送
        if self.target_exist_skip {
            let target_obj_exists = target_oss
                .object_exists(self.target.bucket.as_str(), target_key)
                .await?;

            if target_obj_exists {
                return Ok(());
            }
        }

        target_oss
            .upload_from_local(
                self.target.bucket.as_str(),
                target_key,
                source_file,
                self.large_file_size,
                self.multi_part_chunk,
            )
            .await
    }

    //Todo
    // 重构
    pub async fn exec_record_descriptions(&self, records: Vec<RecordDescription>) -> Result<()> {
        let subffix = records[0].list_file_position.offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        // 先写首行日志，避免错误漏记
        self.offset_map
            .insert(offset_key.clone(), records[0].list_file_position.clone());

        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_t = self.target.gen_oss_client()?;

        for record in records {
            if let Err(e) = self.record_description_handler(&record, &c_t).await {
                record.handle_error(
                    // e,
                    &self.err_counter,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
                log::error!("{}", e);
            }

            self.offset_map
                .insert(offset_key.clone(), record.list_file_position.clone());
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

    async fn record_description_handler(
        &self,
        record: &RecordDescription,
        target_oss: &OssClient,
    ) -> Result<()> {
        let s_path = Path::new(&record.source_key);
        if !s_path.exists() {
            return Ok(());
        }

        // 目标object存在则不推送
        if self.target_exist_skip {
            if target_oss
                .object_exists(self.target.bucket.as_str(), &record.target_key)
                .await?
            {
                return Ok(());
            }
        }

        match record.option {
            Opt::PUT => {
                target_oss
                    .upload_from_local(
                        self.target.bucket.as_str(),
                        &record.target_key,
                        &record.source_key,
                        self.large_file_size,
                        self.multi_part_chunk,
                    )
                    .await
            }
            Opt::REMOVE => {
                let _ = target_oss
                    .remove_object(self.target.bucket.as_str(), &record.target_key)
                    .await
                    .map_err(|e| anyhow::Error::new(e))?;
                Ok(())
            }
            Opt::UNKOWN => Err(anyhow!("option unkown")),
        }
    }
}
