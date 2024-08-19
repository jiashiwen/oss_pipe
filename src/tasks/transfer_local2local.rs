use super::task_actions::TransferTaskActions;
use super::{
    gen_file_path, IncrementAssistant, LocalNotify, TaskStatusSaver, TransferStage,
    TransferTaskAttributes, MODIFIED_PREFIX, NOTIFY_FILE_PREFIX, OFFSET_PREFIX, REMOVED_PREFIX,
    TRANSFER_ERROR_RECORD_PREFIX,
};
use crate::checkpoint::{FileDescription, ListedRecord};
use crate::checkpoint::{FilePosition, Opt, RecordDescription};
use crate::commons::{
    analyze_folder_files_size, copy_file, json_to_struct, merge_file, read_lines,
    scan_folder_files_to_file, struct_to_json_string, LastModifyFilter, Modified, ModifyType,
    NotifyWatcher, PathType, RegexFilter,
};
use crate::tasks::TaskDefaultParameters;
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::sync::{Mutex, RwLock};
use tokio::task::{self, JoinSet};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferLocal2Local {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: String,
    pub target: String,
    pub attributes: TransferTaskAttributes,
}

#[async_trait]
impl TransferTaskActions for TransferLocal2Local {
    async fn analyze_source(&self) -> Result<DashMap<String, i128>> {
        let filter = RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)?;
        analyze_folder_files_size(
            &self.source,
            Some(filter),
            self.attributes.last_modify_filter,
        )
    }

    fn error_record_retry(
        &self,
        stop_mark: Arc<AtomicBool>,
        executing_transfers: Arc<RwLock<usize>>,
    ) -> Result<()> {
        // 遍历meta dir 执行所有err开头文件
        for entry in WalkDir::new(self.attributes.meta_dir.as_str())
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

            if !file_name.starts_with(TRANSFER_ERROR_RECORD_PREFIX) {
                continue;
            };

            if let Some(p) = entry.path().to_str() {
                if let Ok(lines) = read_lines(p) {
                    let mut record_vec = vec![];
                    for line in lines {
                        match line {
                            Ok(content) => {
                                let record = json_to_struct::<RecordDescription>(content.as_str())?;
                                record_vec.push(record);
                            }
                            Err(e) => {
                                log::error!("{}", e);
                                return Err(anyhow!("{}", e));
                            }
                        }
                    }

                    if record_vec.len() > 0 {
                        let local2local = TransferLocal2LocalExecutor {
                            source: self.source.clone(),
                            target: self.target.clone(),
                            stop_mark: stop_mark.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            attributes: self.attributes.clone(),
                            list_file_path: p.to_string(),
                        };
                        let _ = local2local.exec_record_descriptions(record_vec);
                    }
                }
                let _ = fs::remove_file(p);
            }
        }

        Ok(())
    }

    async fn listed_records_transfor(
        &self,
        execute_set: &mut JoinSet<()>,
        _executing_transfers: Arc<RwLock<usize>>,
        records: Vec<ListedRecord>,
        stop_mark: Arc<AtomicBool>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    ) {
        let local2local = TransferLocal2LocalExecutor {
            source: self.source.clone(),
            target: self.target.clone(),
            stop_mark: stop_mark.clone(),
            err_counter,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path: list_file,
        };

        execute_set.spawn(async move {
            if let Err(e) = local2local.exec_listed_records(records).await {
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }

    async fn record_descriptions_transfor(
        &self,
        joinset: &mut JoinSet<()>,
        executing_transfers: Arc<RwLock<usize>>,
        records: Vec<RecordDescription>,
        stop_mark: Arc<AtomicBool>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    ) {
        let local2local = TransferLocal2LocalExecutor {
            source: self.source.clone(),
            target: self.target.clone(),
            stop_mark: stop_mark.clone(),
            err_counter,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path: list_file,
        };

        joinset.spawn(async move {
            if let Err(e) = local2local.exec_record_descriptions(records).await {
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }

    async fn gen_source_object_list_file(
        &self,
        last_modify_filter: Option<LastModifyFilter>,
        object_list_file: &str,
    ) -> Result<FileDescription> {
        scan_folder_files_to_file(self.source.as_str(), &object_list_file, last_modify_filter)
    }

    async fn changed_object_capture_based_target(
        &self,
        timestamp: usize,
    ) -> Result<FileDescription> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        // 获取target object 列表 和removed 列表
        // 根据时间戳生成增量列表
        // 合并删除及新增列表

        let last_modify_filter = LastModifyFilter {
            filter_type: crate::commons::LastModifyFilterType::Greater,
            timestamp,
        };

        let removed = gen_file_path(
            &self.attributes.meta_dir,
            REMOVED_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let modified = gen_file_path(
            &self.attributes.meta_dir,
            MODIFIED_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let mut removed_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&removed)?;

        let mut modified_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&modified)?;

        let mut removed_lines = 0;
        let mut modified_lines = 0;

        for entry in WalkDir::new(&self.target)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.target) {
                    continue;
                }

                let key = match &self.target.ends_with("/") {
                    true => &p[self.target.len()..],
                    false => &p[self.target.len() + 1..],
                };

                let source_key_str = gen_file_path(&self.source, key, "");
                let source_path = Path::new(&source_key_str);
                if !source_path.exists() {
                    let record = RecordDescription {
                        source_key: source_key_str,
                        target_key: p.to_string(),
                        list_file_path: "".to_string(),
                        list_file_position: FilePosition::default(),
                        option: Opt::REMOVE,
                    };
                    let record_str = struct_to_json_string(&record)?;
                    let _ = removed_file.write_all(record_str.as_bytes());
                    let _ = removed_file.write_all("\n".as_bytes());
                    removed_lines += 1;
                }
            };
        }

        for entry in WalkDir::new(&self.source)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.source) {
                    continue;
                }

                let key = match &self.source.ends_with("/") {
                    true => &p[self.target.len()..],
                    false => &p[self.target.len() + 1..],
                };

                let target_key_str = gen_file_path(&self.target, key, "");

                let modified_time = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();
                // if last_modify_filter.filter(i128::from(modified_time)) {
                if last_modify_filter.filter(usize::try_from(modified_time).unwrap()) {
                    let record = RecordDescription {
                        source_key: p.to_string(),
                        target_key: target_key_str,
                        list_file_path: "".to_string(),
                        list_file_position: FilePosition::default(),
                        option: Opt::PUT,
                    };
                    let record_str = struct_to_json_string(&record)?;
                    let _ = modified_file.write_all(record_str.as_bytes());
                    let _ = modified_file.write_all("\n".as_bytes());
                    modified_lines += 1;
                }
            };
        }

        removed_file.flush()?;
        modified_file.flush()?;
        let modified_size = modified_file.metadata()?.len();
        let removed_size = removed_file.metadata()?.len();

        merge_file(&modified, &removed, self.attributes.multi_part_chunk_size)?;
        let total_size = removed_size + modified_size;
        let total_lines = removed_lines + modified_lines;

        fs::rename(&removed, &modified)?;
        let file_desc = FileDescription {
            path: modified.to_string(),
            size: total_size,
            total_lines,
        };

        Ok(file_desc)
    }

    async fn increment_prelude(&self, assistant: Arc<Mutex<IncrementAssistant>>) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let notify_file_path = gen_file_path(
            self.attributes.meta_dir.as_str(),
            NOTIFY_FILE_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let watcher = NotifyWatcher::new(&self.source)?;
        let notify_file_size = Arc::new(AtomicU64::new(0));

        let file_for_notify = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(notify_file_path.as_str())?;

        watcher
            .watch_to_file(file_for_notify, Arc::clone(&notify_file_size))
            .await;

        let local_notify = LocalNotify {
            notify_file_path,
            notify_file_size: Arc::clone(&notify_file_size),
        };

        let mut lock = assistant.lock().await;
        lock.set_local_notify(Some(local_notify));
        drop(lock);

        Ok(())
    }

    async fn execute_increment(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_counter: Arc<AtomicUsize>,
        _joinset: &mut JoinSet<()>,
        executing_transfers: Arc<RwLock<usize>>,
        assistant: Arc<Mutex<IncrementAssistant>>,
        offset_map: Arc<DashMap<String, FilePosition>>,
    ) {
        let lock = assistant.lock().await;
        let local_notify = match lock.local_notify.clone() {
            Some(n) => n,
            None => return,
        };
        drop(lock);

        let executed_file = FileDescription {
            path: local_notify.notify_file_path.clone(),
            size: 0,
            total_lines: 0,
        };
        let file_position = FilePosition::default();

        let mut offset = match TryInto::<u64>::try_into(file_position.offset) {
            Ok(o) => o,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };
        let mut line_num = file_position.line_num;

        let subffix = offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        // 启动 checkpoint 记录器
        let task_status_saver = TaskStatusSaver {
            check_point_path: assistant.lock().await.check_point_path.clone(),
            executed_file,
            stop_mark: Arc::clone(&stop_mark),
            list_file_positon_map: Arc::clone(&offset_map),
            file_for_notify: Some(local_notify.notify_file_path.clone()),
            task_stage: TransferStage::Increment,
            interval: 3,
        };

        let task_id = self.task_id.clone();
        task::spawn(async move {
            task_status_saver.snapshot_to_file(task_id).await;
        });

        let error_file_name = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_ERROR_RECORD_PREFIX,
            &subffix,
        );

        let regex_filter =
            match RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

        loop {
            if local_notify
                .notify_file_size
                .load(Ordering::SeqCst)
                .le(&offset)
            {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
            if self
                .attributes
                .max_errors
                .le(&err_counter.load(std::sync::atomic::Ordering::Relaxed))
            {
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }

            let mut file = match File::open(&local_notify.notify_file_path) {
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

            let mut error_file = match OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(error_file_name.as_str())
            {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

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
                            &local_notify.notify_file_path,
                            offset_usize,
                            line_num,
                        )
                        .await
                    {
                        Ok(r) => {
                            if regex_filter.filter(&r.source_key) {
                                records.push(r);
                            }
                        }
                        Err(e) => {
                            let r = RecordDescription {
                                source_key: "".to_string(),
                                target_key: "".to_string(),
                                list_file_path: local_notify.notify_file_path.clone(),
                                list_file_position: FilePosition {
                                    offset: offset_usize,
                                    line_num,
                                },
                                option: Opt::UNKOWN,
                            };
                            r.handle_error(
                                stop_mark.clone(),
                                &err_counter,
                                self.attributes.max_errors,
                                &offset_map,
                                &mut error_file,
                                offset_key.as_str(),
                            );
                            log::error!("{}", e);
                        }
                    }
                }
            }

            if records.len() > 0 {
                let copy = TransferLocal2LocalExecutor {
                    source: self.source.clone(),
                    target: self.target.clone(),
                    stop_mark: stop_mark.clone(),
                    err_counter: Arc::clone(&err_counter),
                    offset_map: Arc::clone(&offset_map),
                    attributes: self.attributes.clone(),
                    list_file_path: local_notify.notify_file_path.clone(),
                };
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
            offset = local_notify.notify_file_size.load(Ordering::SeqCst);
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

impl TransferLocal2Local {
    async fn modified_str_to_record_description(
        &self,
        modified_str: &str,
        list_file_path: &str,
        offset: usize,
        line_num: u64,
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
pub struct TransferLocal2LocalExecutor {
    pub source: String,
    pub target: String,
    pub stop_mark: Arc<AtomicBool>,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: TransferTaskAttributes,
    pub list_file_path: String,
}

impl TransferLocal2LocalExecutor {
    // Todo
    // 如果在批次处理开始前出现报错则整批数据都不执行，需要有逻辑执行错误记录
    pub async fn exec_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);
        let error_file_name = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_ERROR_RECORD_PREFIX,
            &subffix,
        );

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        for record in records {
            // 记录文件执行位置
            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );

            let s_file_name = gen_file_path(self.source.as_str(), record.key.as_str(), "");
            let t_file_name = gen_file_path(self.target.as_str(), record.key.as_str(), "");

            if let Err(e) = self
                .listed_record_handler(s_file_name.as_str(), t_file_name.as_str())
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
                    self.stop_mark.clone(),
                    &self.err_counter,
                    self.attributes.max_errors,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
                log::error!("{}", e);
            };
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

    async fn listed_record_handler(&self, source_file: &str, target_file: &str) -> Result<()> {
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
        if self.attributes.target_exists_skip {
            if t_path.exists() {
                return Ok(());
            }
        }

        copy_file(
            source_file,
            target_file,
            self.attributes.large_file_size,
            self.attributes.multi_part_chunk_size,
        )
    }

    pub async fn exec_record_descriptions(&self, records: Vec<RecordDescription>) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let mut subffix = records[0].list_file_position.offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        subffix.push_str("_");
        subffix.push_str(now.as_secs().to_string().as_str());

        let error_file_name = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_ERROR_RECORD_PREFIX,
            &subffix,
        );

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        for record in records {
            if let Err(e) = self.record_description_handler(&record).await {
                record.handle_error(
                    self.stop_mark.clone(),
                    &self.err_counter,
                    self.attributes.max_errors,
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
                    self.attributes.large_file_size,
                    self.attributes.multi_part_chunk_size,
                )?;
            }
            Opt::REMOVE => fs::remove_file(record.target_key.as_str())?,
            _ => return Err(anyhow!("unknow option")),
        };
        Ok(())
    }
}
