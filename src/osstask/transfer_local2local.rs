use super::task_actions::TransferTaskActions;
use super::{
    gen_file_path, IncrementAssistant, LocalNotify, TaskStage, TaskStatusSaver,
    TransferTaskAttributes, ERROR_RECORD_PREFIX, NOTIFY_FILE_PREFIX, OFFSET_PREFIX,
};
use crate::checkpoint::ListedRecord;
use crate::checkpoint::{FilePosition, Opt, RecordDescription};
use crate::commons::{
    copy_file, scan_folder_files_last_modify_greater_then_to_file, scan_folder_files_to_file,
    Modified, ModifyType, NotifyWatcher, PathType,
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
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::sync::Mutex;
use tokio::task::{self, JoinSet};

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
        last_modify_timestamp: Option<i64>,
        object_list_file: &str,
    ) -> Result<usize> {
        match last_modify_timestamp {
            Some(t) => {
                let timestamp = TryInto::<u64>::try_into(t)?;
                scan_folder_files_last_modify_greater_then_to_file(
                    self.source.as_str(),
                    &object_list_file,
                    timestamp,
                )
            }
            None => scan_folder_files_to_file(self.source.as_str(), &object_list_file),
        }
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
        assistant: Arc<Mutex<IncrementAssistant>>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        snapshot_stop_mark: Arc<AtomicBool>,
        start_file_position: FilePosition,
    ) {
        let lock = assistant.lock().await;
        let local_notify = match lock.local_notify.clone() {
            Some(n) => n,
            None => return,
        };
        drop(lock);

        let mut offset = TryInto::<u64>::try_into(start_file_position.offset).unwrap();
        let mut line_num = start_file_position.line_num;

        let subffix = offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        // 启动 checkpoint 记录器
        let task_status_saver = TaskStatusSaver {
            check_point_path: assistant.lock().await.check_point_path.clone(),
            execute_file_path: local_notify.notify_file_path.clone(),
            stop_mark: Arc::clone(&snapshot_stop_mark),
            list_file_positon_map: Arc::clone(&offset_map),
            file_for_notify: Some(local_notify.notify_file_path.clone()),
            task_stage: TaskStage::Increment,
            interval: 3,
        };

        task::spawn(async move {
            task_status_saver.snapshot_to_file().await;
        });

        let error_file_name =
            gen_file_path(&self.attributes.meta_dir, ERROR_RECORD_PREFIX, &subffix);

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
                snapshot_stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
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
                            &local_notify.notify_file_path,
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
                                list_file_path: local_notify.notify_file_path.clone(),
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

            if records.len() > 0 {
                let copy = Local2LocalExecutor {
                    source: self.source.clone(),
                    target: self.target.clone(),
                    err_counter: Arc::clone(&err_counter),
                    offset_map: Arc::clone(&offset_map),
                    meta_dir: self.attributes.meta_dir.clone(),
                    target_exist_skip: false,
                    large_file_size: self.attributes.large_file_size,
                    multi_part_chunk: self.attributes.multi_part_chunk,
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

    // async fn execute_increment_from_checkpoint(
    //     &self,
    //     assistant: &IncrementAssistant,
    //     err_counter: Arc<AtomicUsize>,
    //     offset_map: Arc<DashMap<String, FilePosition>>,
    //     snapshot_stop_mark: Arc<AtomicBool>,
    // ) {
    //     todo!()
    // }
}

impl TransferLocal2Local {
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
                    &self.err_counter,
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
        if self.target_exist_skip {
            if t_path.exists() {
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
