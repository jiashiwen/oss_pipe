use crate::{
    checkpoint::{ExecutedFile, FilePosition, ListedRecord, Opt, RecordDescription},
    commons::{json_to_struct, read_lines},
    s3::{
        aws_s3::{oss_download_to_file, OssClient},
        OSSDescription,
    },
};
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::error::GetObjectErrorKind;
use dashmap::DashMap;
use regex::RegexSet;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, Write},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::Mutex,
    task::{self, JoinSet},
};
use walkdir::WalkDir;

use super::{
    gen_file_path, task_actions::TransferTaskActions, IncrementAssistant, TaskStage,
    TaskStatusSaver, TransferTaskAttributes, ERROR_RECORD_PREFIX, OBJECT_LIST_FILE_PREFIX,
    OFFSET_PREFIX,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferOss2Local {
    pub source: OSSDescription,
    pub target: String,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferOss2Local {
    fn default() -> Self {
        Self {
            source: OSSDescription::default(),
            target: "/tmp".to_string(),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferOss2Local {
    fn error_record_retry(&self) -> Result<()> {
        // 遍历错误记录
        // 每个错误文件重新处理
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
                        let download = Oss2LocalListedRecordsExecutor {
                            target: self.target.clone(),
                            source: self.source.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            meta_dir: self.attributes.meta_dir.clone(),
                            target_exist_skip: self.attributes.target_exists_skip,
                            large_file_size: self.attributes.large_file_size,
                            multi_part_chunk: self.attributes.multi_part_chunk,
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

    async fn generate_execute_file(
        &self,
        last_modify_timestamp: Option<i64>,
        object_list_file: &str,
    ) -> Result<ExecutedFile> {
        let client_source = self.source.gen_oss_client()?;

        // 若为持续同步模式，且 last_modify_timestamp 大于 0，则将 last_modify 属性大于last_modify_timestamp变量的对象加入执行列表
        match last_modify_timestamp {
            Some(t) => {
                client_source
                    .append_last_modify_greater_object_to_file(
                        self.source.bucket.clone(),
                        self.source.prefix.clone(),
                        self.attributes.bach_size,
                        object_list_file,
                        t,
                    )
                    .await
            }
            None => {
                client_source
                    .append_all_object_list_to_file(
                        self.source.bucket.clone(),
                        self.source.prefix.clone(),
                        self.attributes.bach_size,
                        object_list_file,
                    )
                    .await
            }
        }
    }

    async fn records_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        records: Vec<ListedRecord>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    ) {
        let download = Oss2LocalListedRecordsExecutor {
            target: self.target.clone(),
            source: self.source.clone(),
            err_counter,
            offset_map,
            meta_dir: self.attributes.meta_dir.clone(),
            target_exist_skip: false,
            large_file_size: self.attributes.large_file_size,
            multi_part_chunk: self.attributes.multi_part_chunk,
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

    async fn increment_prelude(&self, assistant: Arc<Mutex<IncrementAssistant>>) -> Result<()> {
        // 记录当前时间戳
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let timestampe = TryInto::<i64>::try_into(now.as_secs())?;
        assistant.lock().await.last_modify_timestamp = Some(timestampe);
        Ok(())
    }

    async fn execute_increment(
        &self,
        mut execute_set: &mut JoinSet<()>,
        assistant: Arc<Mutex<IncrementAssistant>>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        snapshot_stop_mark: Arc<AtomicBool>,
        start_file_position: FilePosition,
    ) {
        // 循环执行获取lastmodify 大于checkpoint指定的时间戳的对象
        let timestampe = match assistant.lock().await.last_modify_timestamp {
            Some(t) => t,
            None => {
                return;
            }
        };

        let mut executed_file = ExecutedFile {
            path: gen_file_path(
                self.attributes.meta_dir.as_str(),
                OBJECT_LIST_FILE_PREFIX,
                timestampe.to_string().as_str(),
            ),
            size: 0,
            total_lines: 0,
        };

        // 启动checkpoint记录线程
        let stock_status_saver = Arc::new(Mutex::new(TaskStatusSaver {
            check_point_path: assistant.lock().await.check_point_path.clone(),
            executed_file: executed_file.clone(),
            stop_mark: Arc::clone(&snapshot_stop_mark),
            list_file_positon_map: Arc::clone(&offset_map),
            file_for_notify: None,
            task_stage: TaskStage::Increment,
            interval: 3,
        }));
        let saver = Arc::clone(&stock_status_saver);
        task::spawn(async move {
            saver.lock().await.snapshot_to_file().await;
        });
        // drop(saver);

        let mut exclude_regex_set: Option<RegexSet> = None;
        let mut include_regex_set: Option<RegexSet> = None;

        if let Some(vec_regex_str) = self.attributes.exclude.clone() {
            let set = RegexSet::new(&vec_regex_str).unwrap();
            exclude_regex_set = Some(set);
        };

        if let Some(vec_regex_str) = self.attributes.include.clone() {
            let set = RegexSet::new(&vec_regex_str).unwrap();
            include_regex_set = Some(set);
        };
        // loop {
        while !snapshot_stop_mark.load(std::sync::atomic::Ordering::SeqCst)
            && self
                .attributes
                .max_errors
                .le(&err_counter.load(std::sync::atomic::Ordering::SeqCst))
        {
            let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(n) => n,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };
            let obj_list = match self
                .generate_execute_file(Some(timestampe), &executed_file.path)
                .await
            {
                Ok(list) => list,
                Err(e) => {
                    log::error!("{}", e);
                    err_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    continue;
                }
            };

            let mut vec_keys: Vec<ListedRecord> = vec![];
            let mut list_file_position = FilePosition::default();
            let object_list_file = match File::open(&executed_file.path) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{}", e);
                    err_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    continue;
                }
            };
            // 按列表传输object from source to target
            let lines: io::Lines<io::BufReader<File>> =
                io::BufReader::new(object_list_file).lines();
            for line in lines {
                // 若错误达到上限，则停止任务
                if err_counter.load(std::sync::atomic::Ordering::SeqCst)
                    >= self.attributes.max_errors
                {
                    break;
                }
                if let Result::Ok(key) = line {
                    let len = key.bytes().len() + "\n".bytes().len();
                    list_file_position.offset += len;
                    list_file_position.line_num += 1;

                    if !key.ends_with("/") {
                        let record = ListedRecord {
                            key,
                            offset: list_file_position.offset,
                            line_num: list_file_position.line_num,
                        };
                        match exclude_regex_set {
                            Some(ref exclude) => {
                                if exclude.is_match(&record.key) {
                                    continue;
                                }
                            }
                            None => {}
                        }
                        match include_regex_set {
                            Some(ref set) => {
                                if set.is_match(&record.key) {
                                    vec_keys.push(record);
                                }
                            }
                            None => {
                                vec_keys.push(record);
                            }
                        }
                    }
                };

                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.bach_size.to_string())
                {
                    while execute_set.len() >= self.attributes.task_threads {
                        execute_set.join_next().await;
                    }
                    let vk: Vec<ListedRecord> = vec_keys.clone();
                    self.records_excutor(
                        &mut execute_set,
                        vk,
                        Arc::clone(&err_counter),
                        Arc::clone(&offset_map),
                        executed_file.path.clone(),
                    )
                    .await;

                    // 清理临时key vec
                    vec_keys.clear();
                }
            }

            // 处理集合中的剩余数据，若错误达到上限，则不执行后续操作
            if vec_keys.len() > 0
                && err_counter.load(std::sync::atomic::Ordering::SeqCst)
                    < self.attributes.max_errors
            {
                while execute_set.len() >= self.attributes.task_threads {
                    execute_set.join_next().await;
                }

                let vk = vec_keys.clone();
                self.records_excutor(
                    &mut execute_set,
                    vk,
                    Arc::clone(&err_counter),
                    Arc::clone(&offset_map),
                    executed_file.path.clone(),
                )
                .await;
            }

            while execute_set.len() > 0 {
                execute_set.join_next().await;
            }

            executed_file = ExecutedFile {
                path: gen_file_path(
                    self.attributes.meta_dir.as_str(),
                    OBJECT_LIST_FILE_PREFIX,
                    now.as_secs().to_string().as_str(),
                ),
                size: 0,
                total_lines: 0,
            };
            let mut lock = stock_status_saver.lock().await;
            lock.set_executed_file(executed_file.clone());
            drop(lock);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Oss2LocalListedRecordsExecutor {
    pub source: OSSDescription,
    pub target: String,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
    pub list_file_path: String,
}

impl Oss2LocalListedRecordsExecutor {
    pub async fn exec(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);
        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;
        for record in records {
            // 文件位置提前记录避免漏记
            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );

            let t_file_name = gen_file_path(self.target.as_str(), &record.key.as_str(), "");
            if let Err(e) = self
                .record_handler(&record, &c_s, t_file_name.as_str())
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
