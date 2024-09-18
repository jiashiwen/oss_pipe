use super::task_actions::TransferTaskActions;
use super::IncrementAssistant;
use super::LocalNotify;
use super::TaskStatusSaver;
use super::TransferStage;
use super::TransferTaskAttributes;
use super::MODIFIED_PREFIX;
use super::NOTIFY_FILE_PREFIX;
use super::OFFSET_PREFIX;
use super::REMOVED_PREFIX;
use super::{gen_file_path, TRANSFER_ERROR_RECORD_PREFIX};
use crate::checkpoint::{FileDescription, FilePosition, Opt, RecordDescription};
use crate::commons::merge_file;
use crate::commons::struct_to_json_string;
use crate::commons::{
    analyze_folder_files_size, json_to_struct, read_lines, scan_folder_files_to_file,
    LastModifyFilter, Modified, ModifyType, NotifyWatcher, PathType, RegexFilter,
};
use crate::s3::oss_client::OssClient;
use crate::tasks::TaskDefaultParameters;
use crate::{checkpoint::ListedRecord, s3::OSSDescription};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
// use aws_sdk_s3::model::Object;
use aws_sdk_s3::types::Object;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::io::{BufRead, Seek, SeekFrom};
use std::sync::Arc;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Write},
    path::Path,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::{sync::Mutex, task};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferLocal2Oss {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: String,
    pub target: OSSDescription,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferLocal2Oss {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            target: OSSDescription::default(),
            source: "/tmp".to_string(),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferLocal2Oss {
    async fn analyze_source(&self) -> Result<DashMap<String, i128>> {
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        analyze_folder_files_size(
            &self.source,
            regex_filter,
            self.attributes.last_modify_filter.clone(),
        )
    }
    // 错误记录重试
    fn error_record_retry(
        &self,
        stop_mark: Arc<AtomicBool>,
        _executing_transfers: Arc<RwLock<usize>>,
    ) -> Result<()> {
        // 遍历错误记录
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
                                log::error!("{:?}", e);
                                return Err(anyhow!("{}", e));
                            }
                        }
                    }

                    if record_vec.len() > 0 {
                        let upload = TransferLocal2OssExecuter {
                            source: self.source.clone(),
                            target: self.target.clone(),
                            stop_mark: stop_mark.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            attributes: self.attributes.clone(),
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
    async fn listed_records_transfor(
        &self,
        execute_set: &mut JoinSet<()>,
        executing_transfers: Arc<RwLock<usize>>,
        records: Vec<ListedRecord>,
        stop_mark: Arc<AtomicBool>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<std::string::String, FilePosition>>,
        list_file: String,
    ) {
        if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let local2oss = TransferLocal2OssExecuter {
            source: self.source.clone(),
            target: self.target.clone(),
            stop_mark: stop_mark.clone(),
            err_counter,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path: list_file,
        };

        execute_set.spawn(async move {
            if let Err(e) = local2oss
                .exec_listed_records(records, executing_transfers)
                .await
            {
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{:?}", e);
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
        offset_map: Arc<DashMap<std::string::String, FilePosition>>,
        list_file: String,
    ) {
        let local2oss = TransferLocal2OssExecuter {
            source: self.source.clone(),
            target: self.target.clone(),
            stop_mark: stop_mark.clone(),
            err_counter,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path: list_file,
        };

        joinset.spawn(async move {
            if let Err(e) = local2oss.exec_record_descriptions(records).await {
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{:?}", e);
            };
        });
    }

    // 生成对象列表
    async fn gen_source_object_list_file(
        &self,
        // last_modify_filter: Option<LastModifyFilter>,
        object_list_file: &str,
    ) -> Result<FileDescription> {
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        scan_folder_files_to_file(
            self.source.as_str(),
            &object_list_file,
            regex_filter,
            self.attributes.last_modify_filter,
        )
    }

    async fn changed_object_capture_based_target(
        &self,
        timestamp: usize,
    ) -> Result<FileDescription> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
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

        let last_modify_filter = LastModifyFilter {
            filter_type: crate::commons::LastModifyFilterType::Greater,
            timestamp,
        };

        let mut process_target_objects = |objects: Vec<Object>| {
            for obj in objects {
                if let Some(target_key) = obj.key() {
                    let mut source_key = "";
                    if let Some(p) = &self.target.prefix {
                        source_key = match p.ends_with("/") {
                            true => &p[p.len()..],
                            false => &p[p.len() + 1..],
                        };
                    };
                    let source_key_str = gen_file_path(&self.source, source_key, "");
                    let source_path = Path::new(&source_key_str);
                    if !source_path.exists() {
                        let record = RecordDescription {
                            source_key: source_key_str,
                            target_key: target_key.to_string(),
                            list_file_path: "".to_string(),
                            list_file_position: FilePosition::default(),
                            option: Opt::REMOVE,
                        };
                        let record_str = match struct_to_json_string(&record) {
                            Ok(r) => r,
                            Err(e) => {
                                log::error!("{:?}", e);
                                return;
                            }
                        };
                        let _ = removed_file.write_all(record_str.as_bytes());
                        let _ = removed_file.write_all("\n".as_bytes());
                        removed_lines += 1;
                    }
                }
            }
        };

        let target_client = self.target.gen_oss_client()?;
        let resp = target_client
            .list_objects(
                &self.target.bucket,
                self.target.prefix.clone(),
                self.attributes.objects_per_batch,
                None,
            )
            .await?;
        let mut token = resp.next_token;
        if let Some(objects) = resp.object_list {
            process_target_objects(objects);
        }

        while token.is_some() {
            let resp = target_client
                .list_objects(
                    &self.target.bucket,
                    self.target.prefix.clone(),
                    self.attributes.objects_per_batch,
                    None,
                )
                .await?;
            if let Some(objects) = resp.object_list {
                process_target_objects(objects);
            }
            token = resp.next_token;
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
                    true => &p[self.source.len()..],
                    false => &p[self.source.len() + 1..],
                };

                let mut target_key = "".to_string();
                if let Some(p) = &self.target.prefix {
                    target_key.push_str(p);
                }
                target_key.push_str(key);

                let modified_time = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();
                // if last_modify_filter.filter(i128::from(modified_time)) {
                if last_modify_filter.filter(usize::try_from(modified_time).unwrap()) {
                    let record = RecordDescription {
                        source_key: p.to_string(),
                        target_key: target_key.to_string(),
                        list_file_path: "".to_string(),
                        list_file_position: FilePosition::default(),
                        option: Opt::PUT,
                    };
                    let record_str = match struct_to_json_string(&record) {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("{:?}", e);
                            return Err(e);
                        }
                    };
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

        let file_for_notify = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(notify_file_path.as_str())?;

        let watcher = NotifyWatcher::new(&self.source)?;
        let notify_file_size = Arc::new(AtomicU64::new(0));

        let local_notify = LocalNotify {
            notify_file_size: Arc::clone(&notify_file_size),
            notify_file_path,
        };

        let mut lock = assistant.lock().await;
        lock.set_local_notify(Some(local_notify));
        drop(lock);

        watcher
            .watch_to_file(file_for_notify, Arc::clone(&notify_file_size))
            .await;

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
            None => {
                return;
            }
        };
        drop(lock);

        let mut offset = 0;
        let mut line_num = 0;

        let subffix = offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let executed_file = FileDescription {
            path: local_notify.notify_file_path.clone(),
            size: 0,
            total_lines: 0,
        };

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
                    log::error!("{:?}", e);
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
                    log::error!("{:?}", e);
                    return;
                }
            };

            if let Err(e) = file.seek(SeekFrom::Start(offset)) {
                log::error!("{:?}", e);
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
                            log::error!("{:?}", e);
                        }
                    }
                }
            }

            let local_2_oss = TransferLocal2OssExecuter {
                source: self.source.clone(),
                target: self.target.clone(),
                stop_mark: stop_mark.clone(),
                err_counter: Arc::clone(&err_counter),
                offset_map: Arc::clone(&offset_map),
                attributes: self.attributes.clone(),
                list_file_path: local_notify.notify_file_path.clone(),
            };

            if records.len() > 0 {
                let _ = local_2_oss.exec_record_descriptions(records).await;
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

impl TransferLocal2Oss {
    async fn modified_str_to_record_description(
        &self,
        modified_str: &str,
        list_file_path: &str,
        offset: usize,
        line_num: u64,
    ) -> Result<RecordDescription> {
        let modified = from_str::<Modified>(modified_str)?;
        let mut target_path = modified.path.clone();

        // 截取 target key 相对路径
        match self.source.ends_with("/") {
            true => target_path.drain(..self.source.len()),
            false => target_path.drain(..self.source.len() + 1),
        };

        // 补全prefix
        if let Some(mut oss_path) = self.target.prefix.clone() {
            match oss_path.ends_with("/") {
                true => target_path.insert_str(0, &oss_path),
                false => {
                    oss_path.push('/');
                    target_path.insert_str(0, &oss_path);
                }
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
pub struct TransferLocal2OssExecuter {
    pub source: String,
    pub target: OSSDescription,
    pub stop_mark: Arc<AtomicBool>,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: TransferTaskAttributes,
    pub list_file_path: String,
}

impl TransferLocal2OssExecuter {
    pub async fn exec_listed_records(
        &self,
        records: Vec<ListedRecord>,
        executing_transfers: Arc<RwLock<usize>>,
    ) -> Result<()> {
        let mut offset_key = OFFSET_PREFIX.to_string();
        let subffix = records[0].offset.to_string();
        offset_key.push_str(&subffix);

        // Todo
        // 若第一行出错则整组record写入错误记录，若错误记录文件打开报错则停止任务
        let error_file_name = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_ERROR_RECORD_PREFIX,
            &records[0].offset.to_string(),
        );

        let mut error_file = match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())
        {
            Ok(ef) => ef,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(anyhow!(e));
            }
        };

        let target_oss_client = self.target.gen_oss_client()?;

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }

            let source_file_path = gen_file_path(self.source.as_str(), &record.key.as_str(), "");
            let mut target_key = "".to_string();
            if let Some(s) = self.target.prefix.clone() {
                target_key.push_str(&s);
            };
            target_key.push_str(&record.key);

            let e_u = Arc::clone(&executing_transfers);
            if let Err(e) = self
                .listed_record_handler(e_u, &source_file_path, &target_oss_client, &target_key)
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
                    self.stop_mark.clone(),
                    &self.err_counter,
                    self.attributes.max_errors,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
                log::error!("{:?}", e);
            }

            // 文件位置记录后置，避免中断时已记录而传输未完成，续传时丢记录
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
        executing_transfers: Arc<RwLock<usize>>,
        source_file: &str,
        target_oss: &OssClient,
        target_key: &str,
    ) -> Result<()> {
        // 判断源文件是否存在
        let s_path = Path::new(source_file);
        if !s_path.exists() {
            return Ok(());
        }

        // 目标object存在则不推送
        if self.attributes.target_exists_skip {
            let target_obj_exists = target_oss
                .object_exists(self.target.bucket.as_str(), target_key)
                .await?;

            if target_obj_exists {
                return Ok(());
            }
        }

        // target_oss
        //     .upload_local_file(
        //         self.target.bucket.as_str(),
        //         target_key,
        //         source_file,
        //         self.attributes.large_file_size,
        //         self.attributes.multi_part_chunk,
        //     )
        //     .await

        // ToDo
        // 新增阐述chunk_batch 定义分片上传每批上传分片的数量
        target_oss
            .upload_local_file_paralle(
                source_file,
                self.target.bucket.as_str(),
                target_key,
                self.attributes.large_file_size,
                Arc::clone(&executing_transfers),
                self.attributes.multi_part_chunk_size,
                self.attributes.multi_part_chunks_per_batch,
                self.attributes.multi_part_parallelism,
            )
            .await
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

        let c_t = self.target.gen_oss_client()?;

        // Todo
        // 增加去重逻辑，当两条记录相邻为 create和modif时只put一次
        // 增加目录删除逻辑，对应oss删除指定prefix下的所有文件，文件系统删除目录
        for record in records {
            // 记录执行文件位置
            self.offset_map
                .insert(offset_key.clone(), record.list_file_position.clone());

            // 目标object存在则不推送
            if self.attributes.target_exists_skip {
                match c_t
                    .object_exists(self.target.bucket.as_str(), &record.target_key)
                    .await
                {
                    Ok(b) => {
                        if b {
                            continue;
                        }
                    }
                    Err(e) => {
                        record.handle_error(
                            self.stop_mark.clone(),
                            &self.err_counter,
                            self.attributes.max_errors,
                            &self.offset_map,
                            &mut error_file,
                            offset_key.as_str(),
                        );
                        log::error!("{:?}", e);
                        continue;
                    }
                }
            }

            if let Err(e) = match record.option {
                Opt::PUT => {
                    // 判断源文件是否存在
                    let s_path = Path::new(&record.source_key);
                    if !s_path.exists() {
                        continue;
                    }

                    c_t.upload_local_file(
                        self.target.bucket.as_str(),
                        &record.target_key,
                        &record.source_key,
                        self.attributes.large_file_size,
                        self.attributes.multi_part_chunk_size,
                    )
                    .await
                }
                Opt::REMOVE => {
                    match c_t
                        .remove_object(self.target.bucket.as_str(), &record.target_key)
                        .await
                    {
                        Ok(_) => Ok(()),
                        Err(e) => Err(anyhow!("{}", e)),
                    }
                }
                _ => Err(anyhow!("option unkown")),
            } {
                record.handle_error(
                    self.stop_mark.clone(),
                    &self.err_counter,
                    self.attributes.max_errors,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
                log::error!("{:?}", e);
                continue;
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
}
