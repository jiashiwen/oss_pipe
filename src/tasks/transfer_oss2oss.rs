use super::{
    gen_file_path, task_actions::TransferTaskActions, IncrementAssistant, TransferStage,
    TransferTaskAttributes, MODIFIED_PREFIX, OFFSET_PREFIX, REMOVED_PREFIX,
    TRANSFER_ERROR_RECORD_PREFIX,
};
use crate::{
    checkpoint::{
        get_task_checkpoint, FileDescription, FilePosition, ListedRecord, Opt, RecordDescription,
    },
    commons::{
        json_to_struct, merge_file, promote_processbar, read_lines, struct_to_json_string,
        LastModifyFilter, RegexFilter,
    },
    s3::{
        oss_client::{multipart_transfer_obj_paralle_by_range, OssClient},
        OSSDescription,
    },
    tasks::{LogInfo, TaskDefaultParameters},
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::Object;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, Write},
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinSet,
};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferOss2Oss {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferOss2Oss {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: OSSDescription::default(),
            target: OSSDescription::default(),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferOss2Oss {
    async fn analyze_source(&self) -> Result<DashMap<String, i128>> {
        let regex_filter =
            RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)?;
        let client = self.source.gen_oss_client()?;
        client
            .analyze_objects_size(
                &self.source.bucket,
                self.source.prefix.clone(),
                Some(regex_filter),
                self.attributes.last_modify_filter.clone(),
                self.attributes.objects_per_batch,
            )
            .await
    }
    // 错误记录重试
    fn error_record_retry(&self, executing_transfers: Arc<RwLock<usize>>) -> Result<()> {
        // 遍历错误记录
        for entry in WalkDir::new(self.attributes.meta_dir.as_str())
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir() && e.file_name().to_str().is_some())
        {
            let file_name = entry.file_name().to_str().unwrap();

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
                        let transfer = TransferOss2OssRecordsExecutor {
                            source: self.source.clone(),
                            target: self.target.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            attributes: self.attributes.clone(),
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            list_file_path: p.to_string(),
                        };
                        let _ = transfer
                            .exec_record_descriptions(executing_transfers.clone(), record_vec);
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
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    ) {
        let transfer = TransferOss2OssRecordsExecutor {
            source: self.source.clone(),
            target: self.target.clone(),
            err_counter,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path: list_file,
        };

        execute_set.spawn(async move {
            if let Err(e) = transfer
                .exec_listed_records(records, executing_transfers)
                .await
            {
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
        let transfer = TransferOss2OssRecordsExecutor {
            source: self.source.clone(),
            target: self.target.clone(),
            err_counter,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path: list_file,
        };

        joinset.spawn(async move {
            if let Err(e) = transfer
                .exec_record_descriptions(executing_transfers, records)
                .await
            {
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }
    // 生成对象列表
    async fn gen_source_object_list_file(
        &self,
        last_modify_filter: Option<LastModifyFilter>,
        object_list_file: &str,
    ) -> Result<FileDescription> {
        let client_source = self.source.gen_oss_client()?;

        // 若为持续同步模式，且 last_modify_timestamp 大于 0，则将 last_modify 属性大于last_modify_timestamp变量的对象加入执行列表
        client_source
            .append_object_list_to_file(
                self.source.bucket.clone(),
                self.source.prefix.clone(),
                self.attributes.objects_per_batch,
                object_list_file,
                last_modify_filter,
            )
            .await
    }

    async fn changed_object_capture_based_target(
        &self,
        timestamp: i128,
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

        let source_client = self.source.gen_oss_client()?;
        let target_client = self.target.gen_oss_client()?;

        // 筛选源对象，lastmodify大于等于时间戳并转换为RecordDescription格式
        let mut process_source_objects = |source_objects: Vec<Object>| -> Result<()> {
            for obj in source_objects {
                if let Some(source_key) = obj.key() {
                    if let Some(d) = obj.last_modified() {
                        if last_modify_filter.filter(i128::from(d.secs())) {
                            let mut target_key = "".to_string();
                            if let Some(p) = &self.target.prefix {
                                target_key.push_str(p);
                            }
                            target_key.push_str(source_key);

                            let record = RecordDescription {
                                source_key: source_key.to_string(),
                                target_key,
                                list_file_path: "".to_string(),
                                list_file_position: FilePosition::default(),
                                option: Opt::PUT,
                            };

                            let record_str = struct_to_json_string(&record)?;
                            let _ = modified_file.write_all(record_str.as_bytes());
                            let _ = modified_file.write_all("\n".as_bytes());
                            modified_lines += 1;
                        }
                    }
                }
            }
            Ok(())
        };

        // 获取目标所有 object 与源对比得到已删除的 object 并写入文件
        let target_resp = target_client
            .list_objects(
                &self.target.bucket,
                self.target.prefix.clone(),
                self.attributes.objects_per_batch,
                None,
            )
            .await?;

        let mut target_token = target_resp.next_token;

        if let Some(objects) = target_resp.object_list {
            for obj in objects {
                if let Some(target_key) = obj.key() {
                    let mut source_key = "".to_string();
                    if let Some(p) = &self.target.prefix {
                        let key = match p.ends_with("/") {
                            true => &target_key[p.len()..],
                            false => &target_key[p.len() + 1..],
                        };
                        source_key.push_str(key);
                    } else {
                        source_key.push_str(target_key);
                    };

                    if !source_client
                        .object_exists(&self.source.bucket, &source_key)
                        .await?
                    {
                        let record = RecordDescription {
                            source_key,
                            target_key: target_key.to_string(),
                            list_file_path: "".to_string(),
                            list_file_position: FilePosition::default(),
                            option: Opt::REMOVE,
                        };

                        let record_str = struct_to_json_string(&record)?;
                        let _ = removed_file.write_all(record_str.as_bytes());
                        let _ = removed_file.write_all("\n".as_bytes());
                        removed_lines += 1;
                    }
                }
            }
        }

        while target_token.is_some() {
            let resp = target_client
                .list_objects(
                    &self.target.bucket,
                    self.target.prefix.clone(),
                    self.attributes.objects_per_batch,
                    target_token,
                )
                .await?;
            if let Some(objects) = resp.object_list {
                for obj in objects {
                    if let Some(target_key) = obj.key() {
                        let mut source_key = "".to_string();
                        if let Some(p) = &self.target.prefix {
                            let key = match p.ends_with("/") {
                                true => &target_key[p.len()..],
                                false => &target_key[p.len() + 1..],
                            };
                            source_key.push_str(key);
                        } else {
                            source_key.push_str(target_key);
                        };

                        if !source_client
                            .object_exists(&self.source.bucket, &source_key)
                            .await?
                        {
                            let record = RecordDescription {
                                source_key,
                                target_key: target_key.to_string(),
                                list_file_path: "".to_string(),
                                list_file_position: FilePosition::default(),
                                option: Opt::REMOVE,
                            };

                            let record_str = struct_to_json_string(&record)?;
                            let _ = removed_file.write_all(record_str.as_bytes());
                            let _ = removed_file.write_all("\n".as_bytes());
                            removed_lines += 1;
                        }
                    }
                }
            }
            target_token = resp.next_token;
        }

        // 获取源所有增量 object 并写入文件
        let source_resp = source_client
            .list_objects(
                &self.source.bucket,
                self.source.prefix.clone(),
                self.attributes.objects_per_batch,
                None,
            )
            .await
            .context(format!("{}:{}", file!(), line!()))?;
        let mut source_token = source_resp.next_token;
        if let Some(objects) = source_resp.object_list {
            process_source_objects(objects)?;
        }

        while source_token.is_some() {
            let resp = source_client
                .list_objects(
                    &self.source.bucket,
                    self.source.prefix.clone(),
                    self.attributes.objects_per_batch,
                    source_token,
                )
                .await?;
            if let Some(objects) = resp.object_list {
                process_source_objects(objects)?;
            }
            source_token = resp.next_token;
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
            path: modified,
            size: total_size,
            total_lines,
        };
        let log_info = LogInfo {
            task_id: self.task_id.clone(),
            msg: "capture changed object".to_string(),
            additional: Some(file_desc.clone()),
        };
        log::info!("{:?} ", log_info);
        Ok(file_desc)
    }

    async fn increment_prelude(&self, assistant: Arc<Mutex<IncrementAssistant>>) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let timestampe = TryInto::<i64>::try_into(now.as_secs())?;
        let mut lock = assistant.lock().await;
        lock.last_modify_timestamp = Some(timestampe);
        drop(lock);
        Ok(())
    }

    async fn execute_increment(
        &self,
        mut execute_set: &mut JoinSet<()>,
        executing_transfers: Arc<RwLock<usize>>,
        assistant: Arc<Mutex<IncrementAssistant>>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        snapshot_stop_mark: Arc<AtomicBool>,
    ) {
        // 循环执行获取lastmodify 大于checkpoint指定的时间戳的对象
        let lock = assistant.lock().await;

        let checkpoint_path = lock.check_point_path.clone();
        let mut checkpoint = match get_task_checkpoint(&lock.check_point_path) {
            Ok(c) => c,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };
        checkpoint.task_stage = TransferStage::Increment;
        drop(lock);

        let regex_filter =
            match RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

        let mut sleep_time = 5;
        let pd = promote_processbar("executing increment:waiting for data...");
        let mut finished_total_objects = 0;

        while !snapshot_stop_mark.load(std::sync::atomic::Ordering::SeqCst)
            && self
                .attributes
                .max_errors
                .ge(&err_counter.load(std::sync::atomic::Ordering::SeqCst))
        {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let modified = match self
                .changed_object_capture_based_target(checkpoint.task_begin_timestamp)
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

            let mut vec_keys = vec![];
            // 生成执行文件
            let mut list_file_position = FilePosition::default();
            let modified_file = match File::open(&modified.path) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{}", e);
                    err_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    continue;
                }
            };

            let modified_file_is_empty = modified_file.metadata().unwrap().len().eq(&0);

            // 按列表传输object from source to target
            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(modified_file).lines();
            for line in lines {
                // 若错误达到上限，则停止任务
                if err_counter.load(std::sync::atomic::Ordering::SeqCst)
                    >= self.attributes.max_errors
                {
                    return;
                }
                if let Result::Ok(line_str) = line {
                    let len = line_str.bytes().len() + "\n".bytes().len();
                    list_file_position.offset += len;
                    list_file_position.line_num += 1;

                    let record = match from_str::<RecordDescription>(&line_str) {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("{}", e);
                            err_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            continue;
                        }
                    };

                    if regex_filter.filter(&record.source_key) {
                        vec_keys.push(record);
                    }
                };

                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.objects_per_batch.to_string())
                {
                    while execute_set.len() >= self.attributes.task_parallelism {
                        execute_set.join_next().await;
                    }
                    let vk = vec_keys.clone();
                    self.record_discriptions_excutor(
                        &mut execute_set,
                        executing_transfers.clone(),
                        vk,
                        Arc::clone(&err_counter),
                        Arc::clone(&offset_map),
                        modified.path.clone(),
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
                while execute_set.len() >= self.attributes.task_parallelism {
                    execute_set.join_next().await;
                }

                let vk = vec_keys.clone();
                self.record_discriptions_excutor(
                    &mut execute_set,
                    executing_transfers.clone(),
                    vk,
                    Arc::clone(&err_counter),
                    Arc::clone(&offset_map),
                    modified.path.clone(),
                )
                .await;
            }

            while execute_set.len() > 0 {
                execute_set.join_next().await;
            }

            finished_total_objects += modified.total_lines;
            if !modified.total_lines.eq(&0) {
                let msg: String = format!(
                    "executing transfer modified finished this batch {} total {};",
                    modified.total_lines, finished_total_objects
                );
                let log_info = LogInfo::<String> {
                    task_id: self.task_id.clone(),
                    msg,
                    additional: None,
                };

                log::info!("{:?}", log_info);
            }

            let _ = fs::remove_file(&modified.path);
            checkpoint.executed_file_position = FilePosition {
                offset: modified.size.try_into().unwrap(),
                line_num: modified.total_lines,
            };
            checkpoint.executed_file = modified.clone();
            checkpoint.task_begin_timestamp = i128::from(now.as_secs());

            let _ = checkpoint.save_to(&checkpoint_path);

            //递增等待时间
            if modified_file_is_empty {
                if sleep_time.ge(&300) {
                    sleep_time = 60;
                } else {
                    sleep_time += 5;
                }
            } else {
                sleep_time = 5;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(sleep_time)).await;
        }
        pd.finish();
    }
}

impl TransferOss2Oss {
    async fn record_discriptions_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        executing_transfers: Arc<RwLock<usize>>,
        records: Vec<RecordDescription>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    ) {
        let oss2oss = TransferOss2OssRecordsExecutor {
            target: self.target.clone(),
            source: self.source.clone(),
            err_counter,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path: list_file,
        };

        joinset.spawn(async move {
            if let Err(e) = oss2oss
                .exec_record_descriptions(executing_transfers, records)
                .await
            {
                oss2oss
                    .err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }
}

#[derive(Debug, Clone)]
pub struct TransferOss2OssRecordsExecutor {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: TransferTaskAttributes,
    pub list_file_path: String,
}

impl TransferOss2OssRecordsExecutor {
    pub async fn exec_listed_records(
        &self,
        records: Vec<ListedRecord>,
        executing_transfers: Arc<RwLock<usize>>,
    ) -> Result<()> {
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

        let source_client = self.source.gen_oss_client()?;
        let target_client = self.target.gen_oss_client()?;
        let s_c = Arc::new(source_client);
        let t_c = Arc::new(target_client);
        for record in records {
            // 插入文件offset记录
            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );

            let mut target_key = match self.target.prefix.clone() {
                Some(s) => s,
                None => "".to_string(),
            };
            target_key.push_str(&record.key);
            let e_u = Arc::clone(&executing_transfers);

            if let Err(e) = self
                .listed_record_handler(e_u, &record, &s_c, &t_c, &target_key)
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
                recorddesc.handle_error(
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

    //Todo
    // 尝试 source_oss、target_oss 参数 使用Arc<Client>
    async fn listed_record_handler(
        &self,
        executing_transfers: Arc<RwLock<usize>>,
        record: &ListedRecord,
        // source_oss: &OssClient,
        // target_oss: &OssClient,
        source_oss: &Arc<OssClient>,
        target_oss: &Arc<OssClient>,
        target_key: &str,
    ) -> Result<()> {
        let s_obj_output = match source_oss
            .get_object(&self.source.bucket.as_str(), record.key.as_str())
            .await
        {
            core::result::Result::Ok(resp) => resp,
            Err(e) => {
                // 源端文件不存在按传输成功处理
                let service_err = e.into_service_error();
                match service_err.is_no_such_key() {
                    true => {
                        return Ok(());
                    }
                    false => {
                        return Err(service_err.into());
                    }
                }
            }
        };

        // 目标object存在则不推送
        if self.attributes.target_exists_skip {
            let target_obj_exists = target_oss
                .object_exists(self.target.bucket.as_str(), target_key)
                .await?;
            if target_obj_exists {
                return Ok(());
            }
        }

        let content_len = match s_obj_output.content_length() {
            Some(l) => l,
            None => return Err(anyhow!("content length is None")),
        };
        let content_len_usize: usize = content_len.try_into()?;
        let expr = match s_obj_output.expires() {
            Some(d) => Some(*d),
            None => None,
        };
        return match content_len_usize.le(&self.attributes.large_file_size) {
            true => {
                target_oss
                    .upload_object_bytes(
                        self.target.bucket.as_str(),
                        target_key,
                        expr,
                        s_obj_output.body,
                    )
                    .await
            }
            false => {
                // let s_c = Arc::new(source_oss.client.clone());
                // let s_c = source_oss.clone();
                let e_t = Arc::clone(&executing_transfers);
                // target_oss
                //     .multipart_upload_obj_paralle_by_range(
                //         s_c,
                //         &self.source.bucket,
                //         record.key.as_str(),
                //         &self.target.bucket,
                //         target_key,
                //         expr,
                //         e_t,
                //         self.attributes.multi_part_chunk_size,
                //         self.attributes.multi_part_chunks_per_batch,
                //         self.attributes.multi_part_parallelism,
                //     )
                //     .await

                multipart_transfer_obj_paralle_by_range(
                    source_oss.clone(),
                    &self.source.bucket,
                    record.key.as_str(),
                    target_oss.clone(),
                    &self.target.bucket,
                    target_key,
                    expr,
                    e_t,
                    self.attributes.multi_part_chunk_size,
                    self.attributes.multi_part_chunks_per_batch,
                    self.attributes.multi_part_parallelism,
                )
                .await
            }
        };
    }

    pub async fn exec_record_descriptions(
        &self,
        executing_transfers: Arc<RwLock<usize>>,
        records: Vec<RecordDescription>,
    ) -> Result<()> {
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

        let s_client = self.source.gen_oss_client()?;
        let t_client = self.target.gen_oss_client()?;
        let s_c = Arc::new(s_client);
        let t_c = Arc::new(t_client);

        for record in records {
            // 记录执行文件位置
            self.offset_map
                .insert(offset_key.clone(), record.list_file_position.clone());

            if let Err(e) = self
                .record_description_handler(executing_transfers.clone(), &s_c, &t_c, &record)
                .await
            {
                log::error!("{}", e);
                record.handle_error(
                    &self.err_counter,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
            };
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
        executing_transfers: Arc<RwLock<usize>>,
        // source_oss: &OssClient,
        // target_oss: &OssClient,
        source_oss: &Arc<OssClient>,
        target_oss: &Arc<OssClient>,
        // source_oss: &OssClient,
        // target_oss: &OssClient,
        record: &RecordDescription,
    ) -> Result<()> {
        // 目标object存在则不推送
        if self.attributes.target_exists_skip {
            match target_oss
                .object_exists(self.target.bucket.as_str(), &record.target_key)
                .await
            {
                Ok(b) => {
                    if b {
                        return Ok(());
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        match record.option {
            Opt::PUT => {
                let s_obj = match source_oss
                    .get_object(&self.source.bucket, &record.source_key)
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        let service_err = e.into_service_error();
                        match service_err.is_no_such_key() {
                            true => {
                                return Ok(());
                            }
                            false => {
                                log::error!("{}", service_err);
                                return Err(service_err.into());
                            }
                        }
                    }
                };

                let content_len = match s_obj.content_length() {
                    Some(l) => l,
                    None => return Err(anyhow!("content length is None")),
                };
                let content_len_usize: usize = content_len.try_into()?;
                let expr = match s_obj.expires() {
                    Some(d) => Some(*d),
                    None => None,
                };
                // target_oss
                //     .transfer_object(
                //         &self.target.bucket,
                //         &record.target_key,
                //         self.attributes.large_file_size,
                //         self.attributes.multi_part_chunk_size,
                //         s_obj,
                //     )
                //     .await?;
                return match content_len_usize.le(&self.attributes.large_file_size) {
                    true => {
                        target_oss
                            .upload_object_bytes(
                                self.target.bucket.as_str(),
                                &record.target_key,
                                expr,
                                s_obj.body,
                            )
                            .await
                    }
                    false => {
                        let e_t = Arc::clone(&executing_transfers);

                        multipart_transfer_obj_paralle_by_range(
                            source_oss.clone(),
                            &self.source.bucket,
                            &record.source_key,
                            target_oss.clone(),
                            &self.target.bucket,
                            &record.target_key,
                            expr,
                            e_t,
                            self.attributes.multi_part_chunk_size,
                            self.attributes.multi_part_chunks_per_batch,
                            self.attributes.multi_part_parallelism,
                        )
                        .await
                    }
                };
            }
            Opt::REMOVE => {
                target_oss
                    .remove_object(&self.target.bucket, &record.target_key)
                    .await?;
            }
            _ => return Err(anyhow!("option unkown")),
        }
        Ok(())
    }
}
