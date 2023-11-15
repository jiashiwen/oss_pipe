use super::{
    gen_file_path, task_actions::TransferTaskActions, IncrementAssistant, TransferTaskAttributes,
    ERROR_RECORD_PREFIX, OFFSET_PREFIX,
};
use crate::{
    checkpoint::{
        get_task_checkpoint, FileDescription, FilePosition, ListedRecord, Opt, RecordDescription,
    },
    commons::{json_to_struct, read_lines},
    s3::{aws_s3::OssClient, OSSDescription},
    tasks::{MODIFIED_PREFIX, OBJECT_LIST_FILE_PREFIX},
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::error::GetObjectErrorKind;
use dashmap::DashMap;
use regex::RegexSet;
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
use tokio::{sync::Mutex, task::JoinSet};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferOss2Oss {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferOss2Oss {
    fn default() -> Self {
        Self {
            source: OSSDescription::default(),
            target: OSSDescription::default(),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferOss2Oss {
    // 错误记录重试
    fn error_record_retry(&self) -> Result<()> {
        // 遍历错误记录
        for entry in WalkDir::new(self.attributes.meta_dir.as_str())
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
                        let transfer = TransferOss2OssRecordsExecutor {
                            source: self.source.clone(),
                            target: self.target.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            meta_dir: self.attributes.meta_dir.clone(),
                            target_exist_skip: self.attributes.target_exists_skip,
                            large_file_size: self.attributes.large_file_size,
                            multi_part_chunk: self.attributes.multi_part_chunk,
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            list_file_path: p.to_string(),
                        };
                        let _ = transfer.exec_listed_records(record_vec);
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
        let transfer = TransferOss2OssRecordsExecutor {
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
            if let Err(e) = transfer.exec_listed_records(records).await {
                transfer
                    .err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }
    // 生成对象列表
    async fn generate_execute_file(
        &self,
        last_modify_timestamp: Option<i64>,
        object_list_file: &str,
    ) -> Result<FileDescription> {
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
        assistant: Arc<Mutex<IncrementAssistant>>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        snapshot_stop_mark: Arc<AtomicBool>,
    ) {
        // 循环执行获取lastmodify 大于checkpoint指定的时间戳的对象
        let lock = assistant.lock().await;
        let mut timestampe = match lock.last_modify_timestamp {
            Some(t) => t,
            None => {
                return;
            }
        };
        let checkpoint_path = lock.check_point_path.clone();
        let mut checkpoint = match get_task_checkpoint(&lock.check_point_path) {
            Ok(c) => c,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };
        drop(lock);

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

        let mut sleep_time = 5;
        while !snapshot_stop_mark.load(std::sync::atomic::Ordering::SeqCst)
            && self
                .attributes
                .max_errors
                .ge(&err_counter.load(std::sync::atomic::Ordering::SeqCst))
        {
            let (modified_file, new_object_list_file, exec_time) = self
                .gen_modified_record_file(timestampe, &checkpoint.current_stock_object_list_file)
                .await
                .unwrap();
            timestampe = exec_time;

            let mut vec_keys = vec![];
            // 生成执行文件
            let mut list_file_position = FilePosition::default();

            let executed_file = match File::open(&modified_file.path) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{}", e);
                    err_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    continue;
                }
            };

            if sleep_time.ge(&300) {
                sleep_time = 5;
            }
            if executed_file.metadata().unwrap().len().eq(&0) {
                //递增等待时间
                tokio::time::sleep(tokio::time::Duration::from_secs(sleep_time)).await;
                sleep_time += 5;
            } else {
                sleep_time = 5;
            }

            // 按列表传输object from source to target
            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(executed_file).lines();
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

                    match exclude_regex_set {
                        Some(ref exclude) => {
                            if exclude.is_match(&record.source_key) {
                                continue;
                            }
                        }
                        None => {}
                    }
                    match include_regex_set {
                        Some(ref set) => {
                            if set.is_match(&record.source_key) {
                                vec_keys.push(record);
                                continue;
                            }
                        }
                        None => {}
                    }
                    vec_keys.push(record);
                };

                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.bach_size.to_string())
                {
                    while execute_set.len() >= self.attributes.task_threads {
                        execute_set.join_next().await;
                    }
                    let vk = vec_keys.clone();
                    self.record_discriptions_excutor(
                        &mut execute_set,
                        vk,
                        Arc::clone(&err_counter),
                        Arc::clone(&offset_map),
                        modified_file.path.clone(),
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
                self.record_discriptions_excutor(
                    &mut execute_set,
                    vk,
                    Arc::clone(&err_counter),
                    Arc::clone(&offset_map),
                    modified_file.path.clone(),
                )
                .await;
            }

            while execute_set.len() > 0 {
                execute_set.join_next().await;
            }

            let _ = fs::remove_file(&modified_file.path);
            let old_object_file = checkpoint.current_stock_object_list_file.clone();

            checkpoint.executed_file_position = FilePosition {
                offset: modified_file.size.try_into().unwrap(),
                line_num: modified_file.total_lines,
            };
            checkpoint.executed_file = modified_file;
            checkpoint.current_stock_object_list_file = new_object_list_file.path;
            let _ = checkpoint.save_to(&checkpoint_path);
            let _ = fs::remove_file(&old_object_file);
        }
    }
}

impl TransferOss2Oss {
    async fn record_discriptions_excutor(
        &self,
        joinset: &mut JoinSet<()>,
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
            meta_dir: self.attributes.meta_dir.clone(),
            target_exist_skip: false,
            large_file_size: self.attributes.large_file_size,
            multi_part_chunk: self.attributes.multi_part_chunk,
            list_file_path: list_file,
        };

        joinset.spawn(async move {
            if let Err(e) = oss2oss.exec_record_descriptions(records).await {
                oss2oss
                    .err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }

    pub async fn gen_modified_record_file(
        &self,
        timestampe: i64,
        list_file_path: &str,
    ) -> Result<(FileDescription, FileDescription, i64)> {
        // let mut set = JoinSet::new();
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let new_object_list = gen_file_path(
            &self.attributes.meta_dir,
            OBJECT_LIST_FILE_PREFIX,
            now.as_secs().to_string().as_str(),
        );
        let modified = gen_file_path(
            &self.attributes.meta_dir,
            MODIFIED_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let mut new_object_list_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&new_object_list)?;

        let mut modified_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&modified)?;

        let source_client = self.source.gen_oss_client()?;

        // let mut position = FilePosition::default();
        let list_file = File::open(list_file_path)?;
        let mut offset = 0;
        let mut new_list_total_lines = 0;
        let mut modified_total_lines = 0;
        let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(list_file).lines();
        for line in lines {
            if let Result::Ok(key) = line {
                let len = key.bytes().len() + "\n".bytes().len();
                offset += len;
                modified_total_lines += 1;

                let mut target_key = match self.target.prefix.clone() {
                    Some(s) => s,
                    None => "".to_string(),
                };
                target_key.push_str(&key);

                if !source_client
                    .object_exists(&self.source.bucket, &key)
                    .await?
                {
                    // 填充变动对象文件
                    let record = RecordDescription {
                        source_key: key,
                        target_key,
                        list_file_path: list_file_path.to_string(),
                        list_file_position: FilePosition {
                            offset,
                            line_num: modified_total_lines,
                        },
                        option: Opt::REMOVE,
                    };
                    let _ = record.save_json_to_file(&mut modified_file);
                };
            }
        }

        let resp = source_client
            .list_objects(
                self.source.bucket.clone(),
                self.source.prefix.clone(),
                self.attributes.bach_size,
                None,
            )
            .await?;
        let mut token = resp.next_token;
        if let Some(objects) = resp.object_list {
            objects.iter().for_each(|o| match o.key() {
                Some(key) => {
                    let _ = new_object_list_file.write_all(key.as_bytes());
                    let _ = new_object_list_file.write_all("\n".as_bytes());
                    new_list_total_lines += 1;
                    if let Some(d) = o.last_modified() {
                        if d.secs().ge(&timestampe) {
                            // 填充变动对象文件
                            let mut target_key = match self.target.prefix.clone() {
                                Some(s) => s,
                                None => "".to_string(),
                            };
                            target_key.push_str(&key);
                            let record = RecordDescription {
                                source_key: key.to_string(),
                                target_key,
                                list_file_path: "".to_string(),
                                list_file_position: FilePosition::default(),
                                option: Opt::PUT,
                            };
                            let _ = record.save_json_to_file(&mut modified_file);
                            modified_total_lines += 1;
                        }
                    }
                }
                None => {}
            });

            modified_file.flush()?;
            new_object_list_file.flush()?;
        }

        while token.is_some() {
            let resp = source_client
                .list_objects(
                    self.source.bucket.clone(),
                    self.source.prefix.clone(),
                    self.attributes.bach_size,
                    token.clone(),
                )
                .await?;
            if let Some(objects) = resp.object_list {
                objects.iter().for_each(|o| match o.key() {
                    Some(key) => {
                        // 填充新的对象列表文件
                        let _ = new_object_list_file.write_all(key.as_bytes());
                        let _ = new_object_list_file.write_all("\n".as_bytes());
                        new_list_total_lines += 1;
                        if let Some(d) = o.last_modified() {
                            if d.secs().ge(&timestampe) {
                                // 填充变动对象文件
                                let mut target_key = match self.target.prefix.clone() {
                                    Some(s) => s,
                                    None => "".to_string(),
                                };
                                target_key.push_str(&key);

                                let record = RecordDescription {
                                    source_key: key.to_string(),
                                    target_key,
                                    list_file_path: "".to_string(),
                                    list_file_position: FilePosition::default(),
                                    option: Opt::PUT,
                                };
                                let _ = record.save_json_to_file(&mut modified_file);
                                modified_total_lines += 1;
                            }
                        }
                    }
                    None => {}
                });

                modified_file.flush()?;
                new_object_list_file.flush()?;
            }
            token = resp.next_token;
        }

        let new_object_list_file_size = new_object_list_file.metadata()?.len();
        let modified_file_size = modified_file.metadata()?.len();
        let modified_executed_file = FileDescription {
            path: modified.clone(),
            size: modified_file_size,
            total_lines: modified_total_lines,
        };

        let new_object_list_executed_file = FileDescription {
            path: new_object_list.clone(),
            size: new_object_list_file_size,
            total_lines: new_list_total_lines,
        };
        println!("{}", new_object_list_file_size);
        let timestampe = now.as_secs().try_into()?;
        Ok((
            modified_executed_file,
            new_object_list_executed_file,
            timestampe,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct TransferOss2OssRecordsExecutor {
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

impl TransferOss2OssRecordsExecutor {
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

        let c_s = self.source.gen_oss_client()?;
        let c_t = self.target.gen_oss_client()?;
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

            if let Err(e) = self
                .listed_record_handler(&record, &c_s, &c_t, &target_key)
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

    async fn listed_record_handler(
        &self,
        record: &ListedRecord,
        source_oss: &OssClient,
        target_oss: &OssClient,
        target_key: &str,
    ) -> Result<()> {
        let obj_out_put = match source_oss
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
            .transfer_object(
                self.target.bucket.as_str(),
                target_key,
                self.large_file_size,
                self.multi_part_chunk,
                obj_out_put,
            )
            .await
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

        let s_client = self.source.gen_oss_client()?;
        let t_client = self.target.gen_oss_client()?;

        for record in records {
            // 记录执行文件位置
            self.offset_map
                .insert(offset_key.clone(), record.list_file_position.clone());

            if let Err(e) = self
                .record_description_handler(&s_client, &t_client, &record)
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
        source_oss: &OssClient,
        target_oss: &OssClient,
        record: &RecordDescription,
    ) -> Result<()> {
        // 目标object存在则不推送
        if self.target_exist_skip {
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
                let obj = match source_oss
                    .get_object(&self.source.bucket, &record.source_key)
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        let service_err = e.into_service_error();
                        match service_err.kind {
                            GetObjectErrorKind::NoSuchKey(_) => {
                                return Ok(());
                            }
                            _ => {
                                log::error!("{}", service_err);
                                return Err(service_err.into());
                            }
                        }
                    }
                };
                target_oss
                    .transfer_object(
                        &self.target.bucket,
                        &record.target_key,
                        self.large_file_size,
                        self.multi_part_chunk,
                        obj,
                    )
                    .await?;
            }
            Opt::REMOVE => {
                target_oss
                    .remove_object(&self.target.bucket, &record.target_key)
                    .await?;
            }
            Opt::UNKOWN => return Err(anyhow!("option unkown")),
        }
        Ok(())
    }
}
