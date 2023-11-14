use super::{
    gen_file_path, task_actions::TransferTaskActions, IncrementAssistant, TransferTaskAttributes,
    ERROR_RECORD_PREFIX, MODIFIED_PREFIX, OBJECT_LIST_FILE_PREFIX, OFFSET_PREFIX,
};
use crate::{
    checkpoint::{
        get_task_checkpoint, ExecutedFile, FilePosition, ListedRecord, Opt, RecordDescription,
    },
    commons::{json_to_struct, read_lines},
    s3::{
        aws_s3::{download_object_to_file, OssClient},
        OSSDescription,
    },
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
    path::Path,
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
                        let _ = download.exec_listed_records(record_vec);
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
            if let Err(e) = download.exec_listed_records(records).await {
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
        start_file_position: FilePosition,
    ) {
        // Todo
        // 遍历objectlist 找出删除文件
        // 通过lastmodify 时间戳找出新增及变更文件

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

            if executed_file.metadata().unwrap().len().eq(&0) {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
            // 按列表传输object from source to target
            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(executed_file).lines();
            for line in lines {
                // 若错误达到上限，则停止任务
                if err_counter.load(std::sync::atomic::Ordering::SeqCst)
                    >= self.attributes.max_errors
                {
                    break;
                }
                if let Result::Ok(line_str) = line {
                    let len = line_str.bytes().len() + "\n".bytes().len();
                    list_file_position.offset += len;
                    list_file_position.line_num += 1;

                    let record = match from_str::<RecordDescription>(&line_str) {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("{}", e);
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
            // let _ = fs::remove_file(&checkpoint.current_stock_object_list_file);
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

impl TransferOss2Local {
    async fn record_discriptions_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        records: Vec<RecordDescription>,
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
            if let Err(e) = download.exec_record_descriptions(records).await {
                download
                    .err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }
    // 从object list 文件 查找已删除的文件并记录
    pub async fn removed_oss_objects_from_list(
        &self,
        list_file_path: &str,
        out_put_file_name: &str,
    ) -> Result<()> {
        let path = std::path::Path::new(out_put_file_name);
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        };
        //写入文件
        let mut out_put_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(out_put_file_name)?;

        let oss_client = self.source.gen_oss_client()?;
        let list_file = File::open(list_file_path)?;
        let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(list_file).lines();
        let mut position = FilePosition::default();

        for line in lines {
            if let Result::Ok(key) = line {
                let len = key.bytes().len() + "\n".bytes().len();
                position.offset += len;
                position.line_num += 1;
                let target_key = gen_file_path(self.target.as_str(), &key, "");
                if !oss_client.object_exists(&self.source.bucket, &key).await? {
                    let record = RecordDescription {
                        source_key: key,
                        target_key,
                        list_file_path: list_file_path.to_string(),
                        list_file_position: position.clone(),
                        option: Opt::REMOVE,
                    };
                    let _ = record.save_json_to_file(&mut out_put_file);
                };
            }
        }
        Ok(())
    }

    // Todo 多线程改造
    // 遍历源找出last modify 大于指定时间戳的对象并记录，同时生成新的object list，下次以新的objec list 文件为基准计算删除和新增文件
    pub async fn gen_modified_record_file(
        &self,
        timestampe: i64,
        list_file_path: &str,
    ) -> Result<(ExecutedFile, ExecutedFile, i64)> {
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

        let oss_client = self.source.gen_oss_client()?;

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
                let target_key = gen_file_path(self.target.as_str(), &key, "");
                if !oss_client.object_exists(&self.source.bucket, &key).await? {
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

        let resp = oss_client
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
                Some(k) => {
                    let _ = new_object_list_file.write_all(k.as_bytes());
                    let _ = new_object_list_file.write_all("\n".as_bytes());
                    new_list_total_lines += 1;
                    if let Some(d) = o.last_modified() {
                        if d.secs().ge(&timestampe) {
                            // 填充变动对象文件
                            let target_key = gen_file_path(self.target.as_str(), k, "");
                            let record = RecordDescription {
                                source_key: k.to_string(),
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
        }

        while token.is_some() {
            let resp = oss_client
                .list_objects(
                    self.source.bucket.clone(),
                    self.source.prefix.clone(),
                    self.attributes.bach_size,
                    token.clone(),
                )
                .await?;
            if let Some(objects) = resp.object_list {
                objects.iter().for_each(|o| match o.key() {
                    Some(k) => {
                        // 填充新的对象列表文件
                        let _ = new_object_list_file.write_all(k.as_bytes());
                        let _ = new_object_list_file.write_all("\n".as_bytes());
                        new_list_total_lines += 1;
                        if let Some(d) = o.last_modified() {
                            if d.secs().ge(&timestampe) {
                                // 填充变动对象文件
                                let target_key = gen_file_path(self.target.as_str(), k, "");
                                let record = RecordDescription {
                                    source_key: k.to_string(),
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
        let modified_executed_file = ExecutedFile {
            path: modified.clone(),
            size: modified_file_size,
            total_lines: modified_total_lines,
        };

        let new_object_list_executed_file = ExecutedFile {
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
                .listed_record_handler(&record, &c_s, t_file_name.as_str())
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

    async fn listed_record_handler(
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

        download_object_to_file(
            resp,
            &mut t_file,
            self.large_file_size,
            self.multi_part_chunk,
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

        let source_client = self.source.gen_oss_client()?;

        // Todo
        // 增加去重逻辑，当两条记录相邻为 create和modif时只put一次
        // 增加目录删除逻辑，对应oss删除指定prefix下的所有文件，文件系统删除目录
        for record in records {
            // 记录执行文件位置
            self.offset_map
                .insert(offset_key.clone(), record.list_file_position.clone());

            let t_path = Path::new(&record.target_key);
            if let Some(p) = t_path.parent() {
                std::fs::create_dir_all(p)?
            };

            // 目标object存在则不推送
            if self.target_exist_skip {
                if t_path.exists() {
                    continue;
                }
            }

            if let Err(e) = self
                .record_description_handler(&source_client, &record)
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
        source_oss_client: &OssClient,
        record: &RecordDescription,
    ) -> Result<()> {
        match record.option {
            Opt::PUT => {
                let obj = match source_oss_client
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
                let mut t_file = OpenOptions::new()
                    .truncate(true)
                    .create(true)
                    .write(true)
                    .open(&record.target_key)?;
                download_object_to_file(
                    obj,
                    &mut t_file,
                    self.large_file_size,
                    self.multi_part_chunk,
                )
                .await?
            }
            Opt::REMOVE => fs::remove_file(record.target_key.as_str())?,
            Opt::UNKOWN => return Err(anyhow!("option unkown")),
        }
        Ok(())
    }
}
