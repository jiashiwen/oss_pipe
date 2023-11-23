use super::{
    gen_file_path, task_actions::TransferTaskActions, IncrementAssistant, TransferStage,
    TransferTaskAttributes, ERROR_RECORD_PREFIX, OFFSET_PREFIX,
};
use crate::{
    checkpoint::{
        get_task_checkpoint, FileDescription, FilePosition, ListedRecord, Opt, RecordDescription,
    },
    commons::{json_to_struct, promote_processbar, read_lines, LastModifyFilter, RegexFilter},
    s3::{aws_s3::OssClient, OSSDescription},
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::error::GetObjectErrorKind;
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
                self.attributes.bach_size,
            )
            .await
    }
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
                            meta_dir: self.attributes.meta_dir.clone(),
                            target_exist_skip: self.attributes.target_exists_skip,
                            large_file_size: self.attributes.large_file_size,
                            multi_part_chunk: self.attributes.multi_part_chunk,
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            list_file_path: p.to_string(),
                        };
                        let _ = transfer.exec_record_descriptions(record_vec);
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
            meta_dir: self.attributes.meta_dir.clone(),
            target_exist_skip: false,
            large_file_size: self.attributes.large_file_size,
            multi_part_chunk: self.attributes.multi_part_chunk,
            list_file_path: list_file,
        };

        joinset.spawn(async move {
            if let Err(e) = transfer.exec_listed_records(records).await {
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }
    // 生成对象列表
    async fn generate_execute_file(
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
                self.attributes.bach_size,
                object_list_file,
                last_modify_filter,
            )
            .await
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
        checkpoint.task_stage = TransferStage::Increment;
        drop(lock);

        let regex_filter =
            match RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{}", e);
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
            let source_client = match self.source.gen_oss_client() {
                Ok(client) => client,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };
            let (modified_desc, new_object_list_desc, exec_time) = match source_client
                .changed_object_capture(
                    &self.source.bucket,
                    self.source.prefix.clone(),
                    self.target.prefix.clone(),
                    &self.attributes.meta_dir,
                    timestampe,
                    &checkpoint.current_stock_object_list_file,
                    self.attributes.bach_size,
                    self.attributes.multi_part_chunk,
                )
                .await
            {
                Ok((m, n, t)) => (m.clone(), n.clone(), t.clone()),
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            timestampe = exec_time.clone();

            let mut vec_keys = vec![];
            // 生成执行文件
            let mut list_file_position = FilePosition::default();
            let modified_file = match File::open(&modified_desc.path) {
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
                        modified_desc.path.clone(),
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
                    modified_desc.path.clone(),
                )
                .await;
            }

            while execute_set.len() > 0 {
                execute_set.join_next().await;
            }

            finished_total_objects += modified_desc.total_lines;
            if !modified_desc.total_lines.eq(&0) {
                let msg = format!(
                    "executing transfer modified finished this batch {} total {};",
                    modified_desc.total_lines, finished_total_objects
                );
                pd.set_message(msg);
            }

            let _ = fs::remove_file(&checkpoint.current_stock_object_list_file);
            let _ = fs::remove_file(&modified_desc.path);
            checkpoint.executed_file_position = FilePosition {
                offset: modified_desc.size.try_into().unwrap(),
                line_num: modified_desc.total_lines,
            };
            checkpoint.executed_file = modified_desc.clone();
            checkpoint.current_stock_object_list_file = new_object_list_desc.path.clone();
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

    // pub async fn gen_changed_record_file(
    //     &self,
    //     timestampe: i64,
    //     list_file_path: &str,
    // ) -> Result<(FileDescription, FileDescription, i64)> {
    //     // let mut set = JoinSet::new();
    //     let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

    //     let new_object_list = gen_file_path(
    //         &self.attributes.meta_dir,
    //         OBJECT_LIST_FILE_PREFIX,
    //         now.as_secs().to_string().as_str(),
    //     );

    //     let removed = gen_file_path(
    //         &self.attributes.meta_dir,
    //         REMOVED_PREFIX,
    //         now.as_secs().to_string().as_str(),
    //     );
    //     let modified = gen_file_path(
    //         &self.attributes.meta_dir,
    //         MODIFIED_PREFIX,
    //         now.as_secs().to_string().as_str(),
    //     );

    //     let source_client = self.source.gen_oss_client()?;

    //     let removed_description = self
    //         .gen_removed_record_file(&source_client, list_file_path, &removed)
    //         .await?;
    //     let (mut modified_description, new_list_description) = self
    //         .gen_modified_record_file(&source_client, timestampe, &modified, &new_object_list)
    //         .await?;
    //     if removed_description.size.gt(&0) {
    //         merge_file(
    //             &removed_description.path,
    //             &modified_description.path,
    //             self.attributes.multi_part_chunk,
    //         )?;
    //     }

    //     let timestampe = now.as_secs().try_into()?;
    //     modified_description.size = modified_description.size + removed_description.size;
    //     modified_description.total_lines =
    //         modified_description.total_lines + removed_description.total_lines;

    //     let _ = fs::remove_file(&removed_description.path);
    //     Ok((modified_description, new_list_description, timestampe))
    // }

    // async fn gen_removed_record_file(
    //     &self,
    //     source_client: &OssClient,
    //     list_file: &str,
    //     out_put: &str,
    // ) -> Result<FileDescription> {
    //     let obj_list_file = File::open(list_file)?;
    //     let mut list_file_position = FilePosition::default();
    //     let mut out_put_file_total_lines = 0;

    //     let out_put_file = OpenOptions::new()
    //         .create(true)
    //         .truncate(true)
    //         .write(true)
    //         .open(&out_put)?;
    //     let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(obj_list_file).lines();
    //     for line in lines {
    //         if let Result::Ok(key) = line {
    //             let len = key.bytes().len() + "\n".bytes().len();
    //             list_file_position.offset += len;
    //             list_file_position.line_num += 1;

    //             let mut target_key = match self.target.prefix.clone() {
    //                 Some(s) => s,
    //                 None => "".to_string(),
    //             };
    //             target_key.push_str(&key);

    //             if !source_client
    //                 .object_exists(&self.source.bucket, &key)
    //                 .await?
    //             {
    //                 // 填充变动对象文件
    //                 let record = RecordDescription {
    //                     source_key: key,
    //                     target_key,
    //                     list_file_path: list_file.to_string(),
    //                     list_file_position,
    //                     option: Opt::REMOVE,
    //                 };
    //                 let _ = record.save_json_to_file(&out_put_file);
    //                 out_put_file_total_lines += 1;
    //             };
    //         }
    //     }
    //     let size = out_put_file.metadata()?.len();
    //     let out_put_description = FileDescription {
    //         path: out_put.to_string(),
    //         size,
    //         total_lines: out_put_file_total_lines,
    //     };

    //     Ok(out_put_description)
    // }

    // // 通过时间戳生成变更文件的同时生成当前的文件列表
    // async fn gen_modified_record_file(
    //     &self,
    //     source_client: &OssClient,
    //     timestampe: i64,
    //     modified_out_put: &str,
    //     new_list: &str,
    // ) -> Result<(FileDescription, FileDescription)> {
    //     let mut modified_out_put_file = OpenOptions::new()
    //         .create(true)
    //         .truncate(true)
    //         .write(true)
    //         .open(&modified_out_put)?;
    //     let mut new_list_file = OpenOptions::new()
    //         .create(true)
    //         .truncate(true)
    //         .write(true)
    //         .open(&new_list)?;

    //     let resp = source_client
    //         .list_objects(
    //             self.source.bucket.clone(),
    //             self.source.prefix.clone(),
    //             self.attributes.bach_size,
    //             None,
    //         )
    //         .await?;
    //     let mut token = resp.next_token;

    //     let mut new_list_total_lines = 0;
    //     let mut modified_total_lines = 0;

    //     if let Some(objects) = resp.object_list {
    //         objects.iter().for_each(|o| match o.key() {
    //             Some(key) => {
    //                 let _ = new_list_file.write_all(key.as_bytes());
    //                 let _ = new_list_file.write_all("\n".as_bytes());
    //                 new_list_total_lines += 1;
    //                 if let Some(d) = o.last_modified() {
    //                     if d.secs().ge(&timestampe) {
    //                         // 填充变动对象文件
    //                         let target_key = match self.target.prefix.clone() {
    //                             Some(mut s) => {
    //                                 s.push_str(&key);
    //                                 s
    //                             }
    //                             None => {
    //                                 let mut s = "".to_string();
    //                                 s.push_str(&key);
    //                                 s
    //                             }
    //                         };
    //                         // target_key.push_str(&key);
    //                         let record = RecordDescription {
    //                             source_key: key.to_string(),
    //                             target_key,
    //                             list_file_path: "".to_string(),
    //                             list_file_position: FilePosition::default(),
    //                             option: Opt::PUT,
    //                         };
    //                         let _ = record.save_json_to_file(&mut modified_out_put_file);
    //                         modified_total_lines += 1;
    //                     }
    //                 }
    //             }
    //             None => {}
    //         });

    //         modified_out_put_file.flush()?;
    //         new_list_file.flush()?;
    //     }

    //     while token.is_some() {
    //         let resp = source_client
    //             .list_objects(
    //                 self.source.bucket.clone(),
    //                 self.source.prefix.clone(),
    //                 self.attributes.bach_size,
    //                 token,
    //             )
    //             .await?;
    //         if let Some(objects) = resp.object_list {
    //             objects.iter().for_each(|o| match o.key() {
    //                 Some(key) => {
    //                     let _ = new_list_file.write_all(key.as_bytes());
    //                     let _ = new_list_file.write_all("\n".as_bytes());
    //                     new_list_total_lines += 1;
    //                     if let Some(d) = o.last_modified() {
    //                         if d.secs().ge(&timestampe) {
    //                             // 填充变动对象文件
    //                             let target_key = match self.target.prefix.clone() {
    //                                 Some(mut s) => {
    //                                     s.push_str(&key);
    //                                     s
    //                                 }
    //                                 None => {
    //                                     let mut s = "".to_string();
    //                                     s.push_str(&key);
    //                                     s
    //                                 }
    //                             };
    //                             // target_key.push_str(&key);
    //                             let record = RecordDescription {
    //                                 source_key: key.to_string(),
    //                                 target_key,
    //                                 list_file_path: "".to_string(),
    //                                 list_file_position: FilePosition::default(),
    //                                 option: Opt::PUT,
    //                             };
    //                             let _ = record.save_json_to_file(&mut modified_out_put_file);
    //                             modified_total_lines += 1;
    //                         }
    //                     }
    //                 }
    //                 None => {}
    //             });

    //             modified_out_put_file.flush()?;
    //             new_list_file.flush()?;
    //         }
    //         token = resp.next_token;
    //     }

    //     let modified_size = modified_out_put_file.metadata()?.len();
    //     let new_list_size = new_list_file.metadata()?.len();
    //     let modified_file_description = FileDescription {
    //         path: modified_out_put.to_string(),
    //         size: modified_size,
    //         total_lines: modified_total_lines,
    //     };
    //     let new_list_description = FileDescription {
    //         path: new_list.to_string(),
    //         size: new_list_size,
    //         total_lines: new_list_total_lines,
    //     };

    //     Ok((modified_file_description, new_list_description))
    // }
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
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let mut subffix = records[0].list_file_position.offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        subffix.push_str("_");
        subffix.push_str(now.as_secs().to_string().as_str());

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
