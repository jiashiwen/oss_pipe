use crate::commons::{
    json_to_struct, read_lines, scan_folder_files_to_file, Modified, ModifyType, NotifyWatcher,
    PathType,
};
use crate::exception::{process_error, ErrRecord};
use crate::s3::aws_s3::OssClient;
use crate::{checkpoint::Record, s3::OSSDescription};
use anyhow::anyhow;

use anyhow::Result;
use async_trait::async_trait;

use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
use serde_json::from_str;
use std::fs::File;
use std::io::{self, BufRead, Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};

use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;
use walkdir::WalkDir;

use super::err_process;
use super::task_actions::TaskActionsFromLocal;
use super::TaskAttributes;
use super::TaskType;
use super::CURRENT_LINE_PREFIX;
use super::{gen_file_path, Task, TaskDescription, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct UploadTask {
    pub target: OSSDescription,
    pub local_path: String,
    pub task_attributes: TaskAttributes,
}

impl Default for UploadTask {
    fn default() -> Self {
        Self {
            target: OSSDescription::default(),
            local_path: "/tmp".to_string(),
            task_attributes: TaskAttributes::default(),
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
            // let file_name = entry.file_name().to_str().unwrap();
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
                                let record = match json_to_struct::<Record>(content.as_str()) {
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
                            local_path: self.local_path.clone(),
                            target: self.target.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            offset_map: Arc::new(DashMap::<String, usize>::new()),
                            meta_dir: self.task_attributes.meta_dir.clone(),
                            target_exist_skip: self.task_attributes.target_exists_skip,
                            large_file_size: self.task_attributes.large_file_size,
                            multi_part_chunk: self.task_attributes.multi_part_chunk,
                        };
                        let _ = upload.exec_records(record_vec);
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
        records: Vec<Record>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, usize>>,
    ) {
        let upload = UpLoadExecutor {
            local_path: self.local_path.clone(),
            target: self.target.clone(),
            err_counter,
            offset_map,
            meta_dir: self.task_attributes.meta_dir.clone(),
            target_exist_skip: false,
            large_file_size: self.task_attributes.large_file_size,
            multi_part_chunk: self.task_attributes.multi_part_chunk,
        };

        joinset.spawn(async move {
            if let Err(e) = upload.exec_records(records).await {
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
            let total_rs = scan_folder_files_to_file(self.local_path.as_str(), &object_list_file);
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
        let watcher = NotifyWatcher::new(self.local_path.as_str())?;
        Ok(watcher)
    }

    async fn execute_increment(
        &self,
        notify_file: &str,
        notify_file_size: Arc<AtomicU64>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, usize>>,
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

        loop {
            if notify_file_size.load(Ordering::SeqCst).le(&offset) {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            let mut file = match File::open(notify_file) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{}", e);
                    continue;
                }
            };

            if let Err(e) = file.seek(SeekFrom::Start(offset)) {
                log::error!("{}", e);
                continue;
            };

            let subffix = offset.to_string();
            let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
            let mut current_line_key = CURRENT_LINE_PREFIX.to_string();
            offset_key.push_str(&subffix);
            current_line_key.push_str(&line_num.to_string());
            // 先写首行日志，避免错误漏记
            let offset_usize = TryInto::<usize>::try_into(offset).unwrap();
            offset_map.insert(offset_key.clone(), offset_usize);
            // 与记录当前行数
            offset_map.insert(current_line_key.clone(), line_num);

            let subffix = offset.to_string();
            let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
            offset_key.push_str(&subffix);
            let error_file_name = gen_file_path(
                &self.task_attributes.meta_dir,
                ERROR_RECORD_PREFIX,
                &subffix,
            );

            offset_map.insert(offset_key.clone(), offset_usize);

            let mut error_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(error_file_name.as_str())
                .unwrap();

            let lines = io::BufReader::new(file).lines();
            for line in lines {
                if let Result::Ok(key) = line {
                    line_num += 1;
                    // Modifed 解析
                    match from_str::<Modified>(key.as_str()) {
                        Ok(m) => {
                            println!("{:?}", m);
                            let mut target_path = m.path.clone();
                            match self.local_path.ends_with("/") {
                                true => target_path.drain(..self.local_path.len()),
                                false => target_path.drain(..self.local_path.len() + 1),
                            };
                            if let Some(prefix) = self.target.prefix.clone() {
                                target_path.insert_str(0, &prefix);
                            }
                            let record = ErrRecord {
                                source: m.path.clone(),
                                target: target_path,
                                list_file_offset: offset_usize,
                                list_file_line_num: line_num,
                            };
                            if let Err(e) = self.modified_handler(m, &client).await {
                                process_error(
                                    &err_counter,
                                    e,
                                    record,
                                    &mut error_file,
                                    &offset_key,
                                    &current_line_key,
                                    &offset_map,
                                )
                            };
                        }
                        Err(e) => {
                            log::error!("{}", e);
                            continue;
                        }
                    };
                }
            }
            offset = notify_file_size.load(Ordering::SeqCst);
        }
    }

    // ToDo
    // 新建 modify handler，用于处理modify文件
    async fn modified_handler(&self, modified: Modified, client: &OssClient) -> Result<()> {
        let mut target_path = modified.path.clone();

        match self.local_path.ends_with("/") {
            true => target_path.drain(..self.local_path.len()),
            false => target_path.drain(..self.local_path.len() + 1),
        };

        if let Some(prefix) = self.target.prefix.clone() {
            target_path.insert_str(0, &prefix);
        }

        if PathType::File.eq(&modified.path_type) {
            let r = match modified.modify_type {
                ModifyType::Create | ModifyType::Modify => {
                    client
                        .upload_from_local(
                            &self.target.bucket,
                            target_path.as_str(),
                            &modified.path,
                            self.task_attributes.large_file_size,
                            self.task_attributes.multi_part_chunk,
                        )
                        .await
                }
                ModifyType::Delete => {
                    match client
                        .remove_object(&self.target.bucket, target_path.as_str())
                        .await
                    {
                        Ok(_) => Ok(()),
                        Err(e) => Err(anyhow!("{}", e.to_string())),
                    }
                }

                ModifyType::Unkown => Ok(()),
            };
            return r;
        };
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct UpLoadExecutor {
    pub local_path: String,
    pub target: OSSDescription,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
}

impl UpLoadExecutor {
    pub async fn exec_records(&self, records: Vec<Record>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        let mut current_line_key = CURRENT_LINE_PREFIX.to_string();
        offset_key.push_str(&subffix);
        current_line_key.push_str(&records[0].line_num.to_string());
        // 先写首行日志，避免错误漏记
        self.offset_map
            .insert(offset_key.clone(), records[0].offset);
        // 与记录当前行数
        self.offset_map
            .insert(current_line_key.clone(), records[0].line_num);

        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        offset_key.push_str(&subffix);
        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        // 先写首行日志，避免错误漏记
        self.offset_map
            .insert(offset_key.clone(), records[0].offset);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_t = self.target.gen_oss_client()?;
        for record in records {
            let s_file_name = gen_file_path(self.local_path.as_str(), &record.key.as_str(), "");

            // 判断源文件是否存在
            let s_path = Path::new(s_file_name.as_str());
            if !s_path.exists() {
                self.offset_map.insert(offset_key.clone(), record.offset);
                continue;
            }

            let mut s_file = OpenOptions::new().read(true).open(s_file_name.as_str())?;

            let mut target_key = "".to_string();
            if let Some(s) = self.target.prefix.clone() {
                target_key.push_str(&s);
            };
            target_key.push_str(&record.key);

            // 目标object存在则不推送
            if self.target_exist_skip {
                let target_obj_exists = c_t
                    .object_exists(self.target.bucket.as_str(), target_key.as_str())
                    .await;
                match target_obj_exists {
                    Ok(b) => {
                        if b {
                            self.offset_map.insert(offset_key.clone(), record.offset);
                            continue;
                        }
                    }
                    Err(e) => {
                        err_process(
                            &self.err_counter,
                            anyhow!(e.to_string()),
                            record.clone(),
                            &mut error_file,
                            offset_key.as_str(),
                            current_line_key.as_str(),
                            &self.offset_map,
                        );
                        continue;
                    }
                }
            }

            if let Err(e) = c_t
                .upload_from_local(
                    self.target.bucket.as_str(),
                    target_key.as_str(),
                    &s_file_name,
                    self.large_file_size,
                    self.multi_part_chunk,
                )
                .await
            {
                err_process(
                    &self.err_counter,
                    anyhow!(e.to_string()),
                    record,
                    &mut error_file,
                    offset_key.as_str(),
                    current_line_key.as_str(),
                    &self.offset_map,
                );
                continue;
            }

            self.offset_map.insert(offset_key.clone(), record.offset);
            self.offset_map
                .insert(current_line_key.clone(), record.line_num);
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

    pub async fn exec_modifiedes(
        &self,
        modifiedes: Vec<Modified>,
        notify_file_size: Arc<AtomicU64>,
    ) -> Result<()> {
        //     let subffix = modifiedes[0].to_string();
        //     let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        //     let mut current_line_key = CURRENT_LINE_PREFIX.to_string();
        //     offset_key.push_str(&subffix);
        //     current_line_key.push_str(&records[0].line_num.to_string());
        //     // 先写首行日志，避免错误漏记
        //     self.offset_map
        //         .insert(offset_key.clone(), records[0].offset);
        //     // 与记录当前行数
        //     self.offset_map
        //         .insert(current_line_key.clone(), records[0].line_num);

        //     let subffix = records[0].offset.to_string();
        //     let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        //     offset_key.push_str(&subffix);
        //     let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        //     // 先写首行日志，避免错误漏记
        //     self.offset_map
        //         .insert(offset_key.clone(), records[0].offset);

        //     let mut error_file = OpenOptions::new()
        //         .create(true)
        //         .write(true)
        //         .truncate(true)
        //         .open(error_file_name.as_str())?;

        //     let c_t = self.target.gen_oss_client()?;

        Ok(())
    }

    async fn modified_handler(&self, modified: Modified, client: &OssClient) {
        let mut target_path = modified.path.clone();
        match self.local_path.ends_with("/") {
            true => target_path.drain(..self.local_path.len()),
            false => target_path.drain(..self.local_path.len() + 1),
        };
        if let Some(prefix) = self.target.prefix.clone() {
            target_path.insert_str(0, &prefix);
        }

        if PathType::File.eq(&modified.path_type) {
            let r = match modified.modify_type {
                ModifyType::Create | ModifyType::Modify => {
                    client
                        .upload_from_local(
                            &self.target.bucket,
                            target_path.as_str(),
                            &modified.path,
                            self.large_file_size,
                            self.multi_part_chunk,
                        )
                        .await
                }
                ModifyType::Delete => {
                    match client
                        .remove_object(&self.target.bucket, target_path.as_str())
                        .await
                    {
                        Ok(_) => Ok(()),
                        Err(e) => Err(anyhow!("{}", e.to_string())),
                    }
                }

                ModifyType::Unkown => Ok(()),
            };
        };
    }
}
