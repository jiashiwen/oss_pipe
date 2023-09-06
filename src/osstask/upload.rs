use crate::commons::{json_to_struct, read_lines, scan_folder_files_to_file};
use crate::{checkpoint::Record, s3::OSSDescription};
use anyhow::anyhow;

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::types::ByteStream;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
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
use super::task_actions::TaskActions;
use super::TaskAttributes;
use super::TaskType;
use super::CURRENT_LINE_PREFIX;
use super::{
    gen_file_path, Task, TaskDescription, TaskUpLoad, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX,
};

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
impl TaskActions for UploadTask {
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
                        let upload = UpLoadRecordsExecutor {
                            local_path: self.local_path.clone(),
                            target: self.target.clone(),
                            err_counter: Arc::new(AtomicUsize::new(0)),
                            offset_map: Arc::new(DashMap::<String, usize>::new()),
                            meta_dir: self.task_attributes.meta_dir.clone(),
                            target_exist_skip: self.task_attributes.target_exists_skip,
                            large_file_size: self.task_attributes.large_file_size,
                            multi_part_chunk: self.task_attributes.multi_part_chunk,
                        };
                        let _ = upload.exec(record_vec);
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
        let upload = UpLoadRecordsExecutor {
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
            if let Err(e) = upload.exec(records).await {
                upload
                    .err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }

    // 生成对象列表
    fn generate_object_list(
        &self,
        rt: &Runtime,
        last_modify_timestamp: i64,
        object_list_file: &str,
    ) -> Result<usize> {
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
}

#[derive(Debug, Clone)]
pub struct UpLoadRecordsExecutor {
    pub local_path: String,
    pub target: OSSDescription,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
}

impl UpLoadRecordsExecutor {
    pub async fn exec(&self, records: Vec<Record>) -> Result<()> {
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

            let content_len: usize = match match s_file.metadata() {
                Ok(m) => m.len(),
                Err(e) => {
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
            }
            .try_into()
            {
                Ok(l) => l,
                Err(e) => {
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
            };

            // 大文件走 multi part upload 分支
            if let Err(e) = match content_len > self.large_file_size {
                true => {
                    c_t.multipart_upload_local_file(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        &mut s_file,
                        self.multi_part_chunk,
                    )
                    .await
                }
                false => {
                    c_t.upload_object_from_local(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        s_file_name.as_str(),
                    )
                    .await
                }
            } {
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
            };

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
}

#[derive(Debug, Clone)]
pub struct UpLoad {
    pub local_path: String,
    pub target: OSSDescription,
    pub err_counter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
}

impl UpLoad {
    pub fn from_taskupload(
        task_upload: &TaskUpLoad,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, usize>>,
    ) -> Self {
        Self {
            local_path: task_upload.local_path.clone(),
            target: task_upload.target.clone(),
            err_counter,
            offset_map,
            meta_dir: task_upload.meta_dir.clone(),
            target_exist_skip: task_upload.target_exists_skip,
            large_file_size: task_upload.large_file_size,
            multi_part_chunk: task_upload.multi_part_chunk,
        }
    }

    pub fn from_task(
        task: &Task,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, usize>>,
    ) -> Result<Self> {
        if let TaskDescription::Upload(upload) = task.task_desc.clone() {
            let up = Self {
                local_path: upload.local_path.clone(),
                target: upload.target.clone(),
                err_counter,
                offset_map,
                meta_dir: upload.meta_dir.clone(),
                target_exist_skip: upload.target_exists_skip,
                large_file_size: upload.large_file_size,
                multi_part_chunk: upload.multi_part_chunk,
            };
            return Ok(up);
        }
        Err(anyhow!("task type not upload"))
    }

    pub async fn exec(&self, records: Vec<Record>) -> Result<()> {
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
                        log::error!("{}", e);
                        self.err_counter
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = record.save_json_to_file(&mut error_file);
                        self.offset_map.insert(offset_key.clone(), record.offset);
                    }
                }
            }

            let content_len: usize = match match s_file.metadata() {
                Ok(m) => m.len(),
                Err(e) => {
                    log::error!("{}", e);
                    self.err_counter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = record.save_json_to_file(&mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            }
            .try_into()
            {
                Ok(l) => l,
                Err(e) => {
                    log::error!("{}", e);
                    self.err_counter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = record.save_json_to_file(&mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            };

            // 大文件走 multi part upload 分支
            if let Err(e) = match content_len > self.large_file_size {
                true => {
                    c_t.multipart_upload_local_file(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        &mut s_file,
                        self.multi_part_chunk,
                    )
                    .await
                }
                false => {
                    let mut body = vec![];
                    if let Err(e) = s_file.read_to_end(&mut body) {
                        log::error!("{}", e);
                        self.err_counter
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = record.save_json_to_file(&mut error_file);
                        self.offset_map.insert(offset_key.clone(), record.offset);
                        continue;
                    };
                    c_t.upload_object_bytes(
                        self.target.bucket.as_str(),
                        target_key.as_str(),
                        None,
                        ByteStream::from(body),
                    )
                    .await
                }
            } {
                log::error!("{}", e);
                self.err_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = record.save_json_to_file(&mut error_file);
                self.offset_map.insert(offset_key.clone(), record.offset);
            };

            self.offset_map.insert(offset_key.clone(), record.offset);
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
