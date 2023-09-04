use crate::{
    checkpoint::Record,
    commons::{json_to_struct, read_lines},
    exception::save_error_record,
    s3::{
        aws_s3::{byte_stream_multi_partes_to_file, byte_stream_to_file},
        OSSDescription,
    },
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::error::GetObjectErrorKind;
use dashmap::DashMap;

use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::{
    runtime::Runtime,
    task::{self, JoinSet},
};
use walkdir::WalkDir;

use super::{
    err_process, gen_file_path, task_actions::TaskActions, TaskAttributes, TaskType,
    CURRENT_LINE_PREFIX, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct DownloadTask {
    pub source: OSSDescription,
    pub local_path: String,
    pub task_attributes: TaskAttributes,
}

impl Default for DownloadTask {
    fn default() -> Self {
        Self {
            source: OSSDescription::default(),
            local_path: "/tmp".to_string(),
            task_attributes: TaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TaskActions for DownloadTask {
    fn task_type(&self) -> TaskType {
        TaskType::Download
    }

    //基于aws client 通用方案
    // fn exec_task(&self, init: bool) -> Result<()> {
    //     let mut total_lines: usize = 0;
    //     let mut current_line = 0;
    //     let err_counter = Arc::new(AtomicUsize::new(0));
    //     let stop_offset_save_mark = Arc::new(AtomicBool::new(false));
    //     let offset_map = Arc::new(DashMap::<String, usize>::new());

    //     let object_list_file = gen_file_path(self.meta_dir.as_str(), OBJECT_LIST_FILE_PREFIX, "");
    //     let check_point_file = gen_file_path(self.meta_dir.as_str(), CHECK_POINT_FILE_NAME, "");
    //     let mut exclude_regex_set: Option<RegexSet> = None;
    //     let mut include_regex_set: Option<RegexSet> = None;

    //     if let Some(vec_regex_str) = self.exclude.clone() {
    //         let set = RegexSet::new(&vec_regex_str)?;
    //         exclude_regex_set = Some(set);
    //     };

    //     if let Some(vec_regex_str) = self.include.clone() {
    //         let set = RegexSet::new(&vec_regex_str)?;
    //         include_regex_set = Some(set);
    //     };

    //     let rt = runtime::Builder::new_multi_thread()
    //         .worker_threads(num_cpus::get())
    //         .enable_all()
    //         .max_io_events_per_tick(self.task_threads)
    //         .build()?;

    //     // 若不从checkpoint开始，重新生成文件清单
    //     if !self.start_from_checkpoint {
    //         let pb = ProgressBar::new_spinner();
    //         pb.enable_steady_tick(Duration::from_millis(120));
    //         pb.set_style(
    //             ProgressStyle::with_template("{spinner:.green} {msg}")
    //                 .unwrap()
    //                 .tick_strings(&[
    //                     "▰▱▱▱▱▱▱",
    //                     "▰▰▱▱▱▱▱",
    //                     "▰▰▰▱▱▱▱",
    //                     "▰▰▰▰▱▱▱",
    //                     "▰▰▰▰▰▱▱",
    //                     "▰▰▰▰▰▰▱",
    //                     "▰▰▰▰▰▰▰",
    //                     "▰▱▱▱▱▱▱",
    //                 ]),
    //         );
    //         pb.set_message("Generating object list ...");
    //         match self.generate_object_list(&rt, 0, object_list_file.as_str()) {
    //             Ok(lines) => {
    //                 total_lines = lines;
    //                 pb.finish_with_message("object list Done");
    //             }
    //             Err(e) => {
    //                 pb.finish_with_message("object list Fail");
    //                 return Err(e);
    //             }
    //         }
    //     }

    //     let mut execut_set: JoinSet<()> = JoinSet::new();
    //     let mut sys_set: JoinSet<()> = JoinSet::new();
    //     let mut file = File::open(object_list_file.as_str())?;

    //     rt.block_on(async {
    //         let mut file_position = 0;
    //         let mut vec_keys: Vec<Record> = vec![];

    //         if self.start_from_checkpoint {
    //             // 执行错误补偿，重新执行错误日志中的记录
    //             match self.error_record_retry() {
    //                 Ok(_) => {}
    //                 Err(e) => {
    //                     log::error!("{}", e);
    //                     return;
    //                 }
    //             };

    //             let checkpoint =
    //                 match get_task_checkpoint(check_point_file.as_str(), self.meta_dir.as_str()) {
    //                     Ok(c) => c,
    //                     Err(e) => {
    //                         log::error!("{}", e);
    //                         return;
    //                     }
    //                 };
    //             if let Err(e) = file.seek(SeekFrom::Start(checkpoint.execute_position)) {
    //                 log::error!("{}", e);
    //                 return;
    //             };
    //         }

    //         // 启动checkpoint记录线程
    //         let map = Arc::clone(&offset_map);
    //         let stop_mark = Arc::clone(&stop_offset_save_mark);
    //         let obj_list = object_list_file.clone();
    //         let save_to = check_point_file.clone();
    //         sys_set.spawn(async move {
    //             snapshot_offset_to_file(save_to.as_str(), obj_list, stop_mark, map, 3)
    //         });

    //         // 启动进度条线程
    //         let map = Arc::clone(&offset_map);
    //         let stop_mark = Arc::clone(&stop_offset_save_mark);
    //         let total = TryInto::<u64>::try_into(total_lines).unwrap();
    //         sys_set.spawn(async move {
    //             exec_processbar(total, stop_mark, map, CURRENT_LINE_PREFIX);
    //         });

    //         // 按列表传输object from source to target
    //         let lines = io::BufReader::new(file).lines();
    //         let mut line_num = 0;
    //         for line in lines {
    //             // 若错误达到上限，则停止任务
    //             if err_counter.load(std::sync::atomic::Ordering::SeqCst) >= self.max_errors {
    //                 break;
    //             }
    //             if let Result::Ok(key) = line {
    //                 let len = key.bytes().len() + "\n".bytes().len();
    //                 file_position += len;
    //                 line_num += 1;
    //                 if !key.ends_with("/") {
    //                     let record = Record {
    //                         key,
    //                         offset: file_position,
    //                         line_num,
    //                     };
    //                     match exclude_regex_set {
    //                         Some(ref exclude) => {
    //                             if exclude.is_match(&record.key) {
    //                                 continue;
    //                             }
    //                         }
    //                         None => {}
    //                     }
    //                     match include_regex_set {
    //                         Some(ref set) => {
    //                             if set.is_match(&record.key) {
    //                                 vec_keys.push(record);
    //                             }
    //                         }
    //                         None => {
    //                             vec_keys.push(record);
    //                         }
    //                     }
    //                     current_line += 1;
    //                 }
    //             };

    //             if vec_keys.len().to_string().eq(&self.bach_size.to_string()) {
    //                 while execut_set.len() >= self.task_threads {
    //                     execut_set.join_next().await;
    //                 }
    //                 let vk = vec_keys.clone();
    //                 let batch = TryInto::<usize>::try_into(self.bach_size).unwrap();
    //                 self.records_excutor(
    //                     &mut execut_set,
    //                     vk,
    //                     Arc::clone(&err_counter),
    //                     Arc::clone(&offset_map),
    //                     current_line - batch,
    //                 );
    //                 // 清理临时key vec
    //                 vec_keys.clear();
    //             }
    //         }

    //         // 处理集合中的剩余数据，若错误达到上限，则不执行后续操作
    //         if vec_keys.len() > 0
    //             && err_counter.load(std::sync::atomic::Ordering::SeqCst) < self.max_errors
    //         {
    //             while execut_set.len() >= self.task_threads {
    //                 execut_set.join_next().await;
    //             }

    //             let vk = vec_keys.clone();
    //             self.records_excutor(
    //                 &mut execut_set,
    //                 vk,
    //                 Arc::clone(&err_counter),
    //                 Arc::clone(&offset_map),
    //                 total_lines - current_line,
    //             );
    //         }

    //         while execut_set.len() > 0 {
    //             execut_set.join_next().await;
    //         }

    //         // 配置停止 offset save 标识为 true
    //         stop_offset_save_mark.store(true, std::sync::atomic::Ordering::Relaxed);
    //         while sys_set.len() > 0 {
    //             sys_set.join_next().await;
    //         }
    //         // 记录checkpoint
    //         let position: u64 = file_position.try_into().unwrap();
    //         let checkpoint = CheckPoint {
    //             execute_file_path: object_list_file.clone(),
    //             execute_position: position,
    //         };
    //         if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
    //             log::error!("{}", e);
    //         };
    //     });

    //     // pb.finish_with_message("downloaded");
    //     Ok(())
    // }

    fn error_record_retry(&self) -> Result<()> {
        // 遍历错误记录
        for entry in WalkDir::new(self.task_attributes.meta_dir.as_str())
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
                        let download = DownLoadRecordsExecutor {
                            local_path: self.local_path.clone(),
                            source: self.source.clone(),
                            error_conter: Arc::new(AtomicUsize::new(0)),
                            offset_map: Arc::new(DashMap::<String, usize>::new()),
                            meta_dir: self.task_attributes.meta_dir.clone(),
                            target_exist_skip: self.task_attributes.target_exists_skip,
                            large_file_size: self.task_attributes.large_file_size,
                            multi_part_chunk: self.task_attributes.multi_part_chunk,
                            begin_line_number: 0,
                        };
                        let _ = download.exec(record_vec);
                    }
                }

                let _ = fs::remove_file(p);
            }
        }

        Ok(())
    }

    fn generate_object_list(
        &self,
        rt: &Runtime,
        last_modify_timestamp: i64,
        object_list_file: &str,
    ) -> Result<usize> {
        // 预清理meta目录
        let _ = fs::remove_dir_all(self.task_attributes.meta_dir.as_str());
        let mut interrupted = false;
        let mut total_lines = 0;

        rt.block_on(async {
            let client_source = match self.source.gen_oss_client() {
                Result::Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    interrupted = true;
                    return;
                }
            };

            // 若为持续同步模式，且 last_modify_timestamp 大于 0，则将 last_modify 属性大于last_modify_timestamp变量的对象加入执行列表
            let total_rs = match last_modify_timestamp > 0 {
                true => {
                    client_source
                        .append_last_modify_greater_object_to_file(
                            self.source.bucket.clone(),
                            self.source.prefix.clone(),
                            self.task_attributes.bach_size,
                            object_list_file.to_string(),
                            last_modify_timestamp,
                        )
                        .await
                }
                false => {
                    client_source
                        .append_all_object_list_to_file(
                            self.source.bucket.clone(),
                            self.source.prefix.clone(),
                            self.task_attributes.bach_size,
                            object_list_file.to_string(),
                        )
                        .await
                }
            };
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

    async fn records_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        records: Vec<Record>,
        error_conter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, usize>>,
        current_line_number: usize,
    ) {
        let download = DownLoadRecordsExecutor {
            local_path: self.local_path.clone(),
            source: self.source.clone(),
            error_conter,
            offset_map,
            meta_dir: self.task_attributes.meta_dir.clone(),
            target_exist_skip: false,
            large_file_size: self.task_attributes.large_file_size,
            multi_part_chunk: self.task_attributes.multi_part_chunk,
            begin_line_number: current_line_number,
        };
        // task::spawn(async move {
        //     if let Err(e) = download.exec(records).await {
        //         download
        //             .error_conter
        //             .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        //         log::error!("{}", e);
        //     };
        // });
        joinset.spawn(async move {
            if let Err(e) = download.exec(records).await {
                download
                    .error_conter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::error!("{}", e);
            };
        });
    }
}

#[derive(Debug, Clone)]
pub struct DownLoadRecordsExecutor {
    pub local_path: String,
    pub source: OSSDescription,
    pub error_conter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
    pub begin_line_number: usize,
}

impl DownLoadRecordsExecutor {
    pub async fn exec(&self, records: Vec<Record>) -> Result<()> {
        let mut line_num = self.begin_line_number;
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        let mut current_line_key = CURRENT_LINE_PREFIX.to_string();
        offset_key.push_str(&subffix);
        current_line_key.push_str(&self.begin_line_number.to_string());
        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);
        // 先写首行日志，避免错误漏记
        self.offset_map
            .insert(offset_key.clone(), records[0].offset);
        // 与记录当前行数
        let num = TryInto::<usize>::try_into(line_num).unwrap();
        self.offset_map.insert(current_line_key.clone(), num);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;
        for record in records {
            let resp = match c_s
                .get_object(&self.source.bucket.as_str(), record.key.as_str())
                .await
            {
                core::result::Result::Ok(b) => b,
                Err(e) => {
                    log::error!("{}", e);
                    // 源端文件不存在按传输成功处理
                    match e.into_service_error().kind {
                        GetObjectErrorKind::InvalidObjectState(_)
                        | GetObjectErrorKind::Unhandled(_) => {
                            save_error_record(&self.error_conter, record.clone(), &mut error_file);
                        }
                        GetObjectErrorKind::NoSuchKey(_) => {}
                        _ => {}
                    }

                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            };
            let t_file_name = gen_file_path(self.local_path.as_str(), &record.key.as_str(), "");
            let t_path = Path::new(t_file_name.as_str());

            // 目标object存在则不下载
            if self.target_exist_skip {
                if t_path.exists() {
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            }

            if let Some(p) = t_path.parent() {
                if let Err(e) = std::fs::create_dir_all(p) {
                    log::error!("{}", e);
                    save_error_record(&self.error_conter, record.clone(), &mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                };
            };

            let mut t_file = match OpenOptions::new()
                .truncate(true)
                .create(true)
                .write(true)
                .open(t_file_name.as_str())
            {
                Ok(p) => p,
                Err(e) => {
                    err_process(
                        &self.error_conter,
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

            let s_len: usize = match resp.content_length().try_into() {
                Ok(len) => len,
                Err(e) => {
                    err_process(
                        &self.error_conter,
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

            // 大文件走 multi part download 分支
            match s_len > self.large_file_size {
                true => {
                    if let Err(e) =
                        byte_stream_multi_partes_to_file(resp, &mut t_file, self.multi_part_chunk)
                            .await
                    {
                        err_process(
                            &self.error_conter,
                            anyhow!(e.to_string()),
                            record,
                            &mut error_file,
                            offset_key.as_str(),
                            current_line_key.as_str(),
                            &self.offset_map,
                        );
                        continue;
                    };
                }
                false => {
                    if let Err(e) = byte_stream_to_file(resp.body, &mut t_file).await {
                        err_process(
                            &self.error_conter,
                            anyhow!(e.to_string()),
                            record,
                            &mut error_file,
                            offset_key.as_str(),
                            current_line_key.as_str(),
                            &self.offset_map,
                        );
                        continue;
                    };
                }
            }

            self.offset_map.insert(offset_key.clone(), record.offset);
            let num = TryInto::<usize>::try_into(line_num).unwrap();
            self.offset_map.insert(current_line_key.clone(), num);

            line_num += 1;
        }
        self.offset_map.remove(&offset_key);
        self.offset_map.remove(&current_line_key);
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
