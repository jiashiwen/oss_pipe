use super::{
    gen_file_path, TaskStage, TaskStatusSaver, CHECK_POINT_FILE_NAME, OBJECT_LIST_FILE_PREFIX,
    OFFSET_PREFIX,
};
use super::{
    task_actions::TransferTaskActions, IncrementAssistant, TransferLocal2Local, TransferLocal2Oss,
    TransferOss2Local, TransferOss2Oss, TransferTaskAttributes,
};
use crate::{
    checkpoint::FileDescription,
    commons::{promote_processbar, RegexFilter},
    s3::OSSDescription,
    tasks::NOTIFY_FILE_PREFIX,
};
use crate::{
    checkpoint::{get_task_checkpoint, CheckPoint, FilePosition, ListedRecord},
    commons::quantify_processbar,
};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{self, BufRead},
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tabled::builder::Builder;
use tabled::settings::Style;
use tabled::{builder, row, Table};
use tokio::{
    runtime,
    sync::Mutex,
    task::{self, JoinSet},
};
use walkdir::WalkDir;

pub struct AnalyzedResult {
    pub size_range: String,
    pub objects: i128,
    pub total_objects: i128,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
#[serde(rename_all = "lowercase")]
// #[serde(tag = "type")]
pub enum ObjectStorage {
    Local(String),
    OSS(OSSDescription),
}

impl Default for ObjectStorage {
    fn default() -> Self {
        ObjectStorage::OSS(OSSDescription::default())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferTask {
    pub source: ObjectStorage,
    pub target: ObjectStorage,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferTask {
    fn default() -> Self {
        Self {
            source: ObjectStorage::OSS(OSSDescription::default()),
            target: ObjectStorage::OSS(OSSDescription::default()),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

impl TransferTask {
    pub fn gen_transfer_actions(&self) -> Box<dyn TransferTaskActions + Send + Sync> {
        match &self.source {
            ObjectStorage::Local(path_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferLocal2Local {
                        source: path_s.to_string(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferLocal2Oss {
                        source: path_s.to_string(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
            },
            ObjectStorage::OSS(oss_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferOss2Local {
                        source: oss_s.clone(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferOss2Oss {
                        source: oss_s.clone(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
            },
        }
    }

    pub fn analyze(&self) -> Result<()> {
        let task = self.gen_transfer_actions();
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .enable_all()
            .max_io_events_per_tick(self.attributes.task_threads)
            .build()?;

        rt.block_on(async {
            let map = match task.analyze_source().await {
                Ok(m) => m,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            let mut total_objects = 0;
            for v in map.iter() {
                total_objects += *v.value();
            }
            let total = Decimal::from(total_objects);

            let mut builder = Builder::default();
            for kv in map.iter() {
                let val_dec = Decimal::from(*kv.value());

                let key = kv.key().to_string();
                let val = kv.value().to_string();
                let percent = (val_dec / total).round_dp(2).to_string();
                let raw = vec![key, val, percent];
                builder.push_record(raw);
            }

            let header = vec!["size_range", "objects", "percent"];
            builder.set_header(header);

            let mut table = builder.build();
            table.with(Style::ascii_rounded());
            println!("{}", table);
        });
        Ok(())
    }

    pub fn execute(&self) -> Result<()> {
        let task = self.gen_transfer_actions();
        let mut interrupt: bool = false;

        // 执行过程中错误数统计
        let err_counter = Arc::new(AtomicUsize::new(0));
        // 任务停止标准，用于通知所有协程任务结束
        let snapshot_stop_mark = Arc::new(AtomicBool::new(false));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

        let mut executed_file = FileDescription {
            path: gen_file_path(
                self.attributes.meta_dir.as_str(),
                OBJECT_LIST_FILE_PREFIX,
                now.as_secs().to_string().as_str(),
            ),
            size: 0,
            total_lines: 0,
        };

        let check_point_file =
            gen_file_path(self.attributes.meta_dir.as_str(), CHECK_POINT_FILE_NAME, "");

        let mut assistant = IncrementAssistant::default();
        assistant.check_point_path = check_point_file.clone();
        let increment_assistant = Arc::new(Mutex::new(assistant));

        let regex_filter =
            RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)?;

        let mut list_file = None;
        let mut list_file_position = FilePosition::default();

        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .enable_all()
            .max_io_events_per_tick(self.attributes.task_threads)
            .build()?;

        let pd = promote_processbar("Generating object list ...");
        rt.block_on(async {
            if self.attributes.start_from_checkpoint {
                // 执行error retry
                match task.error_record_retry() {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("{}", e);
                        interrupt = true;
                        return;
                    }
                };

                // 变更object_list_file_name文件名
                let checkpoint = match get_task_checkpoint(check_point_file.as_str()) {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("{}", e);
                        interrupt = true;
                        return;
                    }
                };
                executed_file = checkpoint.executed_file.clone();

                // 增加清理notify file 逻辑
                for entry in WalkDir::new(&self.attributes.meta_dir)
                    .into_iter()
                    .filter_map(Result::ok)
                    .filter(|e| !e.file_type().is_dir())
                {
                    if let Some(p) = entry.path().to_str() {
                        if p.contains(NOTIFY_FILE_PREFIX) {
                            let _ = fs::remove_file(p);
                        }
                    };
                }

                match checkpoint.task_stage {
                    TaskStage::Stock => match checkpoint.seeked_execute_file() {
                        Ok(f) => {
                            list_file_position = checkpoint.executed_file_position.clone();
                            list_file = Some(f);
                        }
                        Err(e) => {
                            log::error!("{}", e);
                            interrupt = true;
                            return;
                        }
                    },
                    TaskStage::Increment => {
                        // 清理文件重新生成object list 文件需大于指定时间戳,并根据原始object list 删除位于目标端但源端不存在的文件
                        let timestamp = TryInto::<i64>::try_into(checkpoint.timestampe).unwrap();
                        let _ = fs::remove_file(&executed_file.path);
                        match task
                            .generate_execute_file(Some(timestamp), &executed_file.path)
                            .await
                        {
                            Ok(f) => {
                                executed_file = f;
                            }
                            Err(e) => {
                                log::error!("{}", e);
                                interrupt = true;
                                return;
                            }
                        };
                    }
                }
            } else {
                // 清理 meta 目录
                // 重新生成object list file
                let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());
                match task.generate_execute_file(None, &executed_file.path).await {
                    Ok(f) => {
                        executed_file = f;
                    }
                    Err(e) => {
                        log::error!("{}", e);
                        interrupt = true;
                        return;
                    }
                }
            }
        });

        if interrupt {
            return Err(anyhow!("get object list error"));
        }

        pd.finish_with_message("object list generated");

        // sys_set 用于执行checkpoint、notify等辅助任务
        let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        let mut execut_set: JoinSet<()> = JoinSet::new();

        let object_list_file = match list_file {
            Some(f) => f,
            None => File::open(&executed_file.path)?,
        };
        rt.block_on(async {
            let mut file_for_notify = None;

            // 持续同步逻辑: 执行增量助理
            let task_increment_prelude = self.gen_transfer_actions();
            if self.attributes.continuous {
                let assistant = Arc::clone(&increment_assistant);
                task::spawn(async move {
                    if let Err(e) = task_increment_prelude.increment_prelude(assistant).await {
                        log::error!("{}", e);
                    }
                });

                // 当源存储为本地时，获取notify文件
                if let ObjectStorage::Local(_) = self.source {
                    while file_for_notify.is_none() {
                        let lock = increment_assistant.lock().await;
                        file_for_notify = match lock.get_notify_file_path() {
                            Some(s) => Some(s),
                            None => None,
                        };
                        drop(lock);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }

            // 启动checkpoint记录线程
            let stock_status_saver = TaskStatusSaver {
                check_point_path: check_point_file.clone(),
                executed_file: executed_file.clone(),
                stop_mark: Arc::clone(&snapshot_stop_mark),
                list_file_positon_map: Arc::clone(&offset_map),
                file_for_notify,
                task_stage: TaskStage::Stock,
                interval: 3,
                current_stock_object_list_file: executed_file.path.clone(),
            };
            sys_set.spawn(async move {
                stock_status_saver.snapshot_to_file().await;
            });

            // 启动进度条线程
            let map = Arc::clone(&offset_map);
            let stop_mark = Arc::clone(&snapshot_stop_mark);
            let total = executed_file.total_lines;
            sys_set.spawn(async move {
                // Todo 调整进度条
                quantify_processbar(total, stop_mark, map, OFFSET_PREFIX).await;
            });
            let task_stock = self.gen_transfer_actions();
            let mut vec_keys: Vec<ListedRecord> = vec![];
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

                        if regex_filter.filter(&record.key) {
                            vec_keys.push(record);
                        }
                    }
                };

                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.bach_size.to_string())
                {
                    while execut_set.len() >= self.attributes.task_threads {
                        execut_set.join_next().await;
                    }
                    let vk = vec_keys.clone();
                    task_stock
                        .records_excutor(
                            &mut execut_set,
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
                while execut_set.len() >= self.attributes.task_threads {
                    execut_set.join_next().await;
                }

                let vk = vec_keys.clone();
                task_stock
                    .records_excutor(
                        &mut execut_set,
                        vk,
                        Arc::clone(&err_counter),
                        Arc::clone(&offset_map),
                        executed_file.path.clone(),
                    )
                    .await;
            }

            while execut_set.len() > 0 {
                execut_set.join_next().await;
            }
            // 配置停止 offset save 标识为 true
            snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
            let lock = increment_assistant.lock().await;
            let notify = lock.get_notify_file_path();
            drop(lock);

            // 记录checkpoint
            let mut checkpoint: CheckPoint = CheckPoint {
                executed_file: executed_file.clone(),
                executed_file_position: list_file_position.clone(),
                file_for_notify: notify,
                task_stage: TaskStage::Stock,
                timestampe: 0,
                current_stock_object_list_file: executed_file.path.clone(),
            };
            if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
                log::error!("{}", e);
            };

            while sys_set.len() > 0 {
                sys_set.join_next().await;
            }
        });

        // 增量逻辑
        if self.attributes.continuous {
            rt.block_on(async {
                // let pd = promote_processbar("Executing increment ...");

                let stop_mark = Arc::new(AtomicBool::new(false));
                let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
                let task_increment = self.gen_transfer_actions();

                let _ = task_increment
                    .execute_increment(
                        &mut execut_set,
                        Arc::clone(&increment_assistant),
                        Arc::clone(&err_counter),
                        Arc::clone(&offset_map),
                        Arc::clone(&stop_mark),
                    )
                    .await;
                // 配置停止 offset save 标识为 true
                snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
                // pd.finish_with_message("Execut increment down");
            });
        }

        Ok(())
    }
}
