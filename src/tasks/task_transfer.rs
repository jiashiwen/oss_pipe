use super::{
    de_usize_from_str, gen_file_path, se_usize_to_str, TaskDefaultParameters, TaskStatusSaver,
    TransferStage, OFFSET_PREFIX, TRANSFER_CHECK_POINT_FILE, TRANSFER_OBJECT_LIST_FILE_PREFIX,
};
use super::{
    task_actions::TransferTaskActions, IncrementAssistant, TransferLocal2Local, TransferLocal2Oss,
    TransferOss2Local, TransferOss2Oss,
};
use crate::checkpoint::RecordDescription;
use crate::commons::{json_to_struct, LastModifyFilter};
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

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TransferType {
    Full,
    Stock,
    Increment,
}

impl TransferType {
    pub fn is_full(&self) -> bool {
        match self {
            TransferType::Full => true,
            _ => false,
        }
    }

    pub fn is_stock(&self) -> bool {
        match self {
            TransferType::Stock => true,
            _ => false,
        }
    }

    pub fn is_increment(&self) -> bool {
        match self {
            TransferType::Increment => true,
            _ => false,
        }
    }
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
pub struct TransferTaskAttributes {
    #[serde(default = "TaskDefaultParameters::batch_size_default")]
    pub bach_size: i32,
    #[serde(default = "TaskDefaultParameters::task_threads_default")]
    pub task_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::max_errors_default")]
    pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub target_exists_skip: bool,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub start_from_checkpoint: bool,
    #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub large_file_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunk_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub multi_part_chunk: usize,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::continuous_default")]
    pub continuous: bool,
    #[serde(default = "TaskDefaultParameters::transfer_type_default")]
    pub transfer_type: TransferType,
    #[serde(default = "TaskDefaultParameters::last_modify_filter_default")]
    pub last_modify_filter: Option<LastModifyFilter>,
}

impl Default for TransferTaskAttributes {
    fn default() -> Self {
        Self {
            bach_size: TaskDefaultParameters::batch_size_default(),
            task_parallelism: TaskDefaultParameters::task_threads_default(),
            max_errors: TaskDefaultParameters::max_errors_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            target_exists_skip: TaskDefaultParameters::target_exists_skip_default(),
            start_from_checkpoint: TaskDefaultParameters::target_exists_skip_default(),
            large_file_size: TaskDefaultParameters::large_file_size_default(),
            multi_part_chunk: TaskDefaultParameters::multi_part_chunk_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
            continuous: TaskDefaultParameters::continuous_default(),
            transfer_type: TaskDefaultParameters::transfer_type_default(),
            last_modify_filter: TaskDefaultParameters::last_modify_filter_default(),
        }
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
            .max_io_events_per_tick(self.attributes.task_parallelism)
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
            // builder.set_header(header);
            builder.insert_record(0, header);

            let mut table = builder.build();
            table.with(Style::ascii_rounded());
            println!("{}", table);
        });
        Ok(())
    }

    pub fn execute(&self) -> Result<()> {
        let task = self.gen_transfer_actions();
        let mut interrupt: bool = false;
        let mut exec_modified = false;

        // 执行过程中错误数统计
        let err_counter = Arc::new(AtomicUsize::new(0));
        // 任务停止标准，用于通知所有协程任务结束
        let snapshot_stop_mark = Arc::new(AtomicBool::new(false));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

        let mut executed_file = FileDescription {
            path: gen_file_path(
                self.attributes.meta_dir.as_str(),
                TRANSFER_OBJECT_LIST_FILE_PREFIX,
                now.as_secs().to_string().as_str(),
            ),
            size: 0,
            total_lines: 0,
        };

        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            TRANSFER_CHECK_POINT_FILE,
            "",
        );

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
            .max_io_events_per_tick(self.attributes.task_parallelism)
            .build()?;

        let pd = promote_processbar("Generating object list ...");
        // 生成执行文件
        rt.block_on(async {
            if self.attributes.start_from_checkpoint {
                // 变更object_list_file_name文件名
                let checkpoint = match get_task_checkpoint(check_point_file.as_str()) {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("{}", e);
                        interrupt = true;
                        return;
                    }
                };

                // 执行error retry
                match task.error_record_retry() {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("{}", e);
                        interrupt = true;
                        return;
                    }
                };
                executed_file = checkpoint.executed_file.clone();

                // 清理notify file
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
                    TransferStage::Stock => match checkpoint.seeked_execute_file() {
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
                    TransferStage::Increment => {
                        // Todo 重新分析逻辑，需要再checkpoint中记录每次增量执行前的起始时间点
                        // 清理文件重新生成object list 文件需大于指定时间戳,并根据原始object list 删除位于目标端但源端不存在的文件
                        // 流程逻辑
                        // 扫描target 文件list-> 抓取自扫描🕙开始，源端的变动数据 -> 生成objlist，action 新增target change capture
                        let modified = match task
                            .changed_object_capture_based_target(checkpoint.timestamp)
                            .await
                        {
                            Ok(f) => f,
                            Err(e) => {
                                log::error!("{}", e);
                                interrupt = true;
                                return;
                            }
                        };

                        match File::open(&modified.path) {
                            Ok(f) => list_file = Some(f),
                            Err(e) => {
                                log::error!("{}", e);
                                interrupt = true;
                                return;
                            }
                        }

                        exec_modified = true;
                    }
                }
            } else {
                // 清理 meta 目录
                // 重新生成object list file
                let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());
                match task
                    .gen_source_object_list_file(None, &executed_file.path)
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
        let mut execut_set = JoinSet::new();

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

            if exec_modified {
                let task_modify = self.gen_transfer_actions();
                let mut vec_keys: Vec<RecordDescription> = vec![];
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
                    if let Result::Ok(l) = line {
                        let record = match json_to_struct::<RecordDescription>(&l) {
                            Ok(r) => r,
                            Err(e) => {
                                log::error!("{}", e);
                                err_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                continue;
                            }
                        };
                        vec_keys.push(record);
                    };

                    if vec_keys
                        .len()
                        .to_string()
                        .eq(&self.attributes.bach_size.to_string())
                    {
                        while execut_set.len() >= self.attributes.task_parallelism {
                            execut_set.join_next().await;
                        }
                        let vk = vec_keys.clone();
                        task_modify
                            .record_descriptions_transfor(
                                &mut execut_set,
                                vk,
                                Arc::clone(&snapshot_stop_mark),
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
                    while execut_set.len() >= self.attributes.task_parallelism {
                        execut_set.join_next().await;
                    }

                    let vk = vec_keys.clone();
                    task_modify
                        .record_descriptions_transfor(
                            &mut execut_set,
                            vk,
                            Arc::clone(&snapshot_stop_mark),
                            Arc::clone(&err_counter),
                            Arc::clone(&offset_map),
                            executed_file.path.clone(),
                        )
                        .await;
                }
            } else {
                // 启动checkpoint记录线程
                let stock_status_saver = TaskStatusSaver {
                    check_point_path: check_point_file.clone(),
                    executed_file: executed_file.clone(),
                    stop_mark: Arc::clone(&snapshot_stop_mark),
                    list_file_positon_map: Arc::clone(&offset_map),
                    file_for_notify,
                    task_stage: TransferStage::Stock,
                    interval: 3,
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
                        while execut_set.len() >= self.attributes.task_parallelism {
                            execut_set.join_next().await;
                        }
                        let vk = vec_keys.clone();
                        task_stock
                            .listed_records_transfor(
                                &mut execut_set,
                                vk,
                                Arc::clone(&snapshot_stop_mark),
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
                    while execut_set.len() >= self.attributes.task_parallelism {
                        execut_set.join_next().await;
                    }

                    let vk = vec_keys.clone();
                    task_stock
                        .listed_records_transfor(
                            &mut execut_set,
                            vk,
                            Arc::clone(&snapshot_stop_mark),
                            Arc::clone(&err_counter),
                            Arc::clone(&offset_map),
                            executed_file.path.clone(),
                        )
                        .await;
                }
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
                task_stage: TransferStage::Stock,
                timestamp: 0,
                // current_stock_object_list_file: executed_file.path.clone(),
            };
            if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
                log::error!("{}", e);
            };

            while sys_set.len() > 0 {
                sys_set.join_next().await;
            }
        });

        // match self.attributes.transfer_type{
        //     super::TransferType::Full => todo!(),
        //     super::TransferType::Stock => todo!(),
        //     super::TransferType::Increment => todo!(),
        // }

        // 增量逻辑
        if self.attributes.continuous {
            // if self.attributes.transfer_type.is_full() || self.attributes.transfer_type.is_increment() {
            rt.block_on(async {
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
            });
        }

        Ok(())
    }
}
