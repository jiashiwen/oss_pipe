use super::{osscompare::OssCompare, TaskStatusSaver, TransferTask};
use crate::{
    checkpoint::{get_task_checkpoint, CheckPoint, FileDescription, FilePosition, ListedRecord},
    s3::OSSDescription,
};
use anyhow::{anyhow, Result};
use aws_sdk_s3::model::ObjectIdentifier;
use core::result::Result::Ok;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use snowflake::SnowflakeIdGenerator;
use std::{
    fs::{self, File},
    io::{self, BufRead, Seek, SeekFrom},
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
};
use tokio::{
    runtime::{self},
    task::{self, JoinSet},
};

pub const OBJECT_LIST_FILE_PREFIX: &'static str = "objlist_";
pub const CHECK_POINT_FILE_NAME: &'static str = "checkpoint.yml";
pub const ERROR_RECORD_PREFIX: &'static str = "error_record_";
pub const OFFSET_PREFIX: &'static str = "offset_";
pub const COMPARE_OBJECT_DIFF_PREFIX: &'static str = "diff_object_";
pub const NOTIFY_FILE_PREFIX: &'static str = "notify_";
pub const REMOVED_PREFIX: &'static str = "removed_";
pub const MODIFIED_PREFIX: &'static str = "modified_";

/// 任务阶段，包括存量曾量全量
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TransferStage {
    Stock,
    Increment,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TransferType {
    Full,
    Stock,
    Increment,
}

/// 任务类别，根据传输方式划分
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TaskType {
    Transfer,
    TruncateBucket,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
// #[serde(untagged)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum TaskDescription {
    Transfer(TransferTask),
    OssCompare(TaskOssCompare),
    TruncateBucket(TaskTruncateBucket),
}

// ToDo
// 抽象 task
impl TaskDescription {
    pub fn execute(&self) -> Result<()> {
        match self {
            TaskDescription::Transfer(transfer) => transfer.execute(),
            TaskDescription::TruncateBucket(truncate) => truncate.exec_multi_threads(),
            TaskDescription::OssCompare(oss_compare) => oss_compare.exec_multi_threads(),
        }
    }
}

pub struct TaskDefaultParameters {}

impl TaskDefaultParameters {
    pub fn id_default() -> String {
        task_id_generator().to_string()
    }

    pub fn name_default() -> String {
        "default_name".to_string()
    }

    pub fn batch_size_default() -> i32 {
        100
    }

    pub fn task_threads_default() -> usize {
        num_cpus::get()
    }

    pub fn max_errors_default() -> usize {
        1
    }

    pub fn start_from_checkpoint_default() -> bool {
        false
    }

    pub fn exprirs_diff_scope_default() -> i64 {
        10
    }

    pub fn target_exists_skip_default() -> bool {
        false
    }
    pub fn large_file_size_default() -> usize {
        // 100M
        104857600
    }
    pub fn multi_part_chunk_default() -> usize {
        // 10M
        10485760
    }

    pub fn meta_dir_default() -> String {
        "/tmp/meta_dir".to_string()
    }

    pub fn filter_default() -> Option<Vec<String>> {
        None
    }
    pub fn continuous_default() -> bool {
        false
    }

    pub fn transfer_type_default() -> TransferType {
        TransferType::Stock
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
// #[serde(tag = "type")]
pub struct Task {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub task_desc: TaskDescription,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransferTaskAttributes {
    #[serde(default = "TaskDefaultParameters::batch_size_default")]
    pub bach_size: i32,
    #[serde(default = "TaskDefaultParameters::task_threads_default")]
    pub task_threads: usize,
    #[serde(default = "TaskDefaultParameters::max_errors_default")]
    pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub target_exists_skip: bool,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub start_from_checkpoint: bool,
    #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    pub large_file_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunk_default")]
    pub multi_part_chunk: usize,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::continuous_default")]
    pub continuous: bool,
    #[serde(default = "TaskDefaultParameters::transfer_type_default")]
    pub transfer_type: TransferType,
}

impl Default for TransferTaskAttributes {
    fn default() -> Self {
        Self {
            bach_size: TaskDefaultParameters::batch_size_default(),
            task_threads: TaskDefaultParameters::task_threads_default(),
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
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskTruncateBucket {
    pub oss: OSSDescription,
    #[serde(default = "TaskDefaultParameters::batch_size_default")]
    pub bach_size: i32,
    #[serde(default = "TaskDefaultParameters::task_threads_default")]
    pub task_threads: usize,
    #[serde(default = "TaskDefaultParameters::max_errors_default")]
    pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
}

impl Default for TaskTruncateBucket {
    fn default() -> Self {
        Self {
            bach_size: TaskDefaultParameters::batch_size_default(),
            task_threads: TaskDefaultParameters::task_threads_default(),
            max_errors: TaskDefaultParameters::max_errors_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            oss: OSSDescription::default(),
        }
    }
}
impl TaskTruncateBucket {
    pub fn exec_multi_threads(&self) -> Result<()> {
        let object_list_file = gen_file_path(self.meta_dir.as_str(), OBJECT_LIST_FILE_PREFIX, "");
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .enable_all()
            .build()?;

        // 预清理meta目录
        let _ = fs::remove_dir_all(self.meta_dir.as_str());
        let mut interrupted = false;

        rt.block_on(async {
            let client_source = match self.oss.gen_oss_client() {
                Result::Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    interrupted = true;
                    return;
                }
            };
            if let Err(e) = client_source
                .append_all_object_list_to_file(
                    self.oss.bucket.clone(),
                    self.oss.prefix.clone(),
                    self.bach_size,
                    &object_list_file,
                )
                .await
            {
                log::error!("{}", e);
                interrupted = true;
                return;
            };
        });

        if interrupted {
            return Err(anyhow!("get object list error"));
        }

        let mut set: JoinSet<()> = JoinSet::new();
        let file = File::open(object_list_file.as_str())?;

        rt.block_on(async {
            let mut vec_keys: Vec<ObjectIdentifier> = vec![];

            // 按列表传输object from source to target
            let lines = io::BufReader::new(file).lines();
            for line in lines {
                if let Result::Ok(key) = line {
                    if !key.ends_with("/") {
                        let obj_id = ObjectIdentifier::builder().set_key(Some(key)).build();
                        vec_keys.push(obj_id);
                    }
                };
                if vec_keys.len().to_string().eq(&self.bach_size.to_string()) {
                    while set.len() >= self.task_threads {
                        set.join_next().await;
                    }
                    let c = match self.oss.gen_oss_client() {
                        Ok(c) => c,
                        Err(e) => {
                            log::error!("{}", e);
                            continue;
                        }
                    };
                    let keys = vec_keys.clone();
                    let bucket = self.oss.bucket.clone();
                    set.spawn(async move {
                        if let Err(e) = c.remove_objects(bucket.as_str(), keys).await {
                            log::error!("{}", e);
                        };
                    });

                    vec_keys.clear();
                }
            }

            if vec_keys.len() > 0 {
                while set.len() >= self.task_threads {
                    set.join_next().await;
                }
                let c = match self.oss.gen_oss_client() {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };
                let keys = vec_keys.clone();
                let bucket = self.oss.bucket.clone();
                set.spawn(async move {
                    if let Err(e) = c.remove_objects(bucket.as_str(), keys).await {
                        log::error!("{}", e);
                    };
                });
            }

            if set.len() > 0 {
                set.join_next().await;
            }
        });

        Ok(())
    }
}

pub fn task_id_generator() -> i64 {
    let mut id_generator_generator = SnowflakeIdGenerator::new(1, 1);
    let id = id_generator_generator.real_time_generate();
    id
}

pub fn gen_file_path(dir: &str, file_prefix: &str, file_subffix: &str) -> String {
    let mut file_name = dir.to_string();
    if dir.ends_with("/") {
        file_name.push_str(file_prefix);
        file_name.push_str(file_subffix);
    } else {
        file_name.push_str("/");
        file_name.push_str(file_prefix);
        file_name.push_str(file_subffix);
    }
    file_name
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskOssCompare {
    pub source: OSSDescription,
    pub target: OSSDescription,
    #[serde(default = "TaskDefaultParameters::batch_size_default")]
    pub bach_size: i32,
    #[serde(default = "TaskDefaultParameters::task_threads_default")]
    pub task_threads: usize,
    #[serde(default = "TaskDefaultParameters::max_errors_default")]
    pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    #[serde(default = "TaskDefaultParameters::exprirs_diff_scope_default")]
    pub exprirs_diff_scope: i64,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub start_from_checkpoint: bool,
}

impl Default for TaskOssCompare {
    fn default() -> Self {
        Self {
            source: OSSDescription::default(),
            target: OSSDescription::default(),
            bach_size: TaskDefaultParameters::batch_size_default(),
            task_threads: TaskDefaultParameters::task_threads_default(),
            max_errors: TaskDefaultParameters::max_errors_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            exprirs_diff_scope: TaskDefaultParameters::exprirs_diff_scope_default(),
            start_from_checkpoint: TaskDefaultParameters::start_from_checkpoint_default(),
        }
    }
}

impl TaskOssCompare {
    pub fn exec_multi_threads(&self) -> Result<()> {
        let error_conter = Arc::new(AtomicUsize::new(0));
        let snapshot_stop_mark = Arc::new(AtomicBool::new(false));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());

        // let object_list_file = gen_file_path(self.meta_dir.as_str(), OBJECT_LIST_FILE_PREFIX, "");
        let mut executed_file = FileDescription {
            path: gen_file_path(self.meta_dir.as_str(), OBJECT_LIST_FILE_PREFIX, ""),
            size: 0,
            total_lines: 0,
        };
        let check_point_file = gen_file_path(self.meta_dir.as_str(), CHECK_POINT_FILE_NAME, "");

        let rt = runtime::Builder::new_multi_thread()
            // .worker_threads(self.task_threads)
            .worker_threads(num_cpus::get())
            .enable_all()
            .max_io_events_per_tick(self.task_threads)
            .build()?;

        // 若不从checkpoint开始，重新生成文件清单
        if !self.start_from_checkpoint {
            // 预清理meta目录
            let _ = fs::remove_dir_all(self.meta_dir.as_str());
            let mut interrupted = false;

            rt.block_on(async {
                let client_source = match self.source.gen_oss_client() {
                    Result::Ok(c) => c,
                    Err(e) => {
                        log::error!("{}", e);
                        interrupted = true;
                        return;
                    }
                };
                executed_file = match client_source
                    .append_all_object_list_to_file(
                        self.source.bucket.clone(),
                        self.source.prefix.clone(),
                        self.bach_size,
                        &executed_file.path,
                    )
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        log::error!("{}", e);
                        interrupted = true;
                        return;
                    }
                };
            });

            if interrupted {
                return Err(anyhow!("get object list error"));
            }
        }

        let mut set: JoinSet<()> = JoinSet::new();
        let mut file = File::open(&executed_file.path)?;

        rt.block_on(async {
            let mut file_position = 0;
            let mut vec_keys: Vec<ListedRecord> = vec![];

            if self.start_from_checkpoint {
                let checkpoint =
                    // match get_task_checkpoint(check_point_file.as_str(), self.meta_dir.as_str()) {
                        match get_task_checkpoint(check_point_file.as_str()) {
                        Ok(c) => c,
                        Err(e) => {
                            log::error!("{}", e);
                            return;
                        }
                    };
                let seek_offset =
                    match TryInto::<u64>::try_into(checkpoint.executed_file_position.offset) {
                        Ok(it) => it,
                        Err(e) => {
                            log::error!("{}", e);
                            return;
                        }
                    };
                if let Err(e) = file.seek(SeekFrom::Start(seek_offset)) {
                    log::error!("{}", e);
                    return;
                };
            }

            // 启动checkpoint记录线程
            let status_saver = TaskStatusSaver {
                check_point_path: check_point_file.clone(),
                executed_file: executed_file.clone(),
                stop_mark: Arc::clone(&snapshot_stop_mark),
                list_file_positon_map: Arc::clone(&offset_map),
                file_for_notify: None,
                task_stage: TransferStage::Stock,
                interval: 3,
                current_stock_object_list_file: executed_file.path.clone(),
            };
            task::spawn(async move {
                status_saver.snapshot_to_file().await;
            });

            // 按列表传输object from source to target
            let lines = io::BufReader::new(file).lines();
            let mut line_num = 0;
            for line in lines {
                // 若错误达到上限，则停止任务
                if error_conter.load(std::sync::atomic::Ordering::SeqCst) >= self.max_errors {
                    break;
                }
                if let Result::Ok(key) = line {
                    let len = key.bytes().len() + "\n".bytes().len();
                    file_position += len;
                    line_num += 1;
                    if !key.ends_with("/") {
                        let record = ListedRecord {
                            key,
                            offset: file_position,
                            line_num,
                        };
                        vec_keys.push(record);
                    }
                };

                if vec_keys.len().to_string().eq(&self.bach_size.to_string()) {
                    while set.len() >= self.task_threads {
                        set.join_next().await;
                    }
                    let vk = vec_keys.clone();

                    let compare = OssCompare {
                        source: self.source.clone(),
                        target: self.target.clone(),
                        err_conter: Arc::clone(&error_conter),
                        offset_map: Arc::clone(&offset_map),
                        meta_dir: self.meta_dir.clone(),
                        exprirs_diff_scope: self.exprirs_diff_scope,
                        list_file_path: executed_file.path.clone(),
                    };
                    set.spawn(async move {
                        if let Err(e) = compare.compare(vk).await {
                            compare
                                .err_conter
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            log::error!("{}", e);
                        };
                    });

                    // 清理临时key vec
                    vec_keys.clear();
                }
            }

            // 处理集合中的剩余数据，若错误达到上限，则不执行后续操作
            if vec_keys.len() > 0
                && error_conter.load(std::sync::atomic::Ordering::SeqCst) < self.max_errors
            {
                while set.len() >= self.task_threads {
                    set.join_next().await;
                }

                let vk = vec_keys.clone();

                let compare = OssCompare {
                    source: self.source.clone(),
                    target: self.target.clone(),
                    err_conter: Arc::clone(&error_conter),
                    offset_map: Arc::clone(&offset_map),
                    meta_dir: self.meta_dir.clone(),
                    exprirs_diff_scope: self.exprirs_diff_scope,
                    list_file_path: executed_file.path.clone(),
                };

                set.spawn(async move {
                    if let Err(e) = compare.compare(vk).await {
                        compare
                            .err_conter
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        log::error!("{}", e);
                    };
                });
            }

            while set.len() > 0 {
                set.join_next().await;
            }
            // 配置停止 offset save 标识为 true
            snapshot_stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
            // 记录checkpoint
            let mut checkpoint = CheckPoint {
                executed_file: executed_file.clone(),
                file_for_notify: None,
                task_stage: TransferStage::Stock,
                executed_file_position: FilePosition {
                    offset: file_position,
                    line_num,
                },
                timestampe: 0,
                current_stock_object_list_file: executed_file.path.clone(),
            };
            if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
                log::error!("{}", e);
            };
        });

        Ok(())
    }
}
