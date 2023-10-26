use crate::{
    checkpoint::{get_task_checkpoint, CheckPoint, FilePosition, ListedRecord},
    commons::{
        exec_processbar_with_file_position, json_to_struct, read_lines, read_yaml_file,
        scan_folder_files_to_file,
    },
    osstask::LocalToLocal,
    s3::OSSDescription,
};
use anyhow::{anyhow, Result};
use aws_sdk_s3::model::ObjectIdentifier;
use core::result::Result::Ok;
use dashmap::DashMap;
use indicatif::{ProgressBar, ProgressStyle};
use regex::RegexSet;
use serde::{Deserialize, Serialize};
use snowflake::SnowflakeIdGenerator;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, Seek, SeekFrom},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
        Arc,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
    vec,
};

use super::{
    osscompare::OssCompare,
    task_actions::{TaskActionsFromLocal, TaskActionsFromOss},
    DownloadTask, TaskStatusSaver, TransferTask, UploadTask,
};
use tokio::{
    runtime::{self},
    task::{self, yield_now, JoinSet},
};
use walkdir::WalkDir;

pub const OBJECT_LIST_FILE_PREFIX: &'static str = "objlist_";
pub const CHECK_POINT_FILE_NAME: &'static str = "checkpoint.yml";
pub const ERROR_RECORD_PREFIX: &'static str = "error_record_";
pub const OFFSET_PREFIX: &'static str = "offset_";
pub const OFFSET_EXEC_PREFIX: &'static str = "offset_exec_";
pub const COMPARE_OBJECT_DIFF_PREFIX: &'static str = "diff_object_";
pub const CURRENT_LINE_PREFIX: &'static str = "current_line_";
pub const NOTIFY_FILE_PREFIX: &'static str = "notify_file_";

/// 任务类型，包括存量曾量全量
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TaskRunningStatus {
    Stock,
    Increment,
}

/// 任务类别，根据传输方式划分
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TaskType {
    Download,
    Upload,
    Transfer,
    LocalToLocal,
    TruncateBucket,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
// #[serde(untagged)]
pub enum TaskDescription {
    // Download(TaskDownload),
    Download(DownloadTask),
    // Upload(TaskUpLoad),
    Upload(UploadTask),
    // Transfer(TaskTransfer),
    Transfer(TransferTask),
    LocalToLocal(TaskLocalToLocal),
    OssCompare(TaskOssCompare),
    TruncateBucket(TaskTruncateBucket),
}

// ToDo
// 抽象 task
impl TaskDescription {
    pub fn exec_multi_threads(&self) -> Result<()> {
        match self {
            TaskDescription::Download(download) => {
                execute_task_from_oss_multi_threads(true, download, &download.task_attributes)
            }
            // TaskDescription::Upload(upload) => upload.exec_multi_threads(),
            TaskDescription::Upload(upload) => {
                execute_task_from_local_multi_threads(true, upload, &upload.task_attributes)
            }
            TaskDescription::Transfer(transfer) => {
                execute_task_from_oss_multi_threads(true, transfer, &transfer.task_attributes)
            }
            TaskDescription::LocalToLocal(local_to_local) => local_to_local.exec_multi_threads(),
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

    pub fn error_counter() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(0))
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Task {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub task_desc: TaskDescription,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskAttributes {
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
}

impl Default for TaskAttributes {
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
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskLocalToLocal {
    pub source_path: String,
    pub target_path: String,
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
}

impl Default for TaskLocalToLocal {
    fn default() -> Self {
        Self {
            source_path: "/tmp/source".to_string(),
            target_path: "/tmp/target".to_string(),
            bach_size: TaskDefaultParameters::batch_size_default(),
            task_threads: TaskDefaultParameters::task_threads_default(),
            max_errors: TaskDefaultParameters::max_errors_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            target_exists_skip: TaskDefaultParameters::target_exists_skip_default(),
            start_from_checkpoint: TaskDefaultParameters::start_from_checkpoint_default(),
            large_file_size: TaskDefaultParameters::large_file_size_default(),
            multi_part_chunk: TaskDefaultParameters::multi_part_chunk_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
        }
    }
}

impl TaskLocalToLocal {
    pub fn exec_multi_threads(&self) -> Result<()> {
        let error_times = Arc::new(AtomicUsize::new(0));
        let snapshot_stop_mark = Arc::new(AtomicBool::new(false));
        // let offset_map = Arc::new(DashMap::<String, usize>::new());
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
        let object_list_file = gen_file_path(self.meta_dir.as_str(), OBJECT_LIST_FILE_PREFIX, "");
        let check_point_file = gen_file_path(self.meta_dir.as_str(), CHECK_POINT_FILE_NAME, "");

        let mut exclude_regex_set: Option<RegexSet> = None;
        let mut include_regex_set: Option<RegexSet> = None;
        if let Some(vec_regex_str) = self.exclude.clone() {
            let set = RegexSet::new(&vec_regex_str)?;
            exclude_regex_set = Some(set);
        };
        if let Some(vec_regex_str) = self.include.clone() {
            let set = RegexSet::new(&vec_regex_str)?;
            include_regex_set = Some(set);
        };

        // 若不从checkpoint开始，重新生成文件清单
        if !self.start_from_checkpoint {
            // 预清理meta目录
            let _ = fs::remove_dir_all(self.meta_dir.as_str());
            scan_folder_files_to_file(self.source_path.as_str(), object_list_file.as_str())?;
        }
        let mut set: JoinSet<()> = JoinSet::new();
        let mut file = File::open(object_list_file.as_str())?;

        let mut file_position: usize = 0;
        if self.start_from_checkpoint {
            // 执行错误补偿，重新执行错误日志中的记录
            self.error_retry()?;
            // let checkpoint = get_task_checkpoint(CHECK_POINT_FILE_NAME, self.meta_dir.as_str())?;
            let checkpoint = get_task_checkpoint(CHECK_POINT_FILE_NAME)?;
            let seek_offset = TryInto::<u64>::try_into(checkpoint.execute_file_position.offset)?;
            file.seek(SeekFrom::Start(seek_offset))?;
            file_position = checkpoint.execute_file_position.offset;
        }

        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(self.task_threads)
            .enable_all()
            .build()?;

        rt.block_on(async {
            let map = Arc::clone(&offset_map);
            let stop_mark = Arc::clone(&snapshot_stop_mark);
            let mut vec_keys: Vec<ListedRecord> = vec![];
            let obj_list = object_list_file.clone();

            let status_saver = TaskStatusSaver {
                save_to: check_point_file.clone(),
                execute_file_path: object_list_file.clone(),
                stop_mark: Arc::clone(&snapshot_stop_mark),
                list_file_positon_map: Arc::clone(&offset_map),
                file_for_notify: None,
                task_running_status: TaskRunningStatus::Stock,
                interval: 3,
            };
            task::spawn(async move {
                status_saver.snapshot_to_file().await;
            });
            // 按列表传输object from source to target
            let lines = io::BufReader::new(file).lines();
            let mut line_num = 0;
            for line in lines {
                // 若错误达到上限，则停止任务
                if error_times.load(std::sync::atomic::Ordering::SeqCst) >= self.max_errors {
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
                        match exclude_regex_set {
                            Some(ref exclude) => {
                                if exclude.is_match(&record.key) {
                                    continue;
                                }
                            }
                            None => {}
                        }
                        match include_regex_set {
                            Some(ref set) => {
                                if set.is_match(&record.key) {
                                    vec_keys.push(record);
                                }
                            }
                            None => {
                                vec_keys.push(record);
                            }
                        }
                    }
                };

                if vec_keys.len().to_string().eq(&self.bach_size.to_string()) {
                    while set.len() >= self.task_threads {
                        set.join_next().await;
                    }
                    let vk = vec_keys.clone();

                    let localtolocal = LocalToLocal {
                        source_path: self.source_path.clone(),
                        target_path: self.target_path.clone(),
                        error_conter: Arc::clone(&error_times),
                        meta_dir: self.meta_dir.clone(),
                        target_exist_skip: self.target_exists_skip,
                        large_file_size: self.large_file_size,
                        multi_part_chunk: self.multi_part_chunk,
                        offset_map: Arc::clone(&offset_map),
                        list_file_path: object_list_file.clone(),
                    };
                    // let pre_offset = pre_batch_last_offset;
                    set.spawn(async move {
                        if let Err(e) = localtolocal.exec(vk).await {
                            localtolocal
                                .error_conter
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            log::error!("{}", e);
                        };
                    });

                    vec_keys.clear();
                }
            }

            // 若错误达到上限，则不执行后续操作
            if vec_keys.len() > 0
                && error_times.load(std::sync::atomic::Ordering::SeqCst) < self.max_errors
            {
                while set.len() >= self.task_threads {
                    set.join_next().await;
                }

                let vk = vec_keys.clone();

                let localtolocal = LocalToLocal {
                    source_path: self.source_path.clone(),
                    target_path: self.target_path.clone(),
                    error_conter: Arc::clone(&error_times),
                    meta_dir: self.meta_dir.clone(),
                    target_exist_skip: false,
                    large_file_size: self.large_file_size,
                    multi_part_chunk: self.multi_part_chunk,
                    offset_map: Arc::clone(&offset_map),
                    list_file_path: object_list_file.clone(),
                };

                set.spawn(async move {
                    if let Err(e) = localtolocal.exec(vk).await {
                        localtolocal
                            .error_conter
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        log::error!("{}", e);
                    };
                });
            }

            while set.len() > 0 {
                set.join_next().await;
            }
            snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
            // 记录checkpoint
            let list_file = object_list_file.as_str();
            let checkpoint = CheckPoint {
                execute_file_path: list_file.to_string(),
                execute_file_position: FilePosition {
                    offset: file_position,
                    line_num,
                },
                file_for_notify: None,
                task_running_satus: TaskRunningStatus::Stock,
            };
            if let Err(e) = checkpoint.save_to(list_file) {
                log::error!("{}", e);
            };
        });
        Ok(())
    }

    pub fn error_retry(&self) -> Result<()> {
        // 遍历错误记录
        for entry in WalkDir::new(self.meta_dir.as_str())
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
                        let copy = LocalToLocal {
                            source_path: self.source_path.clone(),
                            target_path: self.target_path.clone(),
                            error_conter: Arc::new(AtomicUsize::new(0)),
                            meta_dir: self.meta_dir.clone(),
                            target_exist_skip: self.target_exists_skip,
                            large_file_size: self.large_file_size,
                            multi_part_chunk: self.multi_part_chunk,
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            list_file_path: p.to_string(),
                        };
                        let _ = copy.exec(record_vec);
                    }
                }

                let _ = fs::remove_file(p);
            }
        }

        Ok(())
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
                    object_list_file.clone(),
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

        let object_list_file = gen_file_path(self.meta_dir.as_str(), OBJECT_LIST_FILE_PREFIX, "");
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
                if let Err(e) = client_source
                    .append_all_object_list_to_file(
                        self.source.bucket.clone(),
                        self.source.prefix.clone(),
                        self.bach_size,
                        object_list_file.clone(),
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
        }

        let mut set: JoinSet<()> = JoinSet::new();
        let mut file = File::open(object_list_file.as_str())?;

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
                    match TryInto::<u64>::try_into(checkpoint.execute_file_position.offset) {
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
            // let map = Arc::clone(&offset_map);
            // let stop_mark = Arc::clone(&snapshot_stop_mark);
            // let obj_list = object_list_file.clone();
            // let save_to = check_point_file.clone();
            // task::spawn(async move {
            //     snapshot_offset_to_file(
            //         save_to.as_str(),
            //         obj_list,
            //         stop_mark,
            //         map,
            //         None,
            //         TaskRunningStatus::Stock,
            //         3,
            //     )
            //     .await
            // });

            let status_saver = TaskStatusSaver {
                save_to: check_point_file.clone(),
                execute_file_path: object_list_file.clone(),
                stop_mark: Arc::clone(&snapshot_stop_mark),
                list_file_positon_map: Arc::clone(&offset_map),
                file_for_notify: None,
                task_running_status: TaskRunningStatus::Stock,
                interval: 3,
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
                        error_conter: Arc::clone(&error_conter),
                        offset_map: Arc::clone(&offset_map),
                        meta_dir: self.meta_dir.clone(),
                        exprirs_diff_scope: self.exprirs_diff_scope,
                    };
                    set.spawn(async move {
                        if let Err(e) = compare.compare(vk).await {
                            compare
                                .error_conter
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
                    error_conter: Arc::clone(&error_conter),
                    offset_map: Arc::clone(&offset_map),
                    meta_dir: self.meta_dir.clone(),
                    exprirs_diff_scope: self.exprirs_diff_scope,
                };

                set.spawn(async move {
                    if let Err(e) = compare.compare(vk).await {
                        compare
                            .error_conter
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
            let checkpoint = CheckPoint {
                execute_file_path: object_list_file.clone(),
                file_for_notify: None,
                task_running_satus: TaskRunningStatus::Stock,
                execute_file_position: FilePosition {
                    offset: file_position,
                    line_num,
                },
            };
            if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
                log::error!("{}", e);
            };
        });

        Ok(())
    }
}

pub fn execute_task_from_oss_multi_threads<T>(
    init: bool,
    task: &T,
    task_attributes: &TaskAttributes,
) -> Result<()>
where
    T: TaskActionsFromOss,
{
    println!("beging execut");
    // 执行 object_list 文件中行的总数
    let mut total_lines: usize = 0;
    // 执行过程中错误数统计
    let error_conter = Arc::new(AtomicUsize::new(0));
    // 任务停止标准，用于通知所有协程任务结束
    let snapshot_stop_mark = Arc::new(AtomicBool::new(false));
    let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let mut last_modify_timestamp = 0;

    let object_list_file = gen_file_path(
        task_attributes.meta_dir.as_str(),
        OBJECT_LIST_FILE_PREFIX,
        now.as_secs().to_string().as_str(),
    );

    let check_point_file =
        gen_file_path(task_attributes.meta_dir.as_str(), CHECK_POINT_FILE_NAME, "");

    // 持续同步逻辑: 执行完成的object list 文件的时间戳，该时间戳为执行起始时间，由此确定凡事last modify 大于该时间戳的对象属于新增对象
    if task_attributes.continuous {
        match read_yaml_file::<CheckPoint>(check_point_file.as_str()) {
            Ok(checkpoint) => {
                let v = checkpoint
                    .execute_file_path
                    .split("_")
                    .collect::<Vec<&str>>();

                if let Some(s) = v.last() {
                    match s.parse::<i64>() {
                        Ok(i) => last_modify_timestamp = i,
                        Err(_) => {}
                    }
                }
            }
            Err(_) => {}
        }
    }
    let mut exclude_regex_set: Option<RegexSet> = None;
    let mut include_regex_set: Option<RegexSet> = None;

    if let Some(vec_regex_str) = task_attributes.exclude.clone() {
        let set = RegexSet::new(&vec_regex_str)?;
        exclude_regex_set = Some(set);
    };

    if let Some(vec_regex_str) = task_attributes.include.clone() {
        let set = RegexSet::new(&vec_regex_str)?;
        include_regex_set = Some(set);
    };

    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .enable_all()
        .max_io_events_per_tick(task_attributes.task_threads)
        .build()?;

    let task_running_status = match init {
        true => TaskRunningStatus::Stock,
        false => TaskRunningStatus::Increment,
    };

    // 若不从checkpoint开始，重新生成文件清单
    if !task_attributes.start_from_checkpoint {
        // 预清理meta目录,任务首次运行清理meta目录
        if init {
            let _ = fs::remove_dir_all(task_attributes.meta_dir.as_str());
        }
        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(Duration::from_millis(120));
        pb.set_style(
            ProgressStyle::with_template("{spinner:.green} {msg}")
                .unwrap()
                .tick_strings(&[
                    "▰▱▱▱▱▱▱",
                    "▰▰▱▱▱▱▱",
                    "▰▰▰▱▱▱▱",
                    "▰▰▰▰▱▱▱",
                    "▰▰▰▰▰▱▱",
                    "▰▰▰▰▰▰▱",
                    "▰▰▰▰▰▰▰",
                    "▰▱▱▱▱▱▱",
                ]),
        );
        pb.set_message("Generating object list ...");

        match task.generate_object_list(&rt, last_modify_timestamp, object_list_file.as_str()) {
            Ok(lines) => {
                total_lines = lines;
                pb.finish_with_message("object list Done");
            }
            Err(e) => {
                pb.finish_with_message("object list Fail");
                return Err(e);
            }
        }

        // 若 返回值为0，证明没有符合条件的object，同步任务结束
        if total_lines.eq(&0) {
            return Ok(());
        }
    }

    let mut sys_set = JoinSet::new();
    let mut execut_set: JoinSet<()> = JoinSet::new();
    let mut file = File::open(object_list_file.as_str())?;

    rt.block_on(async {
        let mut file_position = 0;
        let mut vec_keys: Vec<ListedRecord> = vec![];

        if task_attributes.start_from_checkpoint {
            // 执行错误补偿，重新执行错误日志中的记录
            match task.error_record_retry() {
                Ok(_) => {}
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            let checkpoint = match get_task_checkpoint(check_point_file.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };
            let seek_offset =
                match TryInto::<u64>::try_into(checkpoint.execute_file_position.offset) {
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
            save_to: check_point_file.clone(),
            execute_file_path: object_list_file.clone(),
            stop_mark: Arc::clone(&snapshot_stop_mark),
            list_file_positon_map: Arc::clone(&offset_map),
            file_for_notify: None,
            task_running_status: TaskRunningStatus::Stock,
            interval: 3,
        };
        sys_set.spawn(async move {
            status_saver.snapshot_to_file().await;
        });

        // 启动进度条线程
        let map = Arc::clone(&offset_map);
        let stop_mark = Arc::clone(&snapshot_stop_mark);
        let total = TryInto::<u64>::try_into(total_lines).unwrap();
        sys_set.spawn(async move {
            exec_processbar_with_file_position(total, stop_mark, map, CURRENT_LINE_PREFIX).await;
        });

        // 按列表传输object from source to target
        let lines = io::BufReader::new(file).lines();
        let mut line_num = 0;
        for line in lines {
            // 若错误达到上限，则停止任务
            if error_conter.load(std::sync::atomic::Ordering::SeqCst) >= task_attributes.max_errors
            {
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
                    match exclude_regex_set {
                        Some(ref exclude) => {
                            if exclude.is_match(&record.key) {
                                continue;
                            }
                        }
                        None => {}
                    }
                    match include_regex_set {
                        Some(ref set) => {
                            if set.is_match(&record.key) {
                                vec_keys.push(record);
                            }
                        }
                        None => {
                            vec_keys.push(record);
                        }
                    }
                }
            };

            if vec_keys
                .len()
                .to_string()
                .eq(&task_attributes.bach_size.to_string())
            {
                while execut_set.len() >= task_attributes.task_threads {
                    execut_set.join_next().await;
                }
                let vk = vec_keys.clone();

                task.records_excutor(
                    &mut execut_set,
                    vk,
                    Arc::clone(&error_conter),
                    Arc::clone(&offset_map),
                    object_list_file.clone(),
                )
                .await;

                // 清理临时key vec
                vec_keys.clear();
            }
        }

        // 处理集合中的剩余数据，若错误达到上限，则不执行后续操作
        if vec_keys.len() > 0
            && error_conter.load(std::sync::atomic::Ordering::SeqCst) < task_attributes.max_errors
        {
            while execut_set.len() >= task_attributes.task_threads {
                execut_set.join_next().await;
            }

            let vk = vec_keys.clone();
            task.records_excutor(
                &mut execut_set,
                vk,
                Arc::clone(&error_conter),
                Arc::clone(&offset_map),
                object_list_file.clone(),
            )
            .await;
        }

        while execut_set.len() > 0 {
            execut_set.join_next().await;
        }
        // 配置停止 offset save 标识为 true
        snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
        // 记录checkpoint
        // let position: u64 = file_position.try_into().unwrap();
        let checkpoint = CheckPoint {
            execute_file_path: object_list_file.clone(),
            execute_file_position: FilePosition {
                offset: file_position,
                line_num,
            },
            file_for_notify: None,
            task_running_satus: task_running_status,
        };
        if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
            log::error!("{}", e);
        };
    });

    if task_attributes.continuous {
        execute_task_from_oss_multi_threads(false, task, task_attributes)?;
    }

    Ok(())
}

pub fn execute_task_from_local_multi_threads<T>(
    init: bool,
    task: &T,
    task_attributes: &TaskAttributes,
) -> Result<()>
where
    T: TaskActionsFromLocal,
{
    // 执行 object_list 文件中行的总数
    let mut total_lines: usize = 0;
    // 执行过程中错误数统计
    let error_conter = Arc::new(AtomicUsize::new(0));
    // 任务停止标准，用于通知所有协程任务结束
    let snapshot_stop_mark = Arc::new(AtomicBool::new(false));
    let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    // 增量任务计数器，用于记录notify file 的文件大小
    let notify_file_size = Arc::new(AtomicU64::new(0));

    let object_list_file = gen_file_path(
        task_attributes.meta_dir.as_str(),
        OBJECT_LIST_FILE_PREFIX,
        now.as_secs().to_string().as_str(),
    );

    let notify_file = match task_attributes.continuous {
        true => Some(gen_file_path(
            task_attributes.meta_dir.as_str(),
            NOTIFY_FILE_PREFIX,
            now.as_secs().to_string().as_str(),
        )),
        false => None,
    };

    let check_point_file =
        gen_file_path(task_attributes.meta_dir.as_str(), CHECK_POINT_FILE_NAME, "");

    let mut exclude_regex_set: Option<RegexSet> = None;
    let mut include_regex_set: Option<RegexSet> = None;

    if let Some(vec_regex_str) = task_attributes.exclude.clone() {
        let set = RegexSet::new(&vec_regex_str)?;
        exclude_regex_set = Some(set);
    };

    if let Some(vec_regex_str) = task_attributes.include.clone() {
        let set = RegexSet::new(&vec_regex_str)?;
        include_regex_set = Some(set);
    };

    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .enable_all()
        .max_io_events_per_tick(task_attributes.task_threads)
        .build()?;

    // 若不从checkpoint开始，重新生成文件清单
    if !task_attributes.start_from_checkpoint {
        // 预清理meta目录,任务首次运行清理meta目录,并遍历本地目录生成 object list
        if init {
            let _ = fs::remove_dir_all(task_attributes.meta_dir.as_str());

            let pb = ProgressBar::new_spinner();
            pb.enable_steady_tick(Duration::from_millis(120));
            pb.set_style(
                ProgressStyle::with_template("{spinner:.green} {msg}")
                    .unwrap()
                    .tick_strings(&[
                        "▰▱▱▱▱▱▱",
                        "▰▰▱▱▱▱▱",
                        "▰▰▰▱▱▱▱",
                        "▰▰▰▰▱▱▱",
                        "▰▰▰▰▰▱▱",
                        "▰▰▰▰▰▰▱",
                        "▰▰▰▰▰▰▰",
                        "▰▱▱▱▱▱▱",
                    ]),
            );
            pb.set_message("Generating object list ...");
            match task.generate_object_list(&rt, object_list_file.as_str()) {
                Ok(lines) => {
                    total_lines = lines;
                    pb.finish_with_message("object list Done");
                }
                Err(e) => {
                    pb.finish_with_message("object list Fail");
                    return Err(e);
                }
            }

            // 若 返回值为0，证明没有符合条件的object，同步任务结束
            if total_lines.eq(&0) {
                return Ok(());
            }
        }
    }

    // sys_set 用于执行checkpoint、notify等辅助任务
    let mut sys_set = JoinSet::new();
    // execut_set 用于执行任务
    let mut execut_set: JoinSet<()> = JoinSet::new();
    let mut file = File::open(object_list_file.as_str())?;

    rt.block_on(async {
        let mut file_position = 0;
        let mut vec_keys: Vec<ListedRecord> = vec![];

        if task_attributes.start_from_checkpoint {
            // 执行错误补偿，重新执行错误日志中的记录
            match task.error_record_retry() {
                Ok(_) => {}
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            // let checkpoint = match get_task_checkpoint(
            //     check_point_file.as_str(),
            //     task_attributes.meta_dir.as_str(),
            // ) {
            let checkpoint = match get_task_checkpoint(check_point_file.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };
            let seek_offset =
                match TryInto::<u64>::try_into(checkpoint.execute_file_position.offset) {
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
            save_to: check_point_file.clone(),
            execute_file_path: object_list_file.clone(),
            stop_mark: Arc::clone(&snapshot_stop_mark),
            list_file_positon_map: Arc::clone(&offset_map),
            file_for_notify: notify_file.clone(),
            task_running_status: TaskRunningStatus::Stock,
            interval: 3,
        };
        sys_set.spawn(async move {
            status_saver.snapshot_to_file().await;
        });

        // 持续同步逻辑: 启动本地目录notify 线程
        if let Some(file) = notify_file.clone() {
            let notify = match task.gen_watcher() {
                Ok(n) => n,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };
            let watched_file_size = Arc::clone(&notify_file_size);
            // sys_set.spawn(async move {
            task::spawn(async move {
                let file_for_notify = match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(file.as_str())
                {
                    Ok(f) => f,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };
                notify
                    .watch_to_file(file_for_notify, watched_file_size)
                    .await
            });
        };

        // 启动进度条线程
        let map = Arc::clone(&offset_map);
        let stop_mark = Arc::clone(&snapshot_stop_mark);
        let total = TryInto::<u64>::try_into(total_lines).unwrap();
        sys_set.spawn(async move {
            // Todo 调整进度条
            // exec_processbar(total, stop_mark, map, CURRENT_LINE_PREFIX).await;
            exec_processbar_with_file_position(total, stop_mark, map, OFFSET_PREFIX).await;
        });

        // 按列表传输object from source to target
        let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(file).lines();
        let mut line_num = 0;
        for line in lines {
            // 若错误达到上限，则停止任务
            if error_conter.load(std::sync::atomic::Ordering::SeqCst) >= task_attributes.max_errors
            {
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
                    match exclude_regex_set {
                        Some(ref exclude) => {
                            if exclude.is_match(&record.key) {
                                continue;
                            }
                        }
                        None => {}
                    }
                    match include_regex_set {
                        Some(ref set) => {
                            if set.is_match(&record.key) {
                                vec_keys.push(record);
                            }
                        }
                        None => {
                            vec_keys.push(record);
                        }
                    }
                }
            };

            if vec_keys
                .len()
                .to_string()
                .eq(&task_attributes.bach_size.to_string())
            {
                while execut_set.len() >= task_attributes.task_threads {
                    execut_set.join_next().await;
                }
                let vk = vec_keys.clone();
                task.records_excutor(
                    &mut execut_set,
                    vk,
                    Arc::clone(&error_conter),
                    Arc::clone(&offset_map),
                    object_list_file.clone(),
                )
                .await;

                // 清理临时key vec
                vec_keys.clear();
            }
        }

        // 处理集合中的剩余数据，若错误达到上限，则不执行后续操作
        if vec_keys.len() > 0
            && error_conter.load(std::sync::atomic::Ordering::SeqCst) < task_attributes.max_errors
        {
            while execut_set.len() >= task_attributes.task_threads {
                execut_set.join_next().await;
            }

            let vk = vec_keys.clone();
            task.records_excutor(
                &mut execut_set,
                vk,
                Arc::clone(&error_conter),
                Arc::clone(&offset_map),
                object_list_file.clone(),
            )
            .await;
        }

        while execut_set.len() > 0 {
            execut_set.join_next().await;
        }
        // 配置停止 offset save 标识为 true
        snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
        // 记录checkpoint
        let checkpoint = CheckPoint {
            execute_file_path: object_list_file.clone(),
            execute_file_position: FilePosition {
                offset: file_position,
                line_num,
            },
            file_for_notify: notify_file.clone(),
            task_running_satus: TaskRunningStatus::Stock,
        };
        if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
            log::error!("{}", e);
        };

        while sys_set.len() > 0 {
            sys_set.join_next().await;
        }

        if task_attributes.continuous {
            // let checkpoint = match get_task_checkpoint(
            //     check_point_file.as_str(),
            //     task_attributes.meta_dir.as_str(),
            // ) {
            let checkpoint = match get_task_checkpoint(check_point_file.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    return;
                }
            };

            match checkpoint.file_for_notify {
                Some(f) => {
                    let stop_mark = Arc::new(AtomicBool::new(false));
                    let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
                    // 执行过程中错误数统计
                    // let err_conter = Arc::clone(&error_conter);
                    let increment_file_size = Arc::clone(&notify_file_size);

                    let task_status_saver = TaskStatusSaver {
                        save_to: check_point_file.clone(),
                        execute_file_path: notify_file.clone().unwrap(),
                        stop_mark: Arc::clone(&stop_mark),
                        list_file_positon_map: Arc::clone(&offset_map),
                        file_for_notify: notify_file.clone(),
                        task_running_status: TaskRunningStatus::Increment,
                        interval: 3,
                    };
                    sys_set.spawn(async move {
                        task_status_saver.snapshot_to_file().await;
                    });
                    let _ = task
                        .execute_increment(
                            f.as_str(),
                            increment_file_size,
                            Arc::clone(&error_conter),
                            Arc::clone(&offset_map),
                            Arc::clone(&stop_mark),
                        )
                        .await;
                    // 配置停止 offset save 标识为 true
                    snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
                }
                None => {
                    let err = anyhow!("notify_file is none");
                    log::error!("{}", err);
                    return;
                }
            }
        }
    });

    Ok(())
}
