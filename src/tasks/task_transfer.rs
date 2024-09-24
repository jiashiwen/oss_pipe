use super::{
    de_usize_from_str, gen_file_path, se_usize_to_str, task_delete, TaskDefaultParameters,
    TaskStatusSaver, TransferStage, OFFSET_PREFIX, TRANSFER_CHECK_POINT_FILE,
    TRANSFER_OBJECT_LIST_FILE_PREFIX,
};
use super::{
    task_actions::TransferTaskActions, IncrementAssistant, TransferLocal2Local, TransferLocal2Oss,
    TransferOss2Local, TransferOss2Oss,
};
use crate::checkpoint::RecordDescription;
use crate::commons::{json_to_struct, LastModifyFilter};
use crate::{
    checkpoint::FileDescription, commons::prompt_processbar, s3::OSSDescription,
    tasks::NOTIFY_FILE_PREFIX,
};
use crate::{
    checkpoint::{get_task_checkpoint, FilePosition, ListedRecord},
    commons::quantify_processbar,
};
use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

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

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
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

    #[allow(dead_code)]
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

// ToDo 规范属性名称
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransferTaskAttributes {
    #[serde(default = "TaskDefaultParameters::objects_per_batch_default")]
    pub objects_per_batch: i32,
    #[serde(default = "TaskDefaultParameters::task_parallelism_default")]
    pub task_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::max_errors_default")]
    pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub target_exists_skip: bool,
    #[serde(default = "TaskDefaultParameters::start_from_checkpoint_default")]
    pub start_from_checkpoint: bool,
    #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub large_file_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunk_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub multi_part_chunk_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunks_per_batch_default")]
    pub multi_part_chunks_per_batch: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_parallelism_default")]
    pub multi_part_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::transfer_type_default")]
    pub transfer_type: TransferType,
    #[serde(default = "TaskDefaultParameters::last_modify_filter_default")]
    pub last_modify_filter: Option<LastModifyFilter>,
}

impl Default for TransferTaskAttributes {
    fn default() -> Self {
        Self {
            objects_per_batch: TaskDefaultParameters::objects_per_batch_default(),
            task_parallelism: TaskDefaultParameters::task_parallelism_default(),
            max_errors: TaskDefaultParameters::max_errors_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            target_exists_skip: TaskDefaultParameters::target_exists_skip_default(),
            start_from_checkpoint: TaskDefaultParameters::target_exists_skip_default(),
            large_file_size: TaskDefaultParameters::large_file_size_default(),
            multi_part_chunk_size: TaskDefaultParameters::multi_part_chunk_size_default(),
            multi_part_chunks_per_batch: TaskDefaultParameters::multi_part_chunks_per_batch_default(
            ),
            multi_part_parallelism: TaskDefaultParameters::multi_part_parallelism_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
            transfer_type: TaskDefaultParameters::transfer_type_default(),
            last_modify_filter: TaskDefaultParameters::last_modify_filter_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferTask {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: ObjectStorage,
    pub target: ObjectStorage,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferTask {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: ObjectStorage::OSS(OSSDescription::default()),
            target: ObjectStorage::OSS(OSSDescription::default()),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

impl TransferTask {
    // pub fn gen_transfer_actions(&self) -> Box<dyn TransferTaskActions + Send + Sync> {
    pub fn gen_transfer_actions(&self) -> Arc<dyn TransferTaskActions + Send + Sync> {
        match &self.source {
            ObjectStorage::Local(path_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferLocal2Local {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: path_s.to_string(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    // Box::new(t)
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferLocal2Oss {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: path_s.to_string(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    // Box::new(t)
                    Arc::new(t)
                }
            },
            ObjectStorage::OSS(oss_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferOss2Local {
                        source: oss_s.clone(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    // Box::new(t)
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferOss2Oss {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: oss_s.clone(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    // Box::new(t)
                    Arc::new(t)
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
                    log::error!("{:?}", e);
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

            builder.insert_record(0, header);

            let mut table = builder.build();
            table.with(Style::ascii_rounded());
            println!("{}", table);
        });
        Ok(())
    }

    pub fn start_task(&self) -> Result<()> {
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .enable_all()
            .max_io_events_per_tick(self.attributes.task_parallelism)
            .build()?;
        // sys_set 用于执行checkpoint、notify等辅助任务
        let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        let mut exec_set = JoinSet::new();
        // 正在执行的任务数量，用于控制分片上传并行度
        let task = self.gen_transfer_actions();

        // 执行过程中错误数统计
        let err_counter = Arc::new(AtomicUsize::new(0));
        // 任务停止标准，用于通知所有协程任务结束
        let stop_mark = Arc::new(AtomicBool::new(false));
        let task_err_occur = Arc::new(AtomicBool::new(false));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());

        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            TRANSFER_CHECK_POINT_FILE,
            "",
        );

        let mut assistant = IncrementAssistant::default();
        assistant.check_point_path = check_point_file.clone();
        let increment_assistant = Arc::new(Mutex::new(assistant));
        let err_occur = task_err_occur.clone();
        rt.block_on(async {
            //获取 对象列表文件，列表文件描述，increame_start_from_checkpoint 标识
            let (
                object_list_file,
                executed_file,
                list_file_position,
                start_from_checkpoint_stage_is_increment,
            ) = match self.generate_list_file(stop_mark.clone(), task).await {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{}", e);
                    err_occur.store(true, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
            };

            // 全量同步时: 执行增量助理
            let mut file_for_notify = None;
            let task_increment_prelude = self.gen_transfer_actions();

            // 增量或全量同步时，提前启动 increment_assistant 用于记录 notify 文件
            if self.attributes.transfer_type.is_full()
                || self.attributes.transfer_type.is_increment()
            {
                let assistant = Arc::clone(&increment_assistant);
                let e_o = err_occur.clone();
                task::spawn(async move {
                    if let Err(e) = task_increment_prelude.increment_prelude(assistant).await {
                        log::error!("{:?}", e);
                        e_o.store(true, std::sync::atomic::Ordering::Relaxed);
                        return;
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

            // start from checkpoint 且 stage 为 increment
            match start_from_checkpoint_stage_is_increment {
                true => {
                    self.exec_record_descriptions_file(
                        stop_mark.clone(),
                        err_occur.clone(),
                        err_counter.clone(),
                        &mut exec_set,
                        offset_map.clone(),
                        // list_file_position,
                        object_list_file,
                        executed_file,
                    )
                    .await;
                }
                false => {
                    // 存量或全量场景时，执行对象列表文件
                    match self.attributes.transfer_type {
                        TransferType::Full | TransferType::Stock => {
                            // 启动checkpoint记录线程
                            let stock_status_saver = TaskStatusSaver {
                                check_point_path: check_point_file.clone(),
                                executed_file: executed_file.clone(),
                                stop_mark: Arc::clone(&stop_mark),
                                list_file_positon_map: Arc::clone(&offset_map),
                                file_for_notify,
                                task_stage: TransferStage::Stock,
                                interval: 3,
                            };
                            let task_id = self.task_id.clone();
                            sys_set.spawn(async move {
                                stock_status_saver.snapshot_to_file(task_id).await;
                            });

                            // 启动进度条线程
                            let map = Arc::clone(&offset_map);
                            let bar_stop_mark = Arc::clone(&stop_mark);
                            let total = executed_file.total_lines;
                            let cp = check_point_file.clone();
                            sys_set.spawn(async move {
                                // Todo 调整进度条
                                quantify_processbar(total, bar_stop_mark, map, &cp, OFFSET_PREFIX)
                                    .await;
                            });

                            self.exec_records_file(
                                stop_mark.clone(),
                                err_occur.clone(),
                                err_counter.clone(),
                                &mut exec_set,
                                offset_map.clone(),
                                list_file_position,
                                object_list_file,
                                executed_file,
                            )
                            .await;
                        }
                        TransferType::Increment => {}
                    }
                }
            }

            while exec_set.len() > 0 {
                exec_set.join_next().await;
            }

            // 配置停止 offset save 标识为 true
            stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
            let lock = increment_assistant.lock().await;
            let notify = lock.get_notify_file_path();
            drop(lock);

            let mut checkpoint = match get_task_checkpoint(check_point_file.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };
            checkpoint.file_for_notify = notify;

            if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
                log::error!("{:?}", e);
            };

            while sys_set.len() > 0 {
                task::yield_now().await;
                sys_set.join_next().await;
            }

            if task_err_occur.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }

            if self.attributes.transfer_type.is_full()
                || self.attributes.transfer_type.is_increment()
            {
                let stop_mark = Arc::new(AtomicBool::new(false));
                let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
                let executing_transfers = Arc::new(RwLock::new(0));
                let task_increment = self.gen_transfer_actions();

                task_increment
                    .execute_increment(
                        Arc::clone(&stop_mark),
                        err_occur.clone(),
                        Arc::clone(&err_counter),
                        &mut exec_set,
                        executing_transfers,
                        Arc::clone(&increment_assistant),
                        Arc::clone(&offset_map),
                    )
                    .await;
                // 配置停止 offset save 标识为 true
                // stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        });

        //判断任务是否异常退出
        if task_err_occur.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(anyhow!("task error occur"));
        }

        Ok(())
    }

    // 生成执行列表文件，列表文件描述，是否为增量场景下从checkpoint执行
    async fn generate_list_file(
        &self,
        stop_mark: Arc<AtomicBool>,
        task: Arc<dyn TransferTaskActions + Send + Sync>,
    ) -> Result<(File, FileDescription, FilePosition, bool)> {
        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            TRANSFER_CHECK_POINT_FILE,
            "",
        );

        let mut start_from_checkpoint_stage_is_increment = false;
        return match self.attributes.start_from_checkpoint {
            true => {
                // 正在执行的任务数量，用于控制分片上传并行度
                let executing_transfers = Arc::new(RwLock::new(0));
                // 变更object_list_file_name文件名
                let checkpoint = get_task_checkpoint(check_point_file.as_str())
                    .context(format!("{}:{}", file!(), line!()))?;

                // 执行error retry
                task.error_record_retry(stop_mark.clone(), executing_transfers)
                    .context(format!("{}:{}", file!(), line!()))?;

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
                    TransferStage::Stock => {
                        let list_file_desc = checkpoint.executed_file.clone();
                        let list_file_position = checkpoint.executed_file_position;
                        let list_file = checkpoint.seeked_execute_file()?;
                        Ok((
                            list_file,
                            list_file_desc,
                            list_file_position,
                            start_from_checkpoint_stage_is_increment,
                        ))
                    }
                    TransferStage::Increment => {
                        // Todo 重新分析逻辑，需要再checkpoint中记录每次增量执行前的起始时间点
                        // 清理文件重新生成object list 文件需大于指定时间戳,并根据原始object list 删除位于目标端但源端不存在的文件
                        // 流程逻辑
                        // 扫描target 文件list-> 抓取自扫描时间开始，源端的变动数据 -> 生成objlist，action 新增target change capture
                        // exec_modified = true;
                        let modified = task
                            .changed_object_capture_based_target(
                                usize::try_from(checkpoint.task_begin_timestamp).unwrap(),
                            )
                            .await?;
                        start_from_checkpoint_stage_is_increment = true;
                        let list_file = File::open(&modified.path)?;
                        let list_file_position = FilePosition::default();
                        Ok((
                            list_file,
                            modified,
                            list_file_position,
                            start_from_checkpoint_stage_is_increment,
                        ))
                    }
                }
            }
            false => {
                let pd = prompt_processbar("Generating object list ...");
                // 清理 meta 目录
                // 重新生成object list file
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
                let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());
                log::info!("invoke");
                let executed_file = FileDescription {
                    path: gen_file_path(
                        self.attributes.meta_dir.as_str(),
                        TRANSFER_OBJECT_LIST_FILE_PREFIX,
                        now.as_secs().to_string().as_str(),
                    ),
                    size: 0,
                    total_lines: 0,
                };
                let list_file_desc =
                    match task.gen_source_object_list_file(&executed_file.path).await {
                        Ok(f) => f,
                        Err(e) => {
                            return Err(e);
                        }
                    };

                let list_file = File::open(&list_file_desc.path)?;
                let list_file_position = FilePosition::default();

                pd.finish_with_message("object list generated");
                Ok((
                    list_file,
                    list_file_desc,
                    list_file_position,
                    start_from_checkpoint_stage_is_increment,
                ))
            }
        };
    }

    // 存量迁移，执行列表文件
    async fn exec_records_file(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        err_counter: Arc<AtomicUsize>,
        exec_set: &mut JoinSet<()>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        mut file_position: FilePosition,
        records_file: File,
        executing_file: FileDescription,
    ) {
        let executing_transfers = Arc::new(RwLock::new(0));
        let task_stock = self.gen_transfer_actions();
        let mut vec_keys: Vec<ListedRecord> = vec![];
        let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(records_file).lines();
        let exec_lines = executing_file.total_lines - file_position.line_num;
        for (idx, line) in lines.enumerate() {
            // 若错误达到上限，则停止任务
            if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }

            match line {
                Ok(key) => {
                    // 先写入当前key 开头的 offset，然后更新list_file_position 作为下一个key的offset,待验证效果
                    let len = key.bytes().len() + "\n".bytes().len();
                    if !key.ends_with("/") {
                        let record = ListedRecord {
                            key,
                            offset: file_position.offset,
                            line_num: file_position.line_num,
                        };
                        vec_keys.push(record);
                    }

                    file_position.offset += len;
                    file_position.line_num += 1;
                }
                Err(e) => {
                    log::error!("{:?},file position:{:?}", e, file_position);
                    stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    break;
                }
            }

            if vec_keys
                .len()
                .to_string()
                .eq(&self.attributes.objects_per_batch.to_string())
                || idx.eq(&TryInto::<usize>::try_into(exec_lines - 1).unwrap())
                    && vec_keys.len() > 0
            {
                while exec_set.len() >= self.attributes.task_parallelism {
                    exec_set.join_next().await;
                }

                // 提前插入 offset 保证顺序性
                let subffix = vec_keys[0].offset.to_string();
                let mut offset_key = OFFSET_PREFIX.to_string();
                offset_key.push_str(&subffix);

                offset_map.insert(
                    offset_key.clone(),
                    FilePosition {
                        offset: vec_keys[0].offset,
                        line_num: vec_keys[0].line_num,
                    },
                );

                let vk = vec_keys.clone();
                // task_stock
                //     .listed_records_transfor(
                //         exec_set,
                //         Arc::clone(&executing_transfers),
                //         vk.clone(),
                //         Arc::clone(&stop_mark),
                //         err_occur.clone(),
                //         Arc::clone(&err_counter),
                //         Arc::clone(&offset_map),
                //         executing_file.path.to_string(),
                //     )
                //     .await;

                // Todo
                // 验证gen_transfer_executor 使用exec_set.spawn
                let record_executer = task_stock.gen_transfer_executor(
                    stop_mark.clone(),
                    err_occur.clone(),
                    err_counter.clone(),
                    offset_map.clone(),
                    executing_file.path.to_string(),
                );

                let et = executing_transfers.clone();
                exec_set.spawn(async move {
                    if let Err(e) = record_executer.exec_listed_records(vk, et).await {
                        log::error!("{:?}", e);
                    };
                });

                // 清理临时key vec
                vec_keys.clear();
            }
        }
    }

    // 增量场景下执行record_descriptions 文件
    async fn exec_record_descriptions_file(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        err_counter: Arc<AtomicUsize>,
        exec_set: &mut JoinSet<()>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        records_desc_file: File,
        executing_file: FileDescription,
    ) {
        let executing_transfers = Arc::new(RwLock::new(0));
        let task_modify = self.gen_transfer_actions();
        let mut vec_keys: Vec<RecordDescription> = vec![];
        // 按列表传输object from source to target
        // let desc_file = File::open(&executing_file.path).unwrap();
        let total_lines = executing_file.total_lines;
        let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(records_desc_file).lines();

        for (idx, line) in lines.enumerate() {
            // 若错误达到上限，则停止任务
            if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            if let Result::Ok(l) = line {
                let record = match json_to_struct::<RecordDescription>(&l) {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("{:?}", e);
                        err_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        continue;
                    }
                };
                vec_keys.push(record);
            };

            if vec_keys
                .len()
                .to_string()
                .eq(&self.attributes.objects_per_batch.to_string())
                || idx.to_string().eq(&(total_lines - 1).to_string()) && vec_keys.len() > 0
            {
                while exec_set.len() >= self.attributes.task_parallelism {
                    exec_set.join_next().await;
                }

                let vk = vec_keys.clone();
                // task_modify
                //     .record_descriptions_transfor(
                //         exec_set,
                //         Arc::clone(&executing_transfers),
                //         vk,
                //         Arc::clone(&stop_mark),
                //         Arc::clone(&err_counter),
                //         Arc::clone(&offset_map),
                //         executing_file.path.clone(),
                //     )
                //     .await;

                let record_executer = task_modify.gen_transfer_executor(
                    stop_mark.clone(),
                    err_occur.clone(),
                    err_counter.clone(),
                    offset_map.clone(),
                    executing_file.path.to_string(),
                );

                let et = executing_transfers.clone();
                exec_set.spawn(async move {
                    if let Err(e) = record_executer.exec_record_descriptions(vk, et).await {
                        log::error!("{:?}", e);
                    };
                });

                // 清理临时key vec
                vec_keys.clear();
            }
        }
    }
}
