use super::{
    gen_file_path, task_actions::TransferTaskActions, TaskStage, TaskStatusSaver, TransferTask,
    TransferTaskAttributes, CHECK_POINT_FILE_NAME, OBJECT_LIST_FILE_PREFIX, OFFSET_PREFIX,
};
use crate::{
    checkpoint::{get_task_checkpoint, CheckPoint, FilePosition, ListedRecord},
    commons::exec_processbar,
};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use indicatif::{ProgressBar, ProgressStyle};
use regex::RegexSet;
use std::{
    fs::{self, File},
    io::{self, BufRead},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    runtime,
    sync::Mutex,
    task::{self, JoinSet},
};

#[derive(Debug, Clone)]
pub struct IncrementAssistant {
    pub check_point_path: String,
    pub local_notify: Option<LocalNotify>,
    pub last_modify_timestamp: Option<i64>,
}

impl Default for IncrementAssistant {
    fn default() -> Self {
        Self {
            last_modify_timestamp: None,
            local_notify: None,
            check_point_path: "".to_string(),
        }
    }
}

impl IncrementAssistant {
    pub fn set_local_notify(&mut self, local_notify: Option<LocalNotify>) {
        self.local_notify = local_notify;
    }

    pub fn get_notify_file_path(&self) -> Option<String> {
        match self.local_notify.clone() {
            Some(n) => Some(n.notify_file_path),
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalNotify {
    pub notify_file_path: String,
    pub notify_file_size: Arc<AtomicU64>,
}

// 执行传输任务
// 重点抽象不同数据源统一执行模式
pub fn execute_transfer_task(
    transfer: TransferTask,
    task_attributes: &TransferTaskAttributes,
) -> Result<()> {
    let task = transfer.gen_transfer_actions();
    let mut interrupt: bool = false;
    // 执行 object_list 文件中行的总数
    let mut total_lines: u64 = 0;
    // 执行过程中错误数统计
    let err_counter = Arc::new(AtomicUsize::new(0));
    // 任务停止标准，用于通知所有协程任务结束
    let snapshot_stop_mark = Arc::new(AtomicBool::new(false));
    let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

    let mut object_list_file_name = gen_file_path(
        task_attributes.meta_dir.as_str(),
        OBJECT_LIST_FILE_PREFIX,
        now.as_secs().to_string().as_str(),
    );

    let check_point_file =
        gen_file_path(task_attributes.meta_dir.as_str(), CHECK_POINT_FILE_NAME, "");

    let mut assistant = IncrementAssistant::default();
    assistant.check_point_path = check_point_file.clone();
    let increment_assistant = Arc::new(Mutex::new(assistant));

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

    let mut list_file = None;
    let mut list_file_position = FilePosition::default();

    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .enable_all()
        .max_io_events_per_tick(task_attributes.task_threads)
        .build()?;

    // 生成object list 文件
    rt.block_on(async {
        if task_attributes.start_from_checkpoint {
            // 执行error retry
            match task.error_record_retry() {
                Ok(_) => {}
                Err(e) => {
                    log::error!("{}", e);
                    interrupt = true;
                    return;
                }
            };
            // task.error_record_retry();
            // 从 checkpoint 获取 文件名 object_list_file_name，更改 存量调整 object list file offset
            // 增量 生成 object list file 文件lastmodify大于指定时间戳

            // 变更object_list_file_name文件名
            let checkpoint = match get_task_checkpoint(check_point_file.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    interrupt = true;
                    return;
                }
            };
            object_list_file_name = checkpoint.execute_file;

            let checkpoint = match get_task_checkpoint(check_point_file.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{}", e);
                    interrupt = true;
                    return;
                }
            };

            match checkpoint.task_stage {
                TaskStage::Stock => match checkpoint.seeked_execute_file() {
                    Ok(f) => {
                        total_lines = 100;
                        object_list_file_name = checkpoint.execute_file;
                        list_file_position = checkpoint.execute_file_position.clone();
                        list_file = Some(f);
                    }
                    Err(e) => {
                        log::error!("{}", e);
                        interrupt = true;
                        return;
                    }
                },
                TaskStage::Increment => {
                    // 清理文件重新生成object list 文件需大于指定时间戳
                    let timestamp = TryInto::<i64>::try_into(checkpoint.timestampe).unwrap();
                    let _ = fs::remove_file(&object_list_file_name);
                    match task
                        .generate_object_list(Some(timestamp), &object_list_file_name)
                        .await
                    {
                        Ok(lines) => total_lines = lines,
                        Err(e) => {
                            log::error!("{}", e);
                            return;
                        }
                    };
                }
            }
        } else {
            // 清理 meta 目录
            // 重新生成object list file
            let _ = fs::remove_dir_all(task_attributes.meta_dir.as_str());

            let pd_gen_object_list = ProgressBar::new_spinner();
            pd_gen_object_list.enable_steady_tick(Duration::from_millis(120));
            pd_gen_object_list.set_style(
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
            pd_gen_object_list.set_message("Generating object list ...");
            match task
                .generate_object_list(None, object_list_file_name.as_str())
                .await
            {
                Ok(lines) => {
                    total_lines = lines;
                    pd_gen_object_list.finish_with_message("object list Done");
                }
                Err(e) => {
                    log::error!("{}", e);
                    interrupt = true;
                    pd_gen_object_list.finish_with_message("object list Fail");
                }
            }
        }
    });

    if interrupt {
        return Err(anyhow!("get object list error"));
    }
    // sys_set 用于执行checkpoint、notify等辅助任务
    let mut sys_set = JoinSet::new();
    // execut_set 用于执行任务
    let mut execut_set: JoinSet<()> = JoinSet::new();
    // let mut object_list_file = File::open(object_list_file_name.as_str())?;
    let object_list_file = match list_file {
        Some(f) => f,
        None => File::open(object_list_file_name.as_str())?,
    };
    rt.block_on(async {
        // 持续同步逻辑: 执行增量助理
        let task_increment_prelude = transfer.gen_transfer_actions();
        if task_attributes.continuous {
            let assistant = Arc::clone(&increment_assistant);
            task::spawn(async move {
                if let Err(e) = task_increment_prelude.increment_prelude(assistant).await {
                    log::error!("{}", e);
                }
            });
        }

        // 启动checkpoint记录线程
        let lock = increment_assistant.lock().await;
        let file_for_notify = match lock.get_notify_file_path() {
            Some(s) => Some(s),
            None => None,
        };
        drop(lock);

        let stock_status_saver = TaskStatusSaver {
            check_point_path: check_point_file.clone(),
            execute_file_path: object_list_file_name.clone(),
            stop_mark: Arc::clone(&snapshot_stop_mark),
            list_file_positon_map: Arc::clone(&offset_map),
            file_for_notify,
            task_stage: TaskStage::Stock,
            interval: 3,
        };
        sys_set.spawn(async move {
            stock_status_saver.snapshot_to_file().await;
        });

        // 启动进度条线程
        let map = Arc::clone(&offset_map);
        let stop_mark = Arc::clone(&snapshot_stop_mark);
        let total = total_lines;
        sys_set.spawn(async move {
            // Todo 调整进度条
            exec_processbar(total, stop_mark, map, OFFSET_PREFIX).await;
        });
        let task_stock = transfer.gen_transfer_actions();
        let mut vec_keys: Vec<ListedRecord> = vec![];
        // 按列表传输object from source to target
        let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(object_list_file).lines();
        for line in lines {
            // 若错误达到上限，则停止任务
            if err_counter.load(std::sync::atomic::Ordering::SeqCst) >= task_attributes.max_errors {
                break;
            }
            if let Result::Ok(key) = line {
                let len = key.bytes().len() + "\n".bytes().len();
                list_file_position.offset += len;
                list_file_position.line_num += 1;
                // offset += len;
                // line_num += 1;
                if !key.ends_with("/") {
                    let record = ListedRecord {
                        key,
                        offset: list_file_position.offset,
                        line_num: list_file_position.line_num,
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
                task_stock
                    .records_excutor(
                        &mut execut_set,
                        vk,
                        Arc::clone(&err_counter),
                        Arc::clone(&offset_map),
                        object_list_file_name.clone(),
                    )
                    .await;

                // 清理临时key vec
                vec_keys.clear();
            }
        }

        // 处理集合中的剩余数据，若错误达到上限，则不执行后续操作
        if vec_keys.len() > 0
            && err_counter.load(std::sync::atomic::Ordering::SeqCst) < task_attributes.max_errors
        {
            while execut_set.len() >= task_attributes.task_threads {
                execut_set.join_next().await;
            }

            let vk = vec_keys.clone();
            task_stock
                .records_excutor(
                    &mut execut_set,
                    vk,
                    Arc::clone(&err_counter),
                    Arc::clone(&offset_map),
                    object_list_file_name.clone(),
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
            execute_file: object_list_file_name.clone(),
            execute_file_position: list_file_position.clone(),
            file_for_notify: notify,
            task_stage: TaskStage::Stock,
            timestampe: 0,
        };
        if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
            log::error!("{}", e);
        };

        while sys_set.len() > 0 {
            sys_set.join_next().await;
        }

        // 增量逻辑
        if task_attributes.continuous {
            let stop_mark = Arc::new(AtomicBool::new(false));
            let offset_map = Arc::new(DashMap::<String, FilePosition>::new());

            // let task_status_saver = TaskStatusSaver {
            //     check_point_path: check_point_file.clone(),
            //     execute_file_path: increment_assistant.get_notify_file_path(),
            //     stop_mark: Arc::clone(&stop_mark),
            //     list_file_positon_map: Arc::clone(&offset_map),
            //     file_for_notify: increment_assistant.get_notify_file_path(),
            //     task_stage: TaskStage::Increment,
            //     interval: 3,
            // };
            // sys_set.spawn(async move {
            //     task_status_saver.snapshot_to_file().await;
            // });

            let task_increment = transfer.gen_transfer_actions();
            let _ = task_increment
                .execute_increment(
                    Arc::clone(&increment_assistant),
                    Arc::clone(&err_counter),
                    Arc::clone(&offset_map),
                    Arc::clone(&stop_mark),
                    FilePosition {
                        offset: 0,
                        line_num: 0,
                    },
                )
                .await;
            // 配置停止 offset save 标识为 true
            snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });

    Ok(())
}
