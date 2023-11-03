use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, Seek, SeekFrom},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    checkpoint::{get_task_checkpoint, CheckPoint, FilePosition, ListedRecord},
    commons::{exec_processbar, NotifyWatcher},
    logger,
};

use super::{
    gen_file_path, task_actions::TransferTaskActions, TaskRunningStatus, TaskStatusSaver,
    TransferTaskAttributes, CHECK_POINT_FILE_NAME, NOTIFY_FILE_PREFIX, OBJECT_LIST_FILE_PREFIX,
    OFFSET_PREFIX,
};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use indicatif::{ProgressBar, ProgressStyle};
use regex::RegexSet;
use tokio::{
    runtime,
    task::{self, JoinSet},
};

#[derive(Debug, Clone)]
pub struct IncrementAssistant {
    pub local_notify: Option<LocalNotify>,
    pub last_modify_timestamp: Option<i64>,
}

impl Default for IncrementAssistant {
    fn default() -> Self {
        Self {
            local_notify: None,
            last_modify_timestamp: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalNotify {
    // pub watcher: NotifyWatcher,
    pub notify_file_path: String,
    pub notify_file_size: Arc<AtomicU64>,
}
// 执行传输任务
// 重点抽象不同数据源统一执行模式
pub fn execute_transfer_task<T>(task: &T, task_attributes: &TransferTaskAttributes) -> Result<()>
where
    T: TransferTaskActions,
{
    let mut interrupt = false;
    // 执行 object_list 文件中行的总数
    let mut total_lines: usize = 0;
    // 执行过程中错误数统计
    let error_conter = Arc::new(AtomicUsize::new(0));
    // 任务停止标准，用于通知所有协程任务结束
    let snapshot_stop_mark = Arc::new(AtomicBool::new(false));
    let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    // 增量任务计数器，用于记录notify file 的文件大小
    // let notify_file_size = Arc::new(AtomicU64::new(0));

    let object_list_file = gen_file_path(
        task_attributes.meta_dir.as_str(),
        OBJECT_LIST_FILE_PREFIX,
        now.as_secs().to_string().as_str(),
    );

    let mut increment_assistant = IncrementAssistant::default();

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
        // if init {
        if true {
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

            rt.block_on(async {
                match task
                    .generate_object_list(0, object_list_file.as_str())
                    .await
                {
                    Ok(lines) => {
                        total_lines = lines;
                        pb.finish_with_message("object list Done");
                    }
                    Err(e) => {
                        log::error!("{}", e);
                        interrupt = true;
                        pb.finish_with_message("object list Fail");
                    }
                }
            });

            if interrupt {
                return Err(anyhow!("get object list error"));
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

        // 持续同步逻辑: 执行增量助理
        if task_attributes.continuous {
            if let Err(e) = task.increment_prelude(&mut increment_assistant).await {
                log::error!("{}", e);
                interrupt = true;
            }
        }

        // 启动进度条线程
        let map = Arc::clone(&offset_map);
        let stop_mark = Arc::clone(&snapshot_stop_mark);
        let total = TryInto::<u64>::try_into(total_lines).unwrap();
        sys_set.spawn(async move {
            // Todo 调整进度条
            exec_processbar(total, stop_mark, map, OFFSET_PREFIX).await;
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
            let stop_mark = Arc::new(AtomicBool::new(false));
            let offset_map = Arc::new(DashMap::<String, FilePosition>::new());

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
                    &increment_assistant,
                    Arc::clone(&error_conter),
                    Arc::clone(&offset_map),
                    Arc::clone(&stop_mark),
                )
                .await;
            // 配置停止 offset save 标识为 true
            snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });

    Ok(())
}
