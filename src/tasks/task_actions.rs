use super::IncrementAssistant;
use crate::checkpoint::{ExecutedFile, FilePosition, ListedRecord};
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use std::{
    fs::File,
    io::{self, BufRead},
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
};
use tokio::{sync::Mutex, task::JoinSet};

// Todo
// 需新增objectlistfile executor，用于承载对象列表对象执行逻辑
// 新增increment_prelude 用于执行增量启动前的notify记录以及记录oss 的 lastmodify
// 设计 incrementparameter struct 用于统一存储 lastmodif notify file 以及 notify file size 等原子数据
#[async_trait]
pub trait TransferTaskActions {
    // 错误记录重试
    fn error_record_retry(&self) -> Result<()>;
    // 记录执行器
    async fn records_excutor(
        &self,
        execute_set: &mut JoinSet<()>,
        records: Vec<ListedRecord>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    );

    // 根据执行列表执行传输操作
    // async fn execute_transfer_by_list_file(
    //     &self,
    //     executed_file: &File,
    //     list_file_position: FilePosition,
    //     max_errors: usize,
    //     err_counter: Arc<AtomicUsize>,
    // ) -> Result<()> {
    //     let mut vec_keys: Vec<ListedRecord> = vec![];
    //     // 按列表传输object from source to target
    //     let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(executed_file).lines();

    //     for line in lines {
    //         // 若错误达到上限，则停止任务
    //         if err_counter.load(std::sync::atomic::Ordering::SeqCst) >= max_errors {
    //             break;
    //         }
    //         if let Result::Ok(key) = line {
    //             let len = key.bytes().len() + "\n".bytes().len();
    //             list_file_position.offset += len;
    //             list_file_position.line_num += 1;

    //             if !key.ends_with("/") {
    //                 let record = ListedRecord {
    //                     key,
    //                     offset: list_file_position.offset,
    //                     line_num: list_file_position.line_num,
    //                 };
    //                 match exclude_regex_set {
    //                     Some(ref exclude) => {
    //                         if exclude.is_match(&record.key) {
    //                             continue;
    //                         }
    //                     }
    //                     None => {}
    //                 }
    //                 match include_regex_set {
    //                     Some(ref set) => {
    //                         if set.is_match(&record.key) {
    //                             vec_keys.push(record);
    //                         }
    //                     }
    //                     None => {
    //                         vec_keys.push(record);
    //                     }
    //                 }
    //             }
    //         };

    //         if vec_keys
    //             .len()
    //             .to_string()
    //             .eq(&self.attributes.bach_size.to_string())
    //         {
    //             while execut_set.len() >= self.attributes.task_threads {
    //                 execut_set.join_next().await;
    //             }
    //             let vk = vec_keys.clone();
    //             task_stock
    //                 .records_excutor(
    //                     &mut execut_set,
    //                     vk,
    //                     Arc::clone(&err_counter),
    //                     Arc::clone(&offset_map),
    //                     executed_file.path.clone(),
    //                 )
    //                 .await;

    //             // 清理临时key vec
    //             vec_keys.clear();
    //         }
    //     }

    //     // 处理集合中的剩余数据，若错误达到上限，则不执行后续操作
    //     if vec_keys.len() > 0
    //         && err_counter.load(std::sync::atomic::Ordering::SeqCst) < self.attributes.max_errors
    //     {
    //         while execut_set.len() >= self.attributes.task_threads {
    //             execut_set.join_next().await;
    //         }

    //         let vk = vec_keys.clone();
    //         task_stock
    //             .records_excutor(
    //                 &mut execut_set,
    //                 vk,
    //                 Arc::clone(&err_counter),
    //                 Arc::clone(&offset_map),
    //                 executed_file.path.clone(),
    //             )
    //             .await;
    //     }

    //     while execut_set.len() > 0 {
    //         execut_set.join_next().await;
    //     }
    //     // 配置停止 offset save 标识为 true
    //     snapshot_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
    //     let lock = increment_assistant.lock().await;
    //     let notify = lock.get_notify_file_path();
    //     drop(lock);

    //     // 记录checkpoint
    //     let mut checkpoint: CheckPoint = CheckPoint {
    //         execute_file: executed_file.clone(),
    //         execute_file_position: list_file_position.clone(),
    //         file_for_notify: notify,
    //         task_stage: TaskStage::Stock,
    //         timestampe: 0,
    //     };
    //     if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
    //         log::error!("{}", e);
    //     };

    //     while sys_set.len() > 0 {
    //         sys_set.join_next().await;
    //     }
    // }

    // 生成对象列表
    async fn generate_execute_file(
        &self,
        last_modify_timestamp: Option<i64>,
        object_list_file: &str,
    ) -> Result<ExecutedFile>;

    // 执行增量前置操作，例如启动notify线程，记录last modify 时间戳等
    async fn increment_prelude(&self, assistant: Arc<Mutex<IncrementAssistant>>) -> Result<()>;

    // 执行增量任务
    async fn execute_increment(
        &self,
        execute_set: &mut JoinSet<()>,
        assistant: Arc<Mutex<IncrementAssistant>>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        snapshot_stop_mark: Arc<AtomicBool>,
        start_file_position: FilePosition,
    );
}

#[async_trait]
pub trait CompareTaskActions {
    async fn compare_listed_records(&self, records: Vec<ListedRecord>) -> Result<()>;
    // // 错误记录重试
    // fn error_record_retry(&self) -> Result<()>;
    // // 记录执行器
    // async fn records_excutor(
    //     &self,
    //     joinset: &mut JoinSet<()>,
    //     records: Vec<ListedRecord>,
    //     err_counter: Arc<AtomicUsize>,
    //     offset_map: Arc<DashMap<String, FilePosition>>,
    //     list_file: String,
    // );

    // // 生成对象列表
    // fn generate_object_list(
    //     &self,
    //     rt: &Runtime,
    //     _last_modify_timestamp: i64,
    //     object_list_file: &str,
    // ) -> Result<usize>;
}
