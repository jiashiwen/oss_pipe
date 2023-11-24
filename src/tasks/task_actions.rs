use super::IncrementAssistant;
use crate::{
    checkpoint::{FileDescription, FilePosition, ListedRecord},
    commons::LastModifyFilter,
};
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
};
use tokio::{sync::Mutex, task::JoinSet};

// Todo
// 需新增objectlistfile executor，用于承载对象列表对象执行逻辑
// 新增increment_prelude 用于执行增量启动前的notify记录以及记录oss 的 lastmodify
// 设计 incrementparameter struct 用于统一存储 lastmodif notify file 以及 notify file size 等原子数据
#[async_trait]
pub trait TransferTaskActions {
    async fn analyze_source(&self) -> Result<DashMap<String, i128>>;
    // 错误记录重试
    fn error_record_retry(&self) -> Result<()>;
    // 记录执行器
    async fn records_excutor(
        &self,
        execute_set: &mut JoinSet<()>,
        records: Vec<ListedRecord>,
        stop_mark: Arc<AtomicBool>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        target_exist_skip: bool,
        list_file: String,
    );

    // 生成对象列表
    async fn generate_execute_file(
        &self,
        last_modify_filter: Option<LastModifyFilter>,
        object_list_file: &str,
    ) -> Result<FileDescription>;

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
