use std::{
    fs::File,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
        Arc,
    },
};

use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use notify;
use tokio::{runtime::Runtime, task::JoinSet};

use crate::{
    checkpoint::{FilePosition, ListedRecord},
    commons::NotifyWatcher,
};

use super::TaskType;

#[async_trait]
pub trait TransferTaskActions {
    // 错误记录重试
    fn error_record_retry(&self) -> Result<()>;
    // 记录执行器
    async fn records_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        records: Vec<ListedRecord>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    );

    // 生成对象列表
    async fn generate_object_list(
        &self,
        _last_modify_timestamp: i64,
        object_list_file: &str,
    ) -> Result<usize>;
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

#[async_trait]
pub trait TaskActionsFromOss {
    // type Item;
    // 返回任务类型
    fn task_type(&self) -> TaskType;
    // 执行任务
    // fn exec_task(&self, init: bool) -> Result<()>;
    // 错误记录重试
    fn error_record_retry(&self) -> Result<()>;
    // 记录执行器
    async fn records_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        records: Vec<ListedRecord>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    );

    // 生成对象列表
    fn generate_object_list(
        &self,
        rt: &Runtime,
        last_modify_timestamp: i64,
        object_list_file: &str,
    ) -> Result<usize>;
}

#[async_trait]
pub trait TaskActionsFromLocal {
    // type Item;
    // 返回任务类型
    fn task_type(&self) -> TaskType;
    // 错误记录重试
    fn error_record_retry(&self) -> Result<()>;
    // 记录执行器
    async fn records_excutor(
        &self,
        joinset: &mut JoinSet<()>,
        records: Vec<ListedRecord>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file: String,
    );

    // 生成对象列表
    fn generate_object_list(
        &self,
        rt: &Runtime,
        // last_modify_timestamp: i64,
        object_list_file: &str,
    ) -> Result<usize>;

    fn gen_watcher(&self) -> notify::Result<NotifyWatcher>;

    async fn watch_modify_to_file(
        &self,
        watcher: NotifyWatcher,
        file: File,
        file_size: Arc<AtomicU64>,
    ) {
        watcher.watch_to_file(file, file_size).await;
    }

    async fn execute_increment(
        &self,
        notify_file: &str,
        notify_file_size: Arc<AtomicU64>,
        err_counter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        snapshot_stop_mark: Arc<AtomicBool>,
    );
    // async fn modified_handler(&self, modified: Modified, client: &OssClient) -> Result<()>;
}
