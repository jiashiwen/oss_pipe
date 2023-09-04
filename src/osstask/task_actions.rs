use std::sync::{atomic::AtomicUsize, Arc};

use anyhow::{Ok, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::{runtime::Runtime, task::JoinSet};

use crate::checkpoint::Record;

use super::TaskType;

#[async_trait]
pub trait TaskActions {
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
        records: Vec<Record>,
        error_conter: Arc<AtomicUsize>,
        offset_map: Arc<DashMap<String, usize>>,
        current_line_number: usize,
    );

    // 生成对象列表
    fn generate_object_list(
        &self,
        rt: &Runtime,
        last_modify_timestamp: i64,
        object_list_file: &str,
    ) -> Result<usize>;
    // fn gen_executor(&self) -> Self::Item;
}
