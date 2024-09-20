use super::{CompareTask, TaskDeleteBucket, TransferTask, TransferType};
use crate::{
    commons::{
        byte_size_str_to_usize, byte_size_usize_to_str, struct_to_yaml_string, LastModifyFilter,
    },
    tasks::LogInfo,
};
use anyhow::Result;
use serde::{
    de::{self},
    Deserialize, Deserializer, Serialize, Serializer,
};
use snowflake::SnowflakeIdGenerator;
use std::time::Instant;

pub const TRANSFER_OBJECT_LIST_FILE_PREFIX: &'static str = "transfer_objects_list_";
pub const COMPARE_SOURCE_OBJECT_LIST_FILE_PREFIX: &'static str = "compare_source_list_";
pub const TRANSFER_CHECK_POINT_FILE: &'static str = "checkpoint_transfer.yml";
pub const COMPARE_CHECK_POINT_FILE: &'static str = "checkpoint_compare.yml";
pub const TRANSFER_ERROR_RECORD_PREFIX: &'static str = "transfer_error_record_";
pub const COMPARE_RESULT_PREFIX: &'static str = "compare_result_";
pub const OFFSET_PREFIX: &'static str = "offset_";
pub const NOTIFY_FILE_PREFIX: &'static str = "notify_";
pub const REMOVED_PREFIX: &'static str = "removed_";
pub const MODIFIED_PREFIX: &'static str = "modified_";

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct AnalyzedResult {
    pub max: i128,
    pub min: i128,
}
/// 任务阶段，包括存量曾量全量
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TransferStage {
    Stock,
    Increment,
}

/// 任务类别，根据传输方式划分
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TaskType {
    Transfer,
    DeleteBucket,
    Compare,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
// #[serde(untagged)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum Task {
    Transfer(TransferTask),
    Compare(CompareTask),
    DeleteBucket(TaskDeleteBucket),
}

impl Task {
    pub fn execute(&self) {
        let now = Instant::now();
        match self {
            Task::Transfer(transfer) => {
                log::info!(
                    "Transfer Task Start:\n{}",
                    struct_to_yaml_string(transfer).unwrap()
                );
                match transfer.start_task() {
                    Ok(_) => {
                        let log_info = LogInfo {
                            task_id: transfer.task_id.clone(),
                            msg: "execute ok!".to_string(),
                            additional: Some(now.elapsed()),
                        };
                        log::info!("{:?}", log_info)
                    }
                    Err(e) => {
                        log::error!("{:?}", e);
                    }
                }
            }
            Task::DeleteBucket(truncate) => {
                log::info!(
                    "Truncate Task Start:\n{}",
                    struct_to_yaml_string(truncate).unwrap()
                );
                match truncate.execute() {
                    Ok(_) => {
                        let log_info = LogInfo {
                            task_id: truncate.task_id.clone(),
                            msg: "execute ok!".to_string(),
                            additional: Some(now.elapsed()),
                        };
                        log::info!("{:?}", log_info)
                    }
                    Err(e) => log::error!("{:?}", e),
                }
            }
            Task::Compare(compare) => {
                log::info!(
                    "Compare Task Start:\n{}",
                    struct_to_yaml_string(compare).unwrap()
                );
                match compare.execute() {
                    Ok(_) => {
                        let log_info = LogInfo {
                            task_id: compare.task_id.clone(),
                            msg: "execute ok!".to_string(),
                            additional: Some(now.elapsed()),
                        };
                        log::info!("{:?}", log_info)
                    }
                    Err(e) => log::error!("{:?}", e),
                }
            }
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

    pub fn objects_per_batch_default() -> i32 {
        100
    }

    pub fn task_parallelism_default() -> usize {
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
        // 50M
        10485760 * 5
    }
    pub fn multi_part_chunk_size_default() -> usize {
        // 10M
        10485760
    }

    pub fn multi_part_chunks_per_batch_default() -> usize {
        10
    }
    pub fn multi_part_parallelism_default() -> usize {
        num_cpus::get() * 2
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

    pub fn last_modify_filter_default() -> Option<LastModifyFilter> {
        None
    }
}

pub fn de_usize_from_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    byte_size_str_to_usize(&s).map_err(de::Error::custom)
}

pub fn se_usize_to_str<S>(v: &usize, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let size = byte_size_usize_to_str(*v);
    serializer.serialize_str(size.as_str())
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
