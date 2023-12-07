use super::{CompareTask, TransferTask, TransferType};
use crate::{
    commons::{byte_size_str_to_usize, byte_size_usize_to_str, LastModifyFilter},
    s3::OSSDescription,
};
use anyhow::{anyhow, Result};
use aws_sdk_s3::model::ObjectIdentifier;
use serde::{
    de::{self},
    Deserialize, Deserializer, Serialize, Serializer,
};
use snowflake::SnowflakeIdGenerator;
use std::{
    fs::{self, File},
    io::{self, BufRead},
};
use tokio::{
    runtime::{self},
    task::JoinSet,
};

pub const TRANSFER_OBJECT_LIST_FILE_PREFIX: &'static str = "transfer_objects_list_";
pub const COMPARE_SOURCE_OBJECT_LIST_FILE_PREFIX: &'static str = "compare_source_list_";
pub const TRANSFER_CHECK_POINT_FILE: &'static str = "checkpoint_transfer.yml";
pub const COMPARE_CHECK_POINT_FILE: &'static str = "checkpoint_compare.yml";
pub const TRANSFER_ERROR_RECORD_PREFIX: &'static str = "transfer_error_record_";
pub const COMPARE_ERROR_RECORD_PREFIX: &'static str = "compare_error_record_";
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
    TruncateBucket,
    Compare,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
// #[serde(untagged)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum TaskDescription {
    Transfer(TransferTask),
    Compare(CompareTask),
    TruncateBucket(TaskTruncateBucket),
}

// ToDo
// 抽象 task
impl TaskDescription {
    pub fn execute(&self) -> Result<()> {
        match self {
            TaskDescription::Transfer(transfer) => transfer.execute(),
            TaskDescription::TruncateBucket(truncate) => truncate.exec_multi_threads(),
            TaskDescription::Compare(compare) => compare.execute(),
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
        // 50M
        10485760 * 5
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

    pub fn last_modify_filter_default() -> Option<LastModifyFilter> {
        None
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
        let object_list_file =
            gen_file_path(self.meta_dir.as_str(), TRANSFER_OBJECT_LIST_FILE_PREFIX, "");
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
                .append_object_list_to_file(
                    self.oss.bucket.clone(),
                    self.oss.prefix.clone(),
                    self.bach_size,
                    &object_list_file,
                    None,
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
