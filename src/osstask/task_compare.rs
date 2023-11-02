use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::io::AsyncReadExt;

use super::{ObjectStorage, TaskDefaultParameters};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ObjectDiff {
    NotExists(DiffNotExists),
    ContentLenthDiff(DiffLength),
    ExpiresDiff(DiffExpires),
    ContentDiff(DiffContent),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffNotExists {
    pub key: String,
    pub source_exists: bool,
    pub target_exists: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffExpires {
    pub key: String,
    pub source_expires: Option<DateTime>,
    pub target_expires: Option<DateTime>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DateTime {
    pub seconds: i64,
    pub subsecond_nanos: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffLength {
    pub key: String,
    pub source_content_len: i64,
    pub target_content_len: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffContent {
    pub key: String,
    pub stream_position: usize,
    pub source_byte: u8,
    pub target_byte: u8,
}

impl ObjectDiff {
    pub fn save_json_to_file(&self, file: &mut File) -> Result<()> {
        // 获取文件路径，若不存在则创建路径
        let mut json = serde_json::to_string(self)?;
        json.push_str("\n");
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompareTaskAttributes {
    #[serde(default = "TaskDefaultParameters::batch_size_default")]
    pub bach_size: i32,
    #[serde(default = "TaskDefaultParameters::task_threads_default")]
    pub task_threads: usize,
    #[serde(default = "TaskDefaultParameters::max_errors_default")]
    pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub start_from_checkpoint: bool,
    // #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    // pub large_file_size: usize,
    // #[serde(default = "TaskDefaultParameters::multi_part_chunk_default")]
    // pub multi_part_chunk: usize,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::continuous_default")]
    pub continuous: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompareCheckOption {
    #[serde(default = "CompareCheckOption::default_check_diff_length")]
    check_diff_length: bool,
    #[serde(default = "CompareCheckOption::default_check_diff_expires")]
    check_diff_expires: bool,
    #[serde(default = "CompareCheckOption::default_check_diff_content")]
    check_diff_content: bool,
    #[serde(default = "CompareCheckOption::default_check_diff_meta_data")]
    check_diff_meta_data: bool,
}

impl CompareCheckOption {
    pub fn default_check_diff_length() -> bool {
        true
    }

    pub fn default_check_diff_expires() -> bool {
        false
    }

    pub fn default_check_diff_content() -> bool {
        false
    }

    pub fn default_check_diff_meta_data() -> bool {
        false
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompareTask {
    source: ObjectStorage,
    target: ObjectStorage,
    check_option: CompareCheckOption,
    task_ttributes: CompareTaskAttributes,
}
