use super::{
    task_actions::CompareTaskActions, CompareLocal2Local, CompareLocal2Oss, CompareOss2Local,
    CompareOss2Oss, ObjectStorage, TaskDefaultParameters,
};
use crate::commons::LastModifyFilter;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::Write};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObjectDiff {
    pub source: String,
    pub target: String,
    pub diff: Diff,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Diff {
    ExistsDiff(DiffExists),
    LenthDiff(DiffLength),
    ExpiresDiff(DiffExpires),
    ContentDiff(DiffContent),
    MetaDiff(DiffMeta),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffExists {
    pub source_exists: bool,
    pub target_exists: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffExpires {
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
    pub source_content_len: i64,
    pub target_content_len: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffContent {
    pub stream_position: usize,
    pub source_byte: u8,
    pub target_byte: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffMeta {
    pub source_meta: Option<HashMap<std::string::String, std::string::String>>,
    pub target_meta: Option<HashMap<std::string::String, std::string::String>>,
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
    #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    pub large_file_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunk_default")]
    pub multi_part_chunk: usize,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::exprirs_diff_scope_default")]
    pub exprirs_diff_scope: i64,
    #[serde(default = "TaskDefaultParameters::continuous_default")]
    pub continuous: bool,
    #[serde(default = "TaskDefaultParameters::last_modify_filter_default")]
    pub last_modify_filter: Option<LastModifyFilter>,
}

impl Default for CompareTaskAttributes {
    fn default() -> Self {
        Self {
            bach_size: TaskDefaultParameters::batch_size_default(),
            task_threads: TaskDefaultParameters::task_threads_default(),
            max_errors: TaskDefaultParameters::max_errors_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            start_from_checkpoint: TaskDefaultParameters::target_exists_skip_default(),
            large_file_size: TaskDefaultParameters::large_file_size_default(),
            multi_part_chunk: TaskDefaultParameters::multi_part_chunk_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
            continuous: TaskDefaultParameters::continuous_default(),
            last_modify_filter: TaskDefaultParameters::last_modify_filter_default(),
            exprirs_diff_scope: TaskDefaultParameters::exprirs_diff_scope_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompareCheckOption {
    #[serde(default = "CompareCheckOption::default_check_content_length")]
    check_content_length: bool,
    #[serde(default = "CompareCheckOption::default_check_expires")]
    check_expires: bool,
    #[serde(default = "CompareCheckOption::default_check_content")]
    check_content: bool,
    #[serde(default = "CompareCheckOption::default_check_meta_data")]
    check_meta_data: bool,
}

impl Default for CompareCheckOption {
    fn default() -> Self {
        Self {
            check_content_length: CompareCheckOption::default_check_content_length(),
            check_expires: CompareCheckOption::default_check_expires(),
            check_content: CompareCheckOption::default_check_content(),
            check_meta_data: CompareCheckOption::default_check_meta_data(),
        }
    }
}

impl CompareCheckOption {
    pub fn default_check_content_length() -> bool {
        true
    }

    pub fn default_check_expires() -> bool {
        false
    }

    pub fn default_check_content() -> bool {
        false
    }

    pub fn default_check_meta_data() -> bool {
        false
    }

    pub fn check_content_length(&self) -> bool {
        self.check_content_length
    }

    pub fn check_expires(&self) -> bool {
        self.check_expires
    }

    pub fn check_meta_data(&self) -> bool {
        self.check_meta_data
    }

    pub fn check_content(&self) -> bool {
        self.check_content
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct CompareTask {
    pub source: ObjectStorage,
    pub target: ObjectStorage,
    pub check_option: CompareCheckOption,
    pub attributes: CompareTaskAttributes,
}

impl Default for CompareTask {
    fn default() -> Self {
        Self {
            source: ObjectStorage::default(),
            target: ObjectStorage::default(),
            check_option: CompareCheckOption::default(),
            attributes: CompareTaskAttributes::default(),
        }
    }
}

impl CompareTask {
    pub fn gen_compare_actions(&self) -> Box<dyn CompareTaskActions + Send + Sync> {
        match &self.source {
            ObjectStorage::Local(path_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = CompareLocal2Local {
                        source: path_s.to_string(),
                        target: path_t.to_string(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = CompareLocal2Oss {
                        source: path_s.to_string(),
                        target: oss_t.clone(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
            },
            ObjectStorage::OSS(oss_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = CompareOss2Local {
                        source: oss_s.clone(),
                        target: path_t.to_string(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = CompareOss2Oss {
                        source: oss_s.clone(),
                        target: oss_t.clone(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
            },
        }
    }
    pub fn execute(&self) -> Result<()> {
        Ok(())
    }
}
