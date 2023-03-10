use std::fs;

use crate::{commons::read_lines, s3::OSSDescription};
use anyhow::{Ok, Result};
use clap::builder::Str;
use serde::{Deserialize, Serialize};
use snowflake::SnowflakeIdGenerator;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskTransfer {
    pub task_id: String,
    pub description: String,
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub bach_size: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TaskDownload {
    #[serde(default = "TaskDownload::task_id_default")]
    pub task_id: String,
    #[serde(default = "TaskDownload::task_description_default")]
    pub description: String,
    pub source: OSSDescription,
    pub local_path: String,
    pub bach_size: i32,
}

impl TaskDownload {
    fn task_id_default() -> String {
        task_id_generator().to_string()
    }
    fn task_description_default() -> String {
        String::from("download")
    }
    pub async fn execute(&self) -> Result<()> {
        let client = self.source.gen_oss_client_ref()?;

        // 生成文件清单，文件清单默认文件存储在文件存储目录下 .objlist
        let object_list_file = self.local_path.clone() + "/.objlist";
        let _ = fs::remove_file(object_list_file.clone());
        let r = client
            .append_all_object_list_to_file(
                self.source.bucket.clone(),
                None,
                self.bach_size,
                object_list_file.clone(),
            )
            .await;

        if let Err(e) = r {
            log::error!("{}", e);
        };

        // 根据清单下载文件
        let lines = read_lines(object_list_file.clone())?;
        for line in lines {
            if let Result::Ok(f) = line {
                if !f.ends_with("/") {
                    let r = client
                        .download_object_to_dir(
                            self.source.bucket.clone(),
                            f.clone(),
                            self.local_path.clone(),
                        )
                        .await;
                    if let Err(e) = r {
                        log::error!("{}", e);
                    };
                }
            };
        }

        Ok(())
    }
}

pub fn task_id_generator() -> i64 {
    let mut id_generator_generator = SnowflakeIdGenerator::new(1, 1);
    let id = id_generator_generator.real_time_generate();
    id
}
