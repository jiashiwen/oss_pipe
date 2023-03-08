use crate::s3::OSSDescription;
use anyhow::{Ok, Result};
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
pub struct TaskDownload {
    pub task_id: String,
    pub description: String,
    pub source: OSSDescription,
    pub local_path: String,
    pub bach_size: i32,
}

impl TaskDownload {
    // 获取文件列表
    pub async fn get_object_list_to_file(self, prefix: Option<String>, file: &str) -> Result<()> {
        return match self.source.provider {
            crate::s3::OssProvider::JD => {
                let client = self.source.gen_oss_client();

                let mut token = client
                    .jd_object_list_to_file(
                        prefix.clone(),
                        self.source.bucket.clone(),
                        file.to_string(),
                        self.bach_size,
                        None,
                    )
                    .await?;

                while token.is_some() {
                    token = client
                        .jd_object_list_to_file(
                            prefix.clone(),
                            self.source.bucket.clone(),
                            file.to_string(),
                            self.bach_size,
                            token,
                        )
                        .await?;
                }

                Ok(())
            }
            crate::s3::OssProvider::Ali => todo!(),
            crate::s3::OssProvider::AWS => todo!(),
        };
    }

    // 按批次切分

    // 下载文件到本地
}

pub fn task_id_generator() -> i64 {
    let mut id_generator_generator = SnowflakeIdGenerator::new(1, 1);
    let id = id_generator_generator.real_time_generate();
    id
}

#[cfg(test)]
mod test {
    use crate::{
        commons::read_yaml_file,
        osstask::task::TaskDownload,
        s3::{OSSDescription, OssProvider},
    };

    //cargo test osstask::task::test::test_TaskDownload_get_object_list_to_file -- --nocapture
    #[test]
    fn test_TaskDownload_get_object_list_to_file() {
        println!("test_jdcloud_s3_client");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let vec_oss = read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let mut oss_jd = OSSDescription::default();
        for item in vec_oss.iter() {
            if item.provider == OssProvider::JD {
                oss_jd = item.clone();
            }
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        // 使用 block_on 调用 async 函数
        let _shared_config = rt.block_on(async {
            let td = TaskDownload {
                task_id: "xxxx".to_string(),
                source: oss_jd,
                local_path: "./".to_string(),
                bach_size: 3,
                description: "".to_string(),
            };

            println!("download task is : {:?}", td);

            let list = td.get_object_list_to_file(None, "/tmp/obj_list.txt").await;

            // println!("list write result: {:?}", list);
        });
    }
}
