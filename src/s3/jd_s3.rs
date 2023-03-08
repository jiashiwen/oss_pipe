use std::fs::{File, OpenOptions};
use std::io::{LineWriter, Write};
use std::path::Path;

use anyhow::{anyhow, Error, Ok, Result};
use async_trait::async_trait;
use aws_sdk_s3::output::{ListObjectsOutput, ListObjectsV2Output};
use aws_sdk_s3::Client;

use super::{OSSActions, OSSClient, OssObjectsList};

pub struct OssJdClient {
    pub client: Client,
}

#[async_trait]
impl OSSActions for OssJdClient {
    async fn list_objects(
        &self,
        bucket: String,
        prefix: Option<String>,
        max_keys: i32,
        token: Option<String>,
    ) -> Result<super::OssObjectsList> {
        let mut obj_list = self
            .client
            .list_objects_v2()
            .bucket(bucket.clone())
            .max_keys(max_keys);
        if let Some(prefix_str) = prefix.clone() {
            obj_list = obj_list.prefix(prefix_str);
        }

        if let Some(token_str) = token.clone() {
            obj_list = obj_list.continuation_token(token_str);
        }

        let list = obj_list.send().await?;

        let mut obj_list = None;

        if let Some(l) = list.contents() {
            let mut vec = vec![];
            for item in l.iter() {
                if let Some(str) = item.key() {
                    vec.push(str.to_string());
                };
            }
            if vec.len() > 0 {
                obj_list = Some(vec);
            }
        };
        let mut token = None;
        if let Some(str) = list.next_continuation_token() {
            token = Some(str.to_string());
        };

        let oss_list = OssObjectsList {
            object_list: obj_list,
            next_token: token,
        };
        Ok(oss_list)
    }
}

impl OSSClient {
    pub async fn jd_object_list_to_file(
        &self,
        prefix: Option<String>,
        bucket: String,
        file_path: String,
        batch: i32,
        token: Option<String>,
    ) -> Result<Option<String>> {
        if let Some(jd) = &self.jd_client {
            let mut obj_list = jd.list_objects_v2().bucket(bucket.clone()).max_keys(batch);
            if let Some(prefix_str) = prefix.clone() {
                obj_list = obj_list.prefix(prefix_str);
            }

            if let Some(token_str) = token.clone() {
                obj_list = obj_list.continuation_token(token_str);
            }

            let r = obj_list.send().await?;

            //写入文件
            let file_ref = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(file_path.clone())?;
            let mut file = LineWriter::new(file_ref);
            for item in r.contents().unwrap().iter() {
                let _ = file.write_all(item.key().unwrap().as_bytes());
                let _ = file.write_all("\n".as_bytes());
            }
            file.flush()?;

            return match r.next_continuation_token() {
                Some(str) => Ok(Some(str.to_string())),
                None => Ok(None),
            };
        } else {
            Err(anyhow!("not jd oss client"))
        }
    }

    pub async fn jd_get_object_list(
        &self,
        bucket: String,
        prefix: String,
    ) -> Result<ListObjectsOutput> {
        if let Some(jd) = &self.jd_client {
            let obj_list = jd
                .list_objects()
                .bucket(bucket)
                .prefix(prefix)
                .send()
                .await
                .map_err(|e| Error::new(e))?;
            Ok(obj_list)
        } else {
            Err(anyhow!("not jd oss client"))
        }
    }

    pub async fn jd_get_object_list_v2(
        &self,
        bucket: String,
        prefix: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectsV2Output> {
        if let Some(jd) = &self.jd_client {
            return match prefix {
                Some(prefix) => {
                    let obj_list = jd
                        .list_objects_v2()
                        .bucket(bucket)
                        .prefix(prefix)
                        .max_keys(max_keys)
                        .send()
                        .await
                        .map_err(|e| Error::new(e))?;
                    Ok(obj_list)
                }
                None => {
                    let obj_list = jd
                        .list_objects_v2()
                        .bucket(bucket)
                        .send()
                        .await
                        .map_err(|e| Error::new(e))?;
                    Ok(obj_list)
                }
            };
        } else {
            Err(anyhow!("not jd oss client"))
        }
    }

    pub async fn jd_download_object_to_dir(
        &self,
        bucket: String,
        key: String,
        dir: String,
    ) -> Result<()> {
        if let Some(jd) = &self.jd_client {
            let resp = jd
                .get_object()
                .bucket(bucket)
                .key(key.clone())
                .send()
                .await?;
            let data = resp.body.collect().await?;
            let bytes = data.into_bytes();
            let mut store = dir.clone();
            store.push_str("/");
            store.push_str(&key);

            let store_path = Path::new(store.as_str());

            let path = std::path::Path::new(store_path);

            if let Some(p) = path.parent() {
                std::fs::create_dir_all(p)?;
            };

            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(store_path)?;
            let _ = file.write(&*bytes);
            Ok(())
        } else {
            Err(anyhow!("not jd oss client"))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        commons::read_yaml_file,
        s3::oss::{OSSDescription, OssProvider},
    };

    use super::*;

    //cargo test s3::jd_s3::test::test_jdcloud_s3_client -- --nocapture
    #[test]
    fn test_jdcloud_s3_client() {
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
            let client = oss_jd.gen_oss_client();

            let obj_list = client
                .jd_get_object_list_v2("jsw-bucket".to_string(), None, 2)
                .await
                .unwrap();

            for item in obj_list.contents().unwrap() {
                println!("{:?}", item.key());
            }
        });
    }

    //cargo test s3::jd_s3::test::test_jdcloud_s3_download -- --nocapture
    #[test]
    fn test_jdcloud_s3_download() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let vec_oss = read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let mut oss_jd = OSSDescription::default();
        for item in vec_oss.iter() {
            if item.provider == OssProvider::JD {
                oss_jd = item.clone();
            }
        }

        rt.block_on(async {
            let client = oss_jd.gen_oss_client();

            let r = client
                .jd_download_object_to_dir(
                    "jsw-bucket".to_string(),
                    "test/men.rs".to_string(),
                    "/tmp".to_string(),
                )
                .await;
            println!("{:?}", r);
        });
    }
}
