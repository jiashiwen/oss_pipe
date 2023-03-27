use anyhow::{Ok, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::fs::OpenOptions;
use std::io::{LineWriter, Write};
use std::path::Path;

use super::OSSActions;
use super::{OssObjectsList, OssProvider};

pub struct JRSSClient {
    pub client: Client,
}

#[async_trait]
impl OSSActions for JRSSClient {
    fn oss_client_type(&self) -> OssProvider {
        OssProvider::JRSS
    }
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
            .bucket(bucket)
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

    async fn append_object_list_to_file(
        &self,
        bucket: String,
        prefix: Option<String>,
        batch: i32,
        token: Option<String>,
        file_path: String,
    ) -> Result<Option<String>> {
        let mut obj_list = self
            .client
            .list_objects_v2()
            .bucket(bucket.clone())
            .max_keys(batch);
        if let Some(prefix_str) = prefix.clone() {
            obj_list = obj_list.prefix(prefix_str);
        }

        if let Some(token_str) = token.clone() {
            obj_list = obj_list.continuation_token(token_str);
        }

        let r = obj_list.send().await?;

        //写入文件
        let store_path = Path::new(file_path.as_str());
        let path = std::path::Path::new(store_path);

        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        };
        let file_ref = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(file_path.clone())?;
        let mut file = LineWriter::new(file_ref);
        if let Some(objects) = r.contents() {
            for item in objects.iter() {
                let _ = file.write_all(item.key().unwrap().as_bytes());
                let _ = file.write_all("\n".as_bytes());
            }
            file.flush()?;
        }

        return match r.next_continuation_token() {
            Some(str) => Ok(Some(str.to_string())),
            None => Ok(None),
        };
    }

    async fn append_all_object_list_to_file(
        &self,
        bucket: String,
        prefix: Option<String>,
        batch: i32,
        file_path: String,
    ) -> Result<()> {
        let resp = self
            .list_objects(bucket.clone(), prefix.clone(), batch, None)
            .await?;
        let mut token = resp.next_token;

        let store_path = Path::new(file_path.as_str());
        let path = std::path::Path::new(store_path);

        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        };

        //写入文件
        let file_ref = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(file_path.clone())?;
        let mut file = LineWriter::new(file_ref);
        if let Some(objects) = resp.object_list {
            for item in objects.iter() {
                let _ = file.write_all(item.as_bytes());
                let _ = file.write_all("\n".as_bytes());
            }
            file.flush()?;
        }

        while !token.is_none() {
            let resp = self
                .list_objects(bucket.clone(), prefix.clone(), batch, token.clone())
                .await?;
            if let Some(objects) = resp.object_list {
                for item in objects.iter() {
                    let _ = file.write_all(item.as_bytes());
                    let _ = file.write_all("\n".as_bytes());
                }
                file.flush()?;
            }
            token = resp.next_token;
        }

        Ok(())
    }

    async fn download_object_to_local(
        &self,
        bucket: String,
        key: String,
        dir: String,
    ) -> Result<()> {
        let resp = self
            .client
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
        file.flush()?;
        Ok(())
    }

    async fn download_objects_to_local(
        &self,
        bucket: String,
        keys: Vec<String>,
        dir: String,
    ) -> Result<()> {
        Ok(())
    }

    async fn upload_object_from_local(
        &self,
        bucket: String,
        key: String,
        file_path: String,
    ) -> Result<()> {
        let body = ByteStream::from_path(Path::new(&file_path)).await?;
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await?;

        Ok(())
    }

    async fn get_object_bytes(&self, bucket: String, key: String) -> Result<Bytes> {
        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key.clone())
            .send()
            .await?;
        let data = resp.body.collect().await?;
        let bytes = data.into_bytes();
        Ok(bytes)
    }

    async fn upload_object_bytes(&self, bucket: String, key: String, content: Bytes) -> Result<()> {
        let body = ByteStream::from(content);
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await?;
        Ok(())
    }
}
