use bytes::Bytes;
use infer::Infer;
use std::fs::OpenOptions;
use std::io::{LineWriter, Write};
use std::path::Path;

use aliyun_oss_client::file::Files;
use aliyun_oss_client::QueryKey;
use aliyun_oss_client::{Client, Query};
use anyhow::Result;
use anyhow::{anyhow, Ok};
use async_trait::async_trait;

use super::OssProvider;
use super::{OSSActions, OssObjectsList};

pub struct OssAliClient {
    pub client: Client,
}

#[async_trait]
impl OSSActions for OssAliClient {
    fn oss_client_type(&self) -> OssProvider {
        OssProvider::ALI
    }

    async fn list_objects(
        &self,
        _bucket: String,
        prefix: Option<String>,
        max_keys: i32,
        token: Option<String>,
    ) -> Result<super::OssObjectsList> {
        let mut q = Query::new();
        q.insert("max-keys", max_keys.to_string());

        if let Some(p) = prefix {
            q.insert(QueryKey::Prefix, p);
        };

        if let Some(t) = token {
            q.insert(QueryKey::ContinuationToken, t);
        };

        let list = self
            .client
            .clone()
            .get_object_list(q.clone())
            .await
            .map_err(|e| anyhow!(e.to_string()))
            .unwrap();

        let token = list.next_continuation_token().clone();

        let mut obj_list = None;

        let mut vec = vec![];
        for item in list.object_iter() {
            vec.push(item.path_string());
        }
        if vec.len() > 0 {
            obj_list = Some(vec);
        }

        let oss_list = OssObjectsList {
            object_list: obj_list,
            next_token: token,
        };
        Ok(oss_list)
    }

    async fn append_object_list_to_file(
        &self,
        _bucket: String,
        prefix: Option<String>,
        batch: i32,
        token: Option<String>,
        file_path: String,
    ) -> Result<Option<String>> {
        let mut q = Query::new();
        q.insert("max-keys", batch.to_string());

        if let Some(p) = prefix {
            q.insert(QueryKey::Prefix, p);
        };

        if let Some(t) = token {
            q.insert(QueryKey::ContinuationToken, t);
        };

        let list = self
            .client
            .clone()
            .get_object_list(q)
            .await
            .map_err(|e| anyhow!(e.to_string()))?;

        let token = list.next_continuation_token().clone();

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

        for item in list.object_iter() {
            let _ = file.write_all(item.path_string().as_bytes());
            let _ = file.write_all("\n".as_bytes());
        }
        file.flush()?;

        Ok(token)
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
        _bucket: String,
        key: String,
        dir: String,
    ) -> Result<()> {
        let resp = &self.client.get_object(key.clone(), ..).await?;
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
        let _ = file.write(&*resp);

        Ok(())
    }

    async fn download_objects_to_local(
        &self,
        _bucket: String,
        _keys: Vec<String>,
        _dir: String,
    ) -> Result<()> {
        Ok(())
    }

    async fn upload_object_from_local(
        &self,
        _bucket: String,
        key: String,
        file_path: String,
    ) -> Result<()> {
        let _ = self.client.put_file(file_path.clone(), key).await?;
        Ok(())
    }

    async fn get_object_bytes(&self, _bucket: &str, key: &str) -> Result<Bytes> {
        let resp = &self.client.get_object(key.to_string(), ..).await?;
        let bytes = Bytes::from(resp.clone());
        Ok(bytes)
    }

    async fn upload_object_bytes(&self, _bucket: &str, key: &str, content: Bytes) -> Result<()> {
        let get_content_type =
            |content: &Vec<u8>| Infer::new().get(content).map(|con| con.mime_type());

        self.client
            .put_content(content.to_vec(), key.to_string(), get_content_type)
            .await?;

        Ok(())
    }
}
