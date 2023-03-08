use aliyun_oss_client::QueryKey;
use aliyun_oss_client::{file::File, object::ObjectList, BucketName, Client, Query};
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;

use super::OSSClient;
use super::{OSSActions, OssObjectsList};

impl OSSClient {
    pub async fn ali_get_objects_list(self, query: Query) -> Result<ObjectList> {
        if let Some(ali) = self.ali_client {
            let list = ali
                .get_object_list(query)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
            Ok(list)
        } else {
            Err(anyhow!("not ali client"))
        }
    }

    pub async fn ali_get_object(self, object_path: &str) -> Result<Vec<u8>> {
        if let Some(ali) = self.ali_client {
            let r = &ali.get_object(object_path, ..).await?;
            Ok(r.clone())
        } else {
            Err(anyhow!("not ali client"))
        }
    }
}

pub struct OssAliClient {
    pub client: Client,
}

#[async_trait]
impl OSSActions for OssAliClient {
    async fn list_objects(
        &self,
        bucket: String,
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
            .get_object_list(q)
            .await
            .map_err(|e| anyhow!(e.to_string()))?;

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
}

#[cfg(test)]
mod test {

    use aliyun_oss_client::{Query, QueryKey};

    use crate::{
        commons::read_yaml_file,
        s3::{OSSDescription, OssProvider},
    };

    //cargo test s3::ali_oss::test::test_async_get_object -- --nocapture
    #[test]
    fn test_async_get_object() {
        let vec_oss = read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let mut oss_ali = OSSDescription::default();
        for item in vec_oss.iter() {
            if item.provider == OssProvider::Ali {
                oss_ali = item.clone();
            }
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let client = oss_ali.gen_oss_client();
            let filevec = client.ali_get_object("test/men.rs").await.unwrap();
            std::fs::write("/tmp/test", filevec).unwrap();
        })
    }

    //cargo test s3::ali_oss::test::test_ali_get_objects_list -- --nocapture
    #[test]
    fn test_ali_get_objects_list() {
        let vec_oss = read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let mut oss_ali = OSSDescription::default();
        for item in vec_oss.iter() {
            if item.provider == OssProvider::Ali {
                oss_ali = item.clone();
            }
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let client = &oss_ali.gen_oss_client();
            let mut query = Query::new();
            query.insert("max-keys", 3u16);

            let res = client
                .clone()
                .ali_get_objects_list(query.clone())
                .await
                .unwrap();

            let mut value = res.next_continuation_token().clone();
            for item in res.object_iter() {
                println!("objects list: {:?}", item.path_string());
            }

            while value.clone() != None {
                let v = value.clone().unwrap();
                let mut q = query.clone();
                q.insert(QueryKey::ContinuationToken, v.to_owned());
                let res1 = client.clone().ali_get_objects_list(q).await.unwrap();
                value = res1.next_continuation_token().clone();
                for item in res1.object_iter() {
                    println!("objects list1: {:?}", item.path_string());
                }
            }
        });
    }
}
