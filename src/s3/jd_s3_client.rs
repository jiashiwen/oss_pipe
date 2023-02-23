use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, Error, Result};
use aws_credential_types::Credentials;
use aws_sdk_s3::output::ListObjectsOutput;
use aws_sdk_s3::Client;
use aws_types::region::Region;

// #[derive(Clone)]
// pub struct JdS3 {
//     pub endpoint: String,
//     pub access_key: String,
//     pub secret_access_key: String,
//     pub region: String,
// }

// impl JdS3 {
//     pub async fn new_client(
//         endpoint_str: String,
//         ak: String,
//         sk: String,
//         region: String,
//     ) -> Client {
//         let c = Credentials::new(ak.as_str(), sk.as_str(), None, None, "Static");
//         let shared_config = aws_config::from_env()
//             .credentials_provider(c)
//             .endpoint_url(endpoint_str.clone())
//             .region(Region::new(region.clone()))
//             .load()
//             .await;

//         let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
//         let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
//         client
//     }
// }

pub async fn get_obj_list(
    client: &Client,
    bucket: String,
    prefix: String,
) -> Result<ListObjectsOutput> {
    let obj_list = client
        .list_objects()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .map_err(|e| Error::new(e))?;
    Ok(obj_list)
}

pub async fn download_obj(
    client: &Client,
    bucket: String,
    key: String,
    path: String,
) -> Result<()> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key.clone())
        .send()
        .await?;
    let data = resp.body.collect().await?;
    let bytes = data.into_bytes();

    let v: Vec<_> = key.split('/').collect();
    if let Some(filename) = v.last() {
        let mut store_to = path;
        if !store_to.as_str().ends_with("/") {
            store_to.push_str("/");
        }
        store_to.push_str(filename);

        let store_path = Path::new(store_to.as_str());
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(store_path)?;
        let _ = file.write(&*bytes);
    } else {
        return Err(anyhow!("no file found"));
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{
        commons::read_yaml_file,
        s3::oss::{OSSDescription, OssProvider},
    };

    use super::*;

    //cargo test s3::jd_s3_client::test::test_jdcloud_s3_client -- --nocapture
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
            let client = oss_jd.gen_oss_client().await;

            if let Some(jd) = client.jd_client {
                let obj_list = get_obj_list(&jd, "pingdata".to_string(), "pingdata/".to_string())
                    .await
                    .unwrap();

                for item in obj_list.contents().unwrap() {
                    println!("{:?}", item.key());
                }
            }
        });
    }

    //cargo test s3::jd_s3_client::test::test_jdcloud_s3_download -- --nocapture
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
            let client = oss_jd.gen_oss_client().await;

            if let Some(jd) = client.jd_client {
                let res = download_obj(
                    &jd,
                    "jsw-bucket".to_string(),
                    "ganzhi.rs".to_string(),
                    "/tmp/".to_string(),
                )
                .await;
                println!("{:?}", res);
            }
        });
    }
}
