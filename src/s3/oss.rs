use aliyun_oss_client::BucketName;
use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};

use anyhow::Result;
use aws_sdk_s3::Region;
use serde::{Deserialize, Serialize};

use super::{ali_oss::OssAliClient, jd_s3::OssJdClient};

#[async_trait]
pub trait OSSActions {
    async fn list_objects(
        &self,
        bucket: String,
        prefix: Option<String>,
        max_keys: i32,
        token: Option<String>,
    ) -> Result<OssObjectsList>;
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum OssProvider {
    JD,
    Ali,
    AWS,
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OssObjectsList {
    pub object_list: Option<Vec<String>>,
    pub next_token: Option<String>,
}

#[derive(Clone)]
pub struct OSSClient {
    pub jd_client: Option<aws_sdk_s3::Client>,
    pub ali_client: Option<aliyun_oss_client::Client>,
    // AWSClient(aws_sdk_s3::Client),
}

impl Default for OSSClient {
    fn default() -> Self {
        Self {
            jd_client: None,
            ali_client: None,
        }
    }
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OSSDescription {
    pub provider: OssProvider,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
}

impl Default for OSSDescription {
    fn default() -> Self {
        Self {
            provider: OssProvider::JD,
            access_key_id: "access_key_id".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            endpoint: "http://s3.cn-north-1.jdcloud-oss.com".to_string(),
            region: "cn-north-1".to_string(),
            bucket: "bucket_name".to_string(),
        }
    }
}
impl OSSDescription {
    /// Creates a new [`OSSDescription`].
    pub fn new(
        access_key_id: String,
        provider: OssProvider,
        secret_access_key: String,
        endpoint: String,
        region: String,
        bucket: String,
    ) -> Self {
        Self {
            provider,
            access_key_id,
            secret_access_key,
            bucket,
            endpoint,
            region,
        }
    }

    pub fn get_oss(&self) -> Result<Box<dyn OSSActions>> {
        match self.provider {
            OssProvider::JD => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .build();

                let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let jdclient = OssJdClient { client };
                Ok(Box::new(jdclient))
            }
            OssProvider::Ali => {
                let bucket = BucketName::new(self.bucket.clone())?;
                let client = aliyun_oss_client::Client::new(
                    self.access_key_id.clone().into(),
                    self.secret_access_key.clone().into(),
                    self.endpoint.clone().into(),
                    bucket,
                );
                // let mut client = OSSClient::default();
                let ali_client = OssAliClient { client };
                Ok(Box::new(ali_client))
                // client.ali_client = Some(ali_client);
                // client
            }
            OssProvider::AWS => todo!(),
        }
    }

    pub fn gen_oss_client(&self) -> OSSClient {
        match self.provider {
            OssProvider::JD => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .build();

                let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let mut jd_client = OSSClient::default();
                jd_client.jd_client = Some(client);
                jd_client
            }
            OssProvider::Ali => {
                let bucket = BucketName::new(self.bucket.clone()).unwrap();
                let ali_client = aliyun_oss_client::Client::new(
                    self.access_key_id.clone().into(),
                    self.secret_access_key.clone().into(),
                    self.endpoint.clone().into(),
                    bucket,
                );
                let mut client = OSSClient::default();
                client.ali_client = Some(ali_client);
                client
            }
            OssProvider::AWS => todo!(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::commons::read_yaml_file;

    use super::{OSSDescription, OssProvider};

    //cargo test s3::oss::test::test_ossaction -- --nocapture
    #[test]
    fn test_ossaction() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let vec_oss = read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let mut oss_jd = OSSDescription::default();
        for item in vec_oss.iter() {
            if item.provider == OssProvider::JD {
                oss_jd = item.clone();
            }
        }
        let jd = oss_jd.get_oss();
        rt.block_on(async {
            let client = jd.unwrap();
            let r = client
                .list_objects("jsw-bucket".to_string(), None, 10, None)
                .await;
            println!("{:?}", r);
            let r1 = client
                .list_objects("jsw-bucket".to_string(), None, 10, r.unwrap().next_token)
                .await;
            println!("{:?}", r1);
        });
    }
}
