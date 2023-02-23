use aliyun_oss_client::BucketName;
use aws_credential_types::Credentials;

use aws_sdk_s3::Region;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum OssProvider {
    JD,
    Ali,
    AWS,
}

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

    pub async fn gen_oss_client(&self) -> OSSClient {
        match self.provider {
            OssProvider::JD => {
                let c = Credentials::new(
                    self.access_key_id.clone(),
                    self.secret_access_key.clone(),
                    None,
                    None,
                    "Static",
                );
                let shared_config = aws_config::from_env()
                    .credentials_provider(c)
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .load()
                    .await;

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
