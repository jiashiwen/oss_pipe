extern crate core;

use std::fs::File;
use std::io::Write;

use aws_sdk_s3::{Client, Endpoint};
use aws_types::region::Region;
// use aws_types::{Credentials, SdkConfig};
use aws_credential_types::Credentials;
use aws_types::SdkConfig;
use http::Uri;

// use aws_types::region::Region;

// snippet-end:[s3.rust.client-use]

/// Lists your buckets.
#[tokio::main]
async fn main() -> Result<(), aws_sdk_s3::Error> {
    // let endpoint = Endpoint::immutable(Uri::from_static("http://s3.cn-north-1.jdcloud-oss.com"));
    let c = Credentials::new(
        "4107B314B15BCE99A1C781DFCF119F59",
        "8877CD432EB5738EFF0FA01F630201C9",
        None,
        None,
        "Static",
    );

    let shared_config = aws_config::from_env()
        .credentials_provider(c)
        .endpoint_url("http://s3.cn-north-1.jdcloud-oss.com")
        .region(Region::new("cn-north-1"))
        .load()
        .await;

    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
    // s3_config_builder = s3_config_builder.endpoint_resolver(endpoint);
    let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    let obj_list = client
        .list_objects()
        .bucket("pingdata")
        .prefix("pingdata/5aa4d3ed66bfb7cb8938563d2bb517cd")
        .send()
        .await?;

    for item in obj_list.contents().unwrap() {
        println!("{:?}", item.key());
    }

    let resp = client
        .get_object()
        .bucket("tsp-picture")
        .key("tsp-picture/46b6d4e6-1446-4da2-a10b-91fd3d73cebb.jpg")
        .send()
        .await?;

    // let buckets = resp.buckets().unwrap_or_default();
    // let num_buckets = buckets.len();

    // for bucket in buckets {
    //     println!("{}", bucket.name().unwrap_or_default());
    // }
    let data = resp.body.collect().await;

    // println!("Found {:?} buckets.", data);
    let bytes = data.unwrap().into_bytes();

    let mut file = File::create("/tmp/test.jpg").unwrap();

    file.write(&*bytes);

    Ok(())
}
