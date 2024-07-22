use anyhow::anyhow;
use anyhow::Result;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::Client;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct FilePart {
    pub part_num: i32,
    pub offset: u64,
}

#[derive(Debug, Clone)]
pub struct StreamPart {
    pub part_num: i32,
    pub bytes: Vec<u8>,
}

// Todo 实验range下载，后再合并效率
fn main() {
    let shared_config = SdkConfig::builder()
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "JDC_2C0CFFB5148FAA30F6A4040E2EC8",
            "53A1D2814D6453027BEAC223F61E953E",
            None,
            None,
            "Static",
        )))
        .endpoint_url("http://s3-internal.cn-north-1.jdcloud-oss.com")
        .region(Region::new("cn-north-1"))
        .behavior_version(BehaviorVersion::latest())
        .build();

    let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);

    let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    let rt = tokio::runtime::Runtime::new().unwrap();
    let now = time::Instant::now();
    rt.block_on(async {
        let s_bucket = "jsw-bucket";
        let t_bucket = "jsw-bucket-1";
        let key = "exz1711022914858470037";
        let resp = client
            .get_object()
            .bucket(s_bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        let body_len = TryInto::<usize>::try_into(resp.content_length.unwrap()).unwrap();
        let arc_client = Arc::new(client);
        multipart_upload_object(
            arc_client,
            t_bucket,
            key,
            body_len,
            1024 * 1024 * 10,
            10,
            resp.body,
        )
        .await
        .unwrap();
    });
    println!("{:?}", now.elapsed());
}

pub async fn multipart_upload_object(
    client: Arc<Client>,
    bucket: &str,
    key: &str,
    // expires: Option<aws_smithy_types::DateTime>,
    body_len: usize,
    chunk_size: usize,
    multi_part_chunk_per_batch: usize,
    body: ByteStream,
) -> Result<()> {
    // 计算上传分片
    let mut content_len = body_len;
    let mut byte_stream_async_reader = body.into_async_read();
    // let completed_parts: Vec<CompletedPart> = Vec::new();

    let mut joinset = tokio::task::JoinSet::new();
    joinset.spawn(async move {});

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = match multipart_upload_res.upload_id() {
        Some(id) => id,
        None => {
            return Err(anyhow!("upload id is None"));
        }
    };

    let mut part_number = 0;
    let mut vec_stream_part = vec![];
    let completed_parts_btree: Arc<Mutex<BTreeMap<i32, CompletedPart>>> =
        Arc::new(Mutex::new(BTreeMap::new()));

    loop {
        part_number += 1;
        let buffer = match content_len >= chunk_size {
            true => {
                let mut buffer = vec![0; chunk_size];
                let _ = byte_stream_async_reader.read_exact(&mut buffer).await?;
                content_len -= chunk_size;
                buffer
            }
            false => {
                let mut buffer = vec![0; content_len];
                let _ = byte_stream_async_reader.read_exact(&mut buffer).await?;
                buffer
            }
        };
        let buf_len = buffer.len();

        let s_part = StreamPart {
            part_num: part_number,
            bytes: buffer,
        };

        vec_stream_part.push(s_part);

        if vec_stream_part.len().eq(&multi_part_chunk_per_batch)
            || (content_len == 0 || buf_len < chunk_size)
        {
            while joinset.len() > 8 {
                // task::yield_now().await;
                joinset.join_next().await;
            }

            let c = Arc::clone(&client);
            let up_id = upload_id.to_string();
            let v_s = vec_stream_part.clone();
            let b = bucket.to_string();
            let k = key.to_string();
            let c_b_t = Arc::clone(&completed_parts_btree);
            joinset.spawn(async move {
                println!("upload part");
                let _ = upload_stream_parts_batch(c, &b, &k, &up_id, v_s, c_b_t).await;
            });
            vec_stream_part.clear();
        }

        if content_len == 0 || buf_len < chunk_size {
            break;
        }
    }
    while joinset.len() > 0 {
        joinset.join_next().await;
    }

    let completed_parts = completed_parts_btree
        .lock()
        .await
        .clone()
        .into_values()
        .collect::<Vec<CompletedPart>>();

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    let complete_multi_part_upload_output = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;
    Ok(())
}

pub async fn upload_stream_parts_batch(
    client: Arc<Client>,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts_vec: Vec<StreamPart>,
    completed_parts_btree: Arc<Mutex<BTreeMap<i32, CompletedPart>>>,
) -> Result<()> {
    for p in parts_vec {
        println!("{}", p.part_num);
        let stream = ByteStream::from(p.bytes);
        let presigning = PresigningConfig::expires_in(std::time::Duration::from_secs(3000))?;

        let upload_part_res = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .body(stream)
            .part_number(p.part_num)
            // .send()
            .send_with_plugins(presigning)
            .await?;

        let completed_part = CompletedPart::builder()
            .e_tag(upload_part_res.e_tag.unwrap_or_default())
            .part_number(p.part_num)
            .build();
        let mut b_t = completed_parts_btree.lock().await;
        b_t.insert(p.part_num, completed_part);
    }
    Ok(())
}
