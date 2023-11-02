use anyhow::{anyhow, Result};
use aws_sdk_s3::{
    model::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier},
    output::{CreateMultipartUploadOutput, GetObjectOutput},
    types::ByteStream,
    Client,
};
use std::{
    fs::{File, OpenOptions},
    io::{LineWriter, Read, Write},
    path::Path,
};
use tokio::io::AsyncReadExt;

use super::OssObjectsList;

#[derive(Debug, Clone)]
pub struct OssClient {
    pub client: Client,
}

impl OssClient {
    /// 将bucket中需要迁移的文件写入文件列表
    /// 返回写入文件的条数
    pub async fn append_all_object_list_to_file(
        &self,
        bucket: String,
        prefix: Option<String>,
        batch: i32,
        file_path: String,
    ) -> Result<usize> {
        let mut total = 0;
        let resp = self
            .list_objects(bucket.clone(), prefix.clone(), batch, None)
            .await?;
        let mut token = resp.next_token;

        let path = std::path::Path::new(file_path.as_str());
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
                total += 1;
            }
            file.flush()?;
        }

        while token.is_some() {
            let resp = self
                .list_objects(bucket.clone(), prefix.clone(), batch, token.clone())
                .await?;
            if let Some(objects) = resp.object_list {
                for item in objects.iter() {
                    let _ = file.write_all(item.as_bytes());
                    let _ = file.write_all("\n".as_bytes());
                    total += 1;
                }
                file.flush()?;
            }
            token = resp.next_token;
        }

        Ok(total)
    }

    // 将last_modify 大于某时间戳的object列表写入文件
    // ToDo 为提高效率需进行多线程改造
    // 并发写文件问题如何解决
    // 返回值为写入条目
    pub async fn append_last_modify_greater_object_to_file(
        &self,
        bucket: String,
        prefix: Option<String>,
        batch: i32,
        file_path: String,
        greater: i64,
    ) -> Result<usize> {
        let mut total = 0;
        let resp = self
            .list_objects(bucket.clone(), prefix.clone(), batch, None)
            .await?;
        let mut token = resp.next_token;

        let path = std::path::Path::new(file_path.as_str());
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
                let obj = self.get_object(&bucket, item).await?;
                if let Some(d) = obj.last_modified() {
                    if d.secs() > greater {
                        let _ = file.write_all(item.as_bytes());
                        let _ = file.write_all("\n".as_bytes());
                        total += 1;
                    }
                };
            }
            file.flush()?;
        }

        while token.is_some() {
            let resp = self
                .list_objects(bucket.clone(), prefix.clone(), batch, token.clone())
                .await?;
            if let Some(objects) = resp.object_list {
                for item in objects.iter() {
                    let obj = self.get_object(&bucket, item).await?;
                    if let Some(d) = obj.last_modified() {
                        if d.secs() > greater {
                            let _ = file.write_all(item.as_bytes());
                            let _ = file.write_all("\n".as_bytes());
                            total += 1;
                        }
                    };
                }
                file.flush()?;
            }
            token = resp.next_token;
        }
        Ok(total)
    }

    pub async fn append_object_list_to_file(
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

        if let Some(p) = store_path.parent() {
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

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<
        aws_sdk_s3::output::GetObjectOutput,
        aws_sdk_s3::types::SdkError<aws_sdk_s3::error::GetObjectError>,
    > {
        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key.clone())
            .send()
            .await?;
        std::result::Result::Ok(resp)
    }

    pub async fn remove_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<
        aws_sdk_s3::output::DeleteObjectOutput,
        aws_sdk_s3::types::SdkError<aws_sdk_s3::error::DeleteObjectError>,
    > {
        let resp = self
            .client
            .delete_object()
            .bucket(bucket)
            .key(key.clone())
            .send()
            .await?;
        std::result::Result::Ok(resp)
    }

    pub async fn remove_objects(
        &self,
        bucket: &str,
        keys: Vec<ObjectIdentifier>,
    ) -> std::result::Result<
        aws_sdk_s3::output::DeleteObjectsOutput,
        aws_sdk_s3::types::SdkError<aws_sdk_s3::error::DeleteObjectsError>,
    > {
        self.client
            .delete_objects()
            .bucket(bucket)
            .delete(Delete::builder().set_objects(Some(keys)).build())
            .send()
            .await
    }

    pub async fn get_object_etag(&self, bucket: &str, key: &str) -> Result<Option<String>> {
        let head = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        return match head.e_tag() {
            Some(t) => Ok(Some(t.to_string())),
            None => Ok(None),
        };
    }

    pub async fn list_objects(
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

    pub async fn transfer_object(
        &self,
        bucket: &str,
        key: &str,
        splite_size: usize,
        chunk_size: usize,
        object: GetObjectOutput,
    ) -> Result<()> {
        let content_len_usize: usize = object.content_length().try_into()?;
        let expr = match object.expires() {
            Some(d) => Some(*d),
            None => None,
        };
        return match content_len_usize.le(&splite_size) {
            true => {
                self.upload_object_bytes(bucket, key, expr, object.body)
                    .await
            }
            false => {
                self.multipart_upload_byte_stream(
                    bucket,
                    key,
                    expr,
                    content_len_usize,
                    chunk_size,
                    object.body,
                )
                .await
            }
        };
    }

    pub async fn multipart_upload_byte_stream(
        &self,
        bucket: &str,
        key: &str,
        expires: Option<aws_smithy_types::DateTime>,
        body_len: usize,
        chunk_size: usize,
        body: ByteStream,
    ) -> Result<()> {
        // 计算上传分片
        let mut content_len = body_len;
        let mut byte_stream_async_reader = body.into_async_read();
        let mut upload_parts: Vec<CompletedPart> = Vec::new();

        //获取上传id
        let multipart_upload_res = match expires {
            Some(datatime) => {
                self.client
                    .create_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .expires(datatime)
                    .send()
                    .await
            }
            None => {
                self.client
                    .create_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
            }
        }?;

        let upload_id = match multipart_upload_res.upload_id() {
            Some(id) => id,
            None => {
                return Err(anyhow!("upload id is None"));
            }
        };

        let mut part_num = 0;
        loop {
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
            let upload_part_res = self
                .client
                .upload_part()
                .key(key)
                .bucket(bucket)
                .upload_id(upload_id)
                .body(ByteStream::from(buffer))
                .part_number(part_num)
                .send()
                .await?;
            let completer_part = CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_num)
                .build();
            upload_parts.push(completer_part);
            part_num += 1;

            if content_len == 0 || buf_len < chunk_size {
                break;
            }
        }

        // 完成上传文件合并
        let completed_multipart_upload: CompletedMultipartUpload =
            CompletedMultipartUpload::builder()
                .set_parts(Some(upload_parts))
                .build();

        let _complete_multipart_upload_res = self
            .client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await?;

        Ok(())
    }

    //Todo 新增upload 函数，判断当 file size 大于 chunck size 时主动拆分
    pub async fn upload_from_local(
        &self,
        bucket: &str,
        key: &str,
        path: &str,
        file_max_size: usize,
        chuck_size: usize,
    ) -> Result<()> {
        let mut file = File::open(path)?;
        let file_meta = file.metadata()?;
        let file_max_size_u64 = TryInto::<u64>::try_into(file_max_size)?;
        if file_meta.len().le(&file_max_size_u64) {
            let body = ByteStream::from_path(Path::new(&path)).await?;
            self.client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body)
                .send()
                .await?;
            return Ok(());
        }

        self.multipart_upload_local_file(bucket, key, &mut file, chuck_size)
            .await
    }

    pub async fn upload_object_bytes(
        &self,
        bucket: &str,
        key: &str,
        expires: Option<aws_smithy_types::DateTime>,
        content: ByteStream,
    ) -> Result<()> {
        let mut upload = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(content);
        if let Some(exp) = expires {
            upload = upload.expires(exp);
        };
        upload.send().await?;
        Ok(())
    }

    // multipart upload
    pub async fn multipart_upload_local_file(
        &self,
        bucket: &str,
        key: &str,
        file: &mut File,
        chuck_size: usize,
    ) -> Result<()> {
        let mut part_number = 0;
        let mut upload_parts: Vec<CompletedPart> = Vec::new();

        //获取上传id
        let multipart_upload_res: CreateMultipartUploadOutput = self
            .client
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

        //分段上传文件并记录completer_part
        loop {
            let mut buf = vec![0; chuck_size];
            let read_count = file.read(&mut buf)?;
            part_number += 1;

            if read_count == 0 {
                break;
            }

            let body = &buf[..read_count];
            let stream = ByteStream::from(body.to_vec());

            let upload_part_res = self
                .client
                .upload_part()
                .key(key)
                .bucket(bucket)
                .upload_id(upload_id)
                .body(stream)
                .part_number(part_number)
                .send()
                .await?;

            let completed_part = CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build();

            upload_parts.push(completed_part);

            if read_count != chuck_size {
                break;
            }
        }
        // 完成上传文件合并
        let completed_multipart_upload: CompletedMultipartUpload =
            CompletedMultipartUpload::builder()
                .set_parts(Some(upload_parts))
                .build();

        let _complete_multipart_upload_res = self
            .client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await?;
        Ok(())
    }

    pub async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool> {
        let mut exist = true;
        if let Err(e) = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            let err = e.into_service_error();
            if err.is_not_found() {
                exist = false
            } else {
                return Err(anyhow::Error::new(err));
            }
        };
        Ok(exist)
    }
}

pub async fn oss_download_to_file(
    get_object: GetObjectOutput,
    file: &mut File,
    splite_size: usize,
    chunk_size: usize,
) -> Result<()> {
    let mut content_len_usize: usize = get_object.content_length().try_into()?;
    if content_len_usize.le(&splite_size) {
        let content = get_object.body.collect().await?;
        let bytes = content.into_bytes();
        file.write_all(&bytes)?;
        file.flush()?;
        return Ok(());
    }

    let mut byte_stream_async_reader = get_object.body.into_async_read();
    loop {
        if content_len_usize > chunk_size {
            let mut buffer = vec![0; chunk_size];
            let _ = byte_stream_async_reader.read_exact(&mut buffer).await?;
            file.write_all(&buffer)?;
            content_len_usize -= chunk_size;
            continue;
        } else {
            let mut buffer = vec![0; content_len_usize];
            let _ = byte_stream_async_reader.read_exact(&mut buffer).await?;
            file.write_all(&buffer)?;
            break;
        }
    }

    file.flush()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::{fs::OpenOptions, io::Write};

    use aws_sdk_s3::types::ByteStream;
    use tokio::io::{AsyncBufReadExt, BufReader};

    use crate::{commons::rand_string, s3::OSSDescription};

    //cargo test s3::aws_s3::test::test_append_last_modify_greater_object_to_file -- --nocapture
    #[test]
    fn test_append_last_modify_greater_object_to_file() {
        // 获取oss连接参数
        let vec_oss = crate::commons::read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let oss_desc = vec_oss[0].clone();
        let jd_client = oss_desc.gen_oss_client().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let r = jd_client
                .append_last_modify_greater_object_to_file(
                    "jsw-bucket-1".to_string(),
                    None,
                    100,
                    "/tmp/objlist".to_string(),
                    1689642000,
                )
                .await;

            if let Err(e) = r {
                println!("{}", e.to_string());
                return;
            }
        });
    }

    //cargo test s3::aws_s3::test::test_vec_to_byte_stream -- --nocapture
    #[test]
    fn test_vec_to_byte_stream() {
        // 获取oss连接参数
        let vec_oss = crate::commons::read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let oss_desc = vec_oss[0].clone();
        let jd_client = oss_desc.gen_oss_client().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut vec_line = vec![];

        for _ in 0..99 {
            let mut s = rand_string(8);
            s.push('\n');
            vec_line.push(s.into_bytes());
        }

        let vec_u8 = vec_line.into_iter().flatten().collect::<Vec<u8>>();

        // file.flush().unwrap();
        let stream = ByteStream::from(vec_u8);

        rt.block_on(async {
            let _ = jd_client
                .upload_object_bytes("jsw-bucket-1", "line_file", None, stream)
                .await;
        });
    }
}
