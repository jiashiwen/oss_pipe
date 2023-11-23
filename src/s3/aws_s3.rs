use anyhow::{anyhow, Result};
use aws_sdk_s3::{
    model::{CompletedMultipartUpload, CompletedPart, Delete, Object, ObjectIdentifier},
    output::{CreateMultipartUploadOutput, GetObjectOutput},
    types::ByteStream,
    Client,
};
use dashmap::DashMap;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, LineWriter, Lines, Read, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncReadExt;

use crate::{
    checkpoint::{FileDescription, FilePosition, Opt, RecordDescription},
    commons::{merge_file, size_distributed, LastModifyFilter, RegexFilter},
    tasks::{gen_file_path, MODIFIED_PREFIX, OBJECT_LIST_FILE_PREFIX, REMOVED_PREFIX},
};

#[derive(Debug, Clone)]
pub struct OssClient {
    pub client: Client,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OssObjList {
    pub object_list: Option<Vec<Object>>,
    pub next_token: Option<String>,
}

impl OssClient {
    pub async fn append_object_list_to_file(
        &self,
        bucket: String,
        prefix: Option<String>,
        batch: i32,
        file_path: &str,
        last_modify_filter: Option<LastModifyFilter>,
    ) -> Result<FileDescription> {
        let mut total_lines = 0;
        let path = std::path::Path::new(file_path);
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        };
        //准备写入文件
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(file_path)?;
        let mut line_writer = LineWriter::new(&file);

        let resp = self
            .list_objects(bucket.clone(), prefix.clone(), batch, None)
            .await?;
        let mut token = resp.next_token;

        if let Some(objects) = resp.object_list {
            for item in objects {
                if let Some(f) = last_modify_filter {
                    if let Some(d) = item.last_modified() {
                        if !f.filter(i128::from(d.secs())) {
                            continue;
                        }
                    }
                }
                if let Some(key) = item.key() {
                    let _ = line_writer.write_all(key.as_bytes());
                    let _ = line_writer.write_all("\n".as_bytes());
                    total_lines += 1;
                }
            }
            line_writer.flush()?;
        }

        while token.is_some() {
            let resp = self
                .list_objects(bucket.clone(), prefix.clone(), batch, token.clone())
                .await?;
            if let Some(objects) = resp.object_list {
                for item in objects {
                    if let Some(f) = last_modify_filter {
                        if let Some(d) = item.last_modified() {
                            if !f.filter(i128::from(d.secs())) {
                                continue;
                            }
                        }
                    }
                    if let Some(key) = item.key() {
                        let _ = line_writer.write_all(key.as_bytes());
                        let _ = line_writer.write_all("\n".as_bytes());
                        total_lines += 1;
                    }
                }
                line_writer.flush()?;
            }
            token = resp.next_token;
        }
        let size = file.metadata()?.len();
        let execute_file = FileDescription {
            path: file_path.to_string(),
            size,
            total_lines,
        };
        Ok(execute_file)
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
    ) -> Result<OssObjList> {
        let mut obj_list = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(max_keys);

        if let Some(prefix_str) = &prefix {
            obj_list = obj_list.prefix(prefix_str.to_string());
        }

        if let Some(token_str) = token.clone() {
            obj_list = obj_list.continuation_token(token_str);
        }

        let list = obj_list.send().await?;

        let mut obj_list = None;

        if let Some(l) = list.contents() {
            let mut vec = vec![];
            for item in l.iter() {
                vec.push(item.clone());
            }
            if vec.len() > 0 {
                obj_list = Some(vec);
            }
        };
        let mut token = None;
        if let Some(str) = list.next_continuation_token() {
            token = Some(str.to_string());
        };

        let oss_list = OssObjList {
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

    pub async fn upload(
        &self,
        bucket: &str,
        key: &str,
        local_file: &str,
        file_max_size: usize,
        chuck_size: usize,
    ) -> Result<()> {
        let mut file = File::open(local_file)?;
        let file_meta = file.metadata()?;
        let file_max_size_u64 = TryInto::<u64>::try_into(file_max_size)?;
        if file_meta.len().le(&file_max_size_u64) {
            let body = ByteStream::from_path(Path::new(&local_file)).await?;
            self.client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body)
                .send()
                .await?;
            return Ok(());
        }
        self.multipart_upload_local(bucket, key, &mut file, chuck_size)
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
    pub async fn multipart_upload_local(
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

    pub async fn object_exists(
        &self,
        bucket: impl Into<std::string::String>,
        key: impl Into<std::string::String>,
    ) -> Result<bool> {
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
    pub async fn object_exists_string(&self, bucket: String, key: String) -> Result<bool> {
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
    pub async fn changed_object_capture(
        &self,
        bucket: &str,
        source_prefix: Option<String>,
        target_prefix: Option<String>,
        out_put_dir: &str,
        timestampe: i64,
        list_file_path: &str,
        batch_size: i32,
        multi_part_chunk: usize,
    ) -> Result<(FileDescription, FileDescription, i64)> {
        // let mut set = JoinSet::new();
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

        let new_object_list = gen_file_path(
            out_put_dir,
            OBJECT_LIST_FILE_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let removed = gen_file_path(
            out_put_dir,
            REMOVED_PREFIX,
            now.as_secs().to_string().as_str(),
        );
        let modified = gen_file_path(
            out_put_dir,
            MODIFIED_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let (mut modified_description, new_list_description) = self
            .capture_modified_objects(
                bucket,
                source_prefix.clone(),
                target_prefix.clone(),
                timestampe,
                &modified,
                &new_object_list,
                batch_size,
            )
            .await?;

        let removed_description = self
            .capture_removed_objects(bucket, target_prefix.clone(), list_file_path, &removed)
            .await?;

        if modified_description.size.gt(&0) {
            merge_file(
                &modified_description.path,
                &removed_description.path,
                multi_part_chunk,
            )?;
        }

        let timestampe = now.as_secs().try_into()?;
        modified_description.size = modified_description.size + removed_description.size;
        modified_description.total_lines =
            modified_description.total_lines + removed_description.total_lines;

        fs::rename(&removed_description.path, &modified_description.path)?;
        Ok((modified_description, new_list_description, timestampe))
    }

    async fn capture_removed_objects(
        &self,
        bucket: &str,
        target_prefix: Option<String>,
        current_object_list: &str,
        out_put: &str,
    ) -> Result<FileDescription> {
        let obj_list_file = File::open(current_object_list)?;

        let mut list_file_position = FilePosition::default();
        let mut out_put_file_total_lines = 0;

        let out_put_path = Path::new(out_put);
        if let Some(p) = out_put_path.parent() {
            if !p.exists() {
                std::fs::create_dir_all(p)?;
            }
        };
        let out_put_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(out_put_path)?;

        let lines: Lines<BufReader<File>> = BufReader::new(obj_list_file).lines();
        for line in lines {
            if let Result::Ok(key) = line {
                let len = key.bytes().len() + "\n".bytes().len();
                list_file_position.offset += len;
                list_file_position.line_num += 1;

                let mut target_key = match &target_prefix {
                    Some(s) => s.to_string(),
                    None => "".to_string(),
                };
                target_key.push_str(&key);

                if !self.object_exists(bucket, &key).await? {
                    // 填充变动对象文件
                    let record = RecordDescription {
                        source_key: key,
                        target_key,
                        list_file_path: current_object_list.to_string(),
                        list_file_position,
                        option: Opt::REMOVE,
                    };
                    let _ = record.save_json_to_file(&out_put_file);
                    out_put_file_total_lines += 1;
                };
            }
        }

        let size = out_put_file.metadata()?.len();
        let out_put_description = FileDescription {
            path: out_put.to_string(),
            size,
            total_lines: out_put_file_total_lines,
        };

        Ok(out_put_description)
    }

    async fn capture_modified_objects(
        &self,
        bucket: &str,
        source_prefix: Option<String>,
        target_prefix: Option<String>,
        timestampe: i64,
        out_put_modified: &str,
        out_put_new_list: &str,
        batch_size: i32,
    ) -> Result<(FileDescription, FileDescription)> {
        let modified_path = Path::new(out_put_modified);
        if let Some(p) = modified_path.parent() {
            if !p.exists() {
                std::fs::create_dir_all(p)?;
            }
        };

        let new_list_path = Path::new(out_put_new_list);
        if let Some(p) = new_list_path.parent() {
            if !p.exists() {
                std::fs::create_dir_all(p)?;
            }
        };

        let mut modified_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&modified_path)?;
        let mut new_list_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&new_list_path)?;

        let resp = self
            .list_objects(bucket.to_string(), source_prefix.clone(), batch_size, None)
            .await?;
        let mut token = resp.next_token;

        let mut new_list_total_lines = 0;
        let mut modified_total_lines = 0;

        if let Some(objects) = resp.object_list {
            for obj in objects {
                if let Some(key) = obj.key() {
                    let _ = new_list_file.write_all(key.as_bytes());
                    let _ = new_list_file.write_all("\n".as_bytes());
                    new_list_total_lines += 1;
                    if let Some(d) = obj.last_modified() {
                        if d.secs().ge(&timestampe) {
                            // 填充变动对象文件
                            let mut target_key = match &target_prefix {
                                Some(s) => s.to_string(),
                                None => "".to_string(),
                            };
                            target_key.push_str(&key);

                            let record = RecordDescription {
                                source_key: key.to_string(),
                                target_key,
                                list_file_path: "".to_string(),
                                list_file_position: FilePosition::default(),
                                option: Opt::PUT,
                            };

                            if let Err(e) = record.save_json_to_file(&modified_file) {
                                log::error!("{}", e);
                                continue;
                            }
                            modified_total_lines += 1;
                        }
                    }
                }
            }
        }

        while token.is_some() {
            let resp = self
                .list_objects(bucket.to_string(), source_prefix.clone(), batch_size, token)
                .await?;
            if let Some(objects) = resp.object_list {
                for obj in objects {
                    if let Some(key) = obj.key() {
                        let _ = new_list_file.write_all(key.as_bytes());
                        let _ = new_list_file.write_all("\n".as_bytes());
                        new_list_total_lines += 1;
                        if let Some(d) = obj.last_modified() {
                            if d.secs().ge(&timestampe) {
                                // 填充变动对象文件
                                let mut target_key = match &target_prefix {
                                    Some(s) => s.to_string(),
                                    None => "".to_string(),
                                };
                                target_key.push_str(&key);

                                let record = RecordDescription {
                                    source_key: key.to_string(),
                                    target_key,
                                    list_file_path: "".to_string(),
                                    list_file_position: FilePosition::default(),
                                    option: Opt::PUT,
                                };
                                if let Err(e) = record.save_json_to_file(&modified_file) {
                                    log::error!("{}", e);
                                    continue;
                                }
                                modified_total_lines += 1;
                            }
                        }
                    }
                }
            }
            token = resp.next_token;
        }

        modified_file.flush()?;
        new_list_file.flush()?;

        let modified_size = modified_file.metadata()?.len();
        let new_list_size = new_list_file.metadata()?.len();
        let modified_file_description = FileDescription {
            path: out_put_modified.to_string(),
            size: modified_size,
            total_lines: modified_total_lines,
        };
        let new_list_description = FileDescription {
            path: out_put_new_list.to_string(),
            size: new_list_size,
            total_lines: new_list_total_lines,
        };

        Ok((modified_file_description, new_list_description))
    }

    pub async fn analyze_objects_size(
        &self,
        bucket: &str,
        prefix: Option<String>,
        regex_filter: Option<RegexFilter>,
        last_modify_filter: Option<LastModifyFilter>,
        batch_size: i32,
    ) -> Result<DashMap<String, i128>> {
        let size_map = DashMap::<String, i128>::new();
        let resp = self
            .list_objects(bucket.to_string(), prefix.clone(), batch_size, None)
            .await?;
        let mut token = resp.next_token;

        if let Some(objects) = resp.object_list {
            for obj in objects.into_iter() {
                let key = match obj.key() {
                    Some(k) => k,
                    None => {
                        continue;
                    }
                };

                if let Some(p) = &prefix {
                    if !key.starts_with(p) {
                        continue;
                    }
                }

                if let Some(f) = &regex_filter {
                    if !f.filter(key) {
                        continue;
                    }
                }

                if let Some(f) = last_modify_filter {
                    if let Some(d) = obj.last_modified() {
                        if !f.filter(i128::from(d.secs())) {
                            continue;
                        }
                    }
                }

                let obj_size = i128::from(obj.size());
                let map_key = size_distributed(obj_size);
                let mut map_val = match size_map.get(&map_key) {
                    Some(m) => *m.value(),
                    None => 0,
                };
                map_val += 1;
                size_map.insert(map_key, map_val);
            }
        }

        while token.is_some() {
            let resp = self
                .list_objects(bucket.to_string(), prefix.clone(), batch_size, token)
                .await?;
            if let Some(objects) = resp.object_list {
                for obj in objects.into_iter() {
                    let key = match obj.key() {
                        Some(k) => k,
                        None => {
                            continue;
                        }
                    };

                    if let Some(p) = &prefix {
                        if !key.starts_with(p) {
                            continue;
                        }
                    }

                    if let Some(f) = &regex_filter {
                        if !f.filter(key) {
                            continue;
                        }
                    }

                    if let Some(f) = last_modify_filter {
                        if let Some(d) = obj.last_modified() {
                            if !f.filter(i128::from(d.secs())) {
                                continue;
                            }
                        }
                    }
                    let obj_size = i128::from(obj.size());
                    let map_key = size_distributed(obj_size);
                    let mut map_val = match size_map.get(&map_key) {
                        Some(m) => *m.value(),
                        None => 0,
                    };
                    map_val += 1;
                    size_map.insert(map_key, map_val);
                }
            }
            token = resp.next_token;
        }

        Ok(size_map)
    }
}

pub async fn download_object(
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

    use aws_sdk_s3::types::ByteStream;

    use crate::{commons::rand_string, s3::OSSDescription};

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
