use crate::{
    checkpoint::FileDescription,
    commons::{
        fill_file_with_zero, gen_file_part_plan, size_distributed, FilePart, LastModifyFilter,
        RegexFilter, DOWNLOAD_TMP_FILE_SUBFFIX,
    },
};
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::operation::{
    abort_multipart_upload::AbortMultipartUploadOutput,
    complete_multipart_upload::CompleteMultipartUploadOutput, get_object::GetObjectOutput,
};
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::types::Delete;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::types::ObjectIdentifier;
use aws_sdk_s3::Client;
use aws_sdk_s3::{
    operation::create_multipart_upload::CreateMultipartUploadOutput, presigning::PresigningConfig,
};
use aws_smithy_types::{body::SdkBody, byte_stream::ByteStream};
use dashmap::DashMap;

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{LineWriter, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, RwLock},
    task::{self, JoinSet},
};

#[derive(Debug, Clone)]
pub struct ObjectRange {
    pub part_num: i32,
    pub begin: usize,
    pub end: usize,
}

//Todo 尝试修改为Arc::<Client>
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
            std::fs::create_dir_all(p).context(format!("{}:{}", file!(), line!()))?;
        };
        //准备写入文件
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(file_path)?;
        let mut line_writer = LineWriter::new(&file);

        let mut process_objects = |objects: Vec<Object>| -> Result<()> {
            for obj in objects {
                if let Some(f) = last_modify_filter {
                    if let Some(d) = obj.last_modified() {
                        if !f.filter(usize::try_from(d.secs())?) {
                            continue;
                        }
                    }
                }

                if let Some(key) = obj.key() {
                    let _ = line_writer.write_all(key.as_bytes());
                    let _ = line_writer.write_all("\n".as_bytes());
                    total_lines += 1;
                }
            }
            let _ = line_writer.flush()?;
            Ok(())
        };

        let resp = self
            .list_objects(bucket.clone(), prefix.clone(), batch, None)
            .await
            .context(format!("{}:{}", file!(), line!()))?;
        let mut token = resp.next_token;

        if let Some(objects) = resp.object_list {
            process_objects(objects)?
        }

        while token.is_some() {
            let resp = self
                .list_objects(bucket.clone(), prefix.clone(), batch, token.clone())
                .await
                .context(format!("{}:{}", file!(), line!()))?;
            if let Some(objects) = resp.object_list {
                process_objects(objects)?
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

    pub async fn transfer_object(
        &self,
        bucket: &str,
        key: &str,
        splite_size: usize,
        chunk_size: usize,
        object: GetObjectOutput,
    ) -> Result<()> {
        let content_len = match object.content_length() {
            Some(l) => l,
            None => return Err(anyhow!("content length is None")),
        };
        let content_len_usize: usize = content_len.try_into()?;
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

    pub async fn upload_local_file(
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
        self.multipart_upload_local_file(bucket, key, &mut file, chuck_size)
            .await
    }

    pub async fn upload_local_file_paralle(
        &self,
        local_file: &str,
        bucket: &str,
        key: &str,
        splited_file_size: usize,
        executing_transfers: Arc<RwLock<usize>>,
        multi_part_chunk_size: usize,
        multi_part_chunk_per_batch: usize,
        multi_part_parallelism: usize,
    ) -> Result<()> {
        let file = File::open(local_file)?;
        let file_meta = file.metadata()?;
        let file_max_size_u64 = TryInto::<u64>::try_into(splited_file_size)?;
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

        self.multipart_upload_local_file_paralle_batch(
            local_file,
            bucket,
            key,
            executing_transfers.clone(),
            multi_part_chunk_size,
            multi_part_chunk_per_batch,
            multi_part_parallelism,
        )
        .await
    }

    pub async fn download_object_by_range(
        &self,
        // s_client: Arc<Client>,
        s_bucket: &str,
        s_key: &str,
        file_path: &str,
        executing_transfers: Arc<RwLock<usize>>,
        multi_part_chunk_size: usize,
        multi_part_chunks_per_batch: usize,
        multi_part_parallelism: usize,
    ) -> Result<()> {
        let s_client = Arc::new(self.client.clone());
        let filling_file = file_path.to_string() + DOWNLOAD_TMP_FILE_SUBFFIX;

        let s_obj = s_client
            .get_object()
            .bucket(s_bucket)
            .key(s_key)
            .send()
            .await?;
        let vec_obj_range = gen_object_part_plan(&s_obj, multi_part_chunk_size)?;
        let content_len = match s_obj.content_length() {
            Some(l) => l,
            None => return Err(anyhow!("content length is None")),
        };
        let filling_file_size = TryInto::<usize>::try_into(content_len)?;
        fill_file_with_zero(filling_file_size, multi_part_chunk_size, &filling_file)?;

        let mut obj_range_batch = vec![];
        let vec_obj_range_len = vec_obj_range.len();
        let err_mark = Arc::new(AtomicBool::new(false));
        let mut joinset = JoinSet::new();

        for range in vec_obj_range {
            let part_num = range.part_num;
            obj_range_batch.push(range);
            if obj_range_batch.len().eq(&multi_part_chunks_per_batch)
                || vec_obj_range_len.eq(&TryInto::<usize>::try_into(part_num)?)
            {
                while executing_transfers.read().await.ge(&multi_part_parallelism) {
                    task::yield_now().await;
                }

                let e_t = Arc::clone(&executing_transfers);
                let s_c = Arc::clone(&s_client);
                let s_b = s_bucket.to_string();
                let s_k = s_key.to_string();
                let f_n = filling_file.to_string();
                let v_o_r = obj_range_batch.clone();
                let e_m = Arc::clone(&err_mark);

                joinset.spawn(async move {
                    {
                        let mut num = e_t.write().await;
                        *num += 1;
                    }

                    if let Err(e) = fill_parts_to_file_batch_by_range(
                        s_c,
                        &s_b,
                        &s_k,
                        &f_n,
                        multi_part_chunk_size,
                        v_o_r,
                    )
                    .await
                    {
                        log::error!("{:?}", e);
                        e_m.store(true, std::sync::atomic::Ordering::SeqCst);
                    };

                    let mut num = e_t.write().await;
                    *num -= 1;
                });
                obj_range_batch.clear();
            }
        }

        while joinset.len() > 0 {
            if err_mark.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(anyhow!("upload error"));
            }
            joinset.join_next().await;
        }

        if err_mark.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("upload error"));
        }
        fs::rename(filling_file, file_path)?;

        Ok(())
    }
}

impl OssClient {
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<
        aws_sdk_s3::operation::get_object::GetObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    > {
        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        std::result::Result::Ok(resp)
    }

    pub async fn remove_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<
        aws_sdk_s3::operation::delete_object::DeleteObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_object::DeleteObjectError>,
    > {
        let resp = self
            .client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        std::result::Result::Ok(resp)
    }

    pub async fn remove_objects(
        &self,
        bucket: &str,
        keys: Vec<ObjectIdentifier>,
    ) -> std::result::Result<
        aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_objects::DeleteObjectsError>,
    > {
        let del = Delete::builder().set_objects(Some(keys)).build()?;
        self.client
            .delete_objects()
            .bucket(bucket)
            .delete(del)
            .send()
            .await
    }

    #[allow(dead_code)]
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
        bucket: impl Into<std::string::String>,
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

        let list = obj_list
            .send()
            .await
            .context(format!("{}:{}", file!(), line!()))?;

        let mut obj_list = None;

        let mut vec = vec![];
        for o in list.contents() {
            vec.push(o.clone());
        }

        if vec.len() > 0 {
            obj_list = Some(vec);
        }

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
        let mut completed_parts: Vec<CompletedPart> = Vec::new();

        let multipart_upload_res = self.create_multipart_upload(bucket, key, expires).await?;

        let upload_id = match multipart_upload_res.upload_id() {
            Some(id) => id,
            None => {
                return Err(anyhow!("upload id is None"));
            }
        };

        let mut part_number = 0;
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
            let stream = ByteStream::from(buffer);
            part_number += 1;

            let completed_part = self
                .upload_part(upload_id, part_number, bucket, key, stream)
                .await?;
            completed_parts.push(completed_part);

            if content_len == 0 || buf_len < chunk_size {
                break;
            }
        }

        let _ = self
            .complete_multipart_upload(bucket, key, upload_id, completed_parts)
            .await?;
        Ok(())
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
        let mut completed_parts: Vec<CompletedPart> = Vec::new();
        let multipart_upload_res: CreateMultipartUploadOutput =
            self.create_multipart_upload(bucket, key, None).await?;
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

            let completed_part = self
                .upload_part(upload_id, part_number, bucket, key, stream)
                .await?;
            completed_parts.push(completed_part);

            if read_count != chuck_size {
                break;
            }
        }
        // 完成上传文件合并
        self.complete_multipart_upload(bucket, key, upload_id, completed_parts)
            .await?;
        Ok(())
    }

    pub async fn multipart_upload_local_file_paralle_batch(
        &self,
        file_path: &str,
        bucket: &str,
        key: &str,
        executing_transfers: Arc<RwLock<usize>>,
        multi_part_chunk_size: usize,
        multi_part_chunk_per_batch: usize,
        multi_part_parallelism: usize,
    ) -> Result<()> {
        let multipart_upload_res: CreateMultipartUploadOutput =
            self.create_multipart_upload(bucket, key, None).await?;
        let upload_id = match multipart_upload_res.upload_id() {
            Some(id) => id,
            None => {
                return Err(anyhow!("upload id is None"));
            }
        };

        let completed_parts = self
            .upload_file_parts(
                file_path,
                bucket,
                key,
                upload_id,
                Arc::clone(&executing_transfers),
                multi_part_chunk_size,
                multi_part_chunk_per_batch,
                multi_part_parallelism,
            )
            .await?;

        // 完成上传文件合并
        self.complete_multipart_upload(bucket, key, upload_id, completed_parts)
            .await?;
        Ok(())
    }

    // pub async fn multipart_upload_obj_paralle_by_range(
    //     &self,
    //     // s_client: Arc<Client>,
    //     s_client: Arc<OssClient>,
    //     s_bucket: &str,
    //     s_key: &str,
    //     t_bucket: &str,
    //     t_key: &str,
    //     expires: Option<aws_smithy_types::DateTime>,
    //     executing_transfers: Arc<RwLock<usize>>,
    //     multi_part_chunk_size: usize,
    //     multi_part_chunks_per_batch: usize,
    //     multi_part_parallelism: usize,
    // ) -> Result<()> {
    //     let multipart_upload_res: CreateMultipartUploadOutput = self
    //         .create_multipart_upload(t_bucket, t_key, expires)
    //         .await?;

    //     let upload_id = match multipart_upload_res.upload_id() {
    //         Some(id) => id,
    //         None => {
    //             return Err(anyhow!("upload id is None"));
    //         }
    //     };

    //     while executing_transfers.read().await.ge(&multi_part_parallelism) {
    //         task::yield_now().await;
    //     }

    //     let completed_parts = self
    //         .transfer_object_parts_by_range(
    //             s_client,
    //             s_bucket,
    //             s_key,
    //             t_bucket,
    //             t_key,
    //             upload_id,
    //             Arc::clone(&executing_transfers),
    //             multi_part_chunk_size,
    //             multi_part_chunks_per_batch,
    //             multi_part_parallelism,
    //         )
    //         .await?;

    //     // 完成上传文件合并
    //     self.complete_multipart_upload(t_bucket, t_key, upload_id, completed_parts)
    //         .await?;
    //     Ok(())
    // }

    #[inline]
    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        expires: Option<aws_smithy_types::DateTime>,
    ) -> Result<CreateMultipartUploadOutput> {
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
        Ok(multipart_upload_res)
    }

    #[inline]
    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        bucket: &str,
        key: &str,
        stream: ByteStream,
    ) -> Result<CompletedPart> {
        let presigning = PresigningConfig::expires_in(std::time::Duration::from_secs(3000))?;
        let upload_part_res = self
            .client
            .upload_part()
            .upload_id(upload_id)
            .part_number(part_number)
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send_with_plugins(presigning)
            // .send()
            .await?;

        let completed_part = CompletedPart::builder()
            .e_tag(upload_part_res.e_tag.unwrap_or_default())
            .part_number(part_number)
            .build();
        Ok(completed_part)
    }

    #[inline]
    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        completed_parts: Vec<CompletedPart>,
    ) -> Result<CompleteMultipartUploadOutput> {
        let completed_multipart_upload: CompletedMultipartUpload =
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build();

        let complete_multi_part_upload_output = self
            .client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await?;

        Ok(complete_multi_part_upload_output)
    }

    // pub async fn transfer_object_parts_by_range(
    //     &self,
    //     // s_client: Arc<Client>,
    //     s_client: Arc<OssClient>,
    //     s_bucket: &str,
    //     s_key: &str,
    //     t_bucket: &str,
    //     t_key: &str,
    //     upload_id: &str,
    //     executing_transfers: Arc<RwLock<usize>>,
    //     multi_part_chunk_size: usize,
    //     multi_part_chunks_per_batch: usize,
    //     multi_part_parallelism: usize,
    // ) -> Result<Vec<CompletedPart>> {
    //     let s_obj = s_client
    //         .client
    //         .get_object()
    //         .bucket(s_bucket)
    //         .key(s_key)
    //         .send()
    //         .await?;
    //     let vec_obj_range = gen_object_part_plan(&s_obj, multi_part_chunk_size)?;

    //     let client = self.clone();
    //     let arc_t_client = Arc::new(client);
    //     let err_mark = Arc::new(AtomicBool::new(false));
    //     let mut joinset = JoinSet::new();

    //     let completed_parts_btree: Arc<Mutex<BTreeMap<i32, CompletedPart>>> =
    //         Arc::new(Mutex::new(BTreeMap::new()));

    //     let mut vec_obj_range_tmp = vec![];
    //     let vec_obj_range_len = vec_obj_range.len();

    //     for range in vec_obj_range {
    //         let part_num = range.part_num;
    //         vec_obj_range_tmp.push(range);
    //         if vec_obj_range_tmp.len().eq(&multi_part_chunks_per_batch)
    //             || vec_obj_range_len.eq(&TryInto::<usize>::try_into(part_num)?)
    //         {
    //             while executing_transfers.read().await.ge(&multi_part_parallelism) {
    //                 task::yield_now().await;
    //             }

    //             let e_t = Arc::clone(&executing_transfers);
    //             let s_c = Arc::clone(&s_client);
    //             let t_c = Arc::clone(&arc_t_client);
    //             let s_b = s_bucket.to_string();
    //             let t_b = t_bucket.to_string();
    //             let s_k = s_key.to_string();
    //             let t_k = t_key.to_string();
    //             let up_id = upload_id.to_string();
    //             let v_o_r = vec_obj_range_tmp.clone();
    //             let c_b_t = Arc::clone(&completed_parts_btree);
    //             let e_m = Arc::clone(&err_mark);

    //             joinset.spawn(async move {
    //                 {
    //                     let mut num = e_t.write().await;
    //                     *num += 1;
    //                 }
    //                 if let Err(e) = transfer_parts_batch_by_range(
    //                     s_c, t_c, &s_b, &t_b, &s_k, &t_k, &up_id, v_o_r, c_b_t,
    //                 )
    //                 .await
    //                 {
    //                     log::error!("{:?}", e);
    //                     e_m.store(true, std::sync::atomic::Ordering::SeqCst);
    //                 };

    //                 let mut num = e_t.write().await;
    //                 *num -= 1;
    //             });
    //             vec_obj_range_tmp.clear();
    //         }
    //     }

    //     while joinset.len() > 0 {
    //         if err_mark.load(std::sync::atomic::Ordering::SeqCst) {
    //             return Err(anyhow!("upload error"));
    //         }
    //         joinset.join_next().await;
    //     }

    //     if err_mark.load(std::sync::atomic::Ordering::SeqCst) {
    //         return Err(anyhow!("upload error"));
    //     }

    //     let completed_parts = completed_parts_btree
    //         .lock()
    //         .await
    //         .clone()
    //         .into_values()
    //         .collect::<Vec<CompletedPart>>();

    //     Ok(completed_parts)
    // }

    //Todo 增加client:Arc<Client> 参数，修改self.client.clone();在上一层生成Arc<Client>
    // Arc::clone 更改为 变量.clone()
    pub async fn upload_file_parts(
        &self,
        file_name: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        executing_transfers: Arc<RwLock<usize>>,
        multi_part_chunk_size: usize,
        multi_part_chunk_per_batch: usize,
        multi_part_parallelism: usize,
    ) -> Result<Vec<CompletedPart>> {
        let file_parts = gen_file_part_plan(file_name, multi_part_chunk_size)?;
        let client = self.client.clone();
        let arc_client = Arc::new(client);
        let err_mark = Arc::new(AtomicBool::new(false));
        let mut joinset = JoinSet::new();

        let completed_parts_btree: Arc<Mutex<BTreeMap<i32, CompletedPart>>> =
            Arc::new(Mutex::new(BTreeMap::new()));

        let mut batch = file_parts.len() / multi_part_chunk_per_batch;
        if (file_parts.len() % multi_part_chunk_per_batch) > 0 {
            batch += 1;
        }

        let mut parts_vec = vec![];
        let file_parts_len = file_parts.len();

        for f_p in file_parts {
            let part_num_usize: usize = TryInto::try_into(f_p.part_num)?;
            parts_vec.push(f_p);

            if (parts_vec.len() % batch).eq(&0) || part_num_usize.eq(&file_parts_len) {
                // let e_t = Arc::clone(&executing_transfers);
                let e_t = executing_transfers.clone();
                let c = Arc::clone(&arc_client);
                let err_mark = Arc::clone(&err_mark);
                let f = file_name.to_string();
                let b = bucket.to_string();
                let k = key.to_string();
                let u_id = upload_id.to_string();
                let p_v = parts_vec.clone();
                let c_b_tree = Arc::clone(&completed_parts_btree);

                while e_t.read().await.ge(&multi_part_parallelism) {
                    task::yield_now().await;
                }

                joinset.spawn(async move {
                    {
                        let mut num = e_t.write().await;
                        *num += 1;
                    }

                    if let Err(e) = upload_file_parts_batch(
                        c,
                        &f,
                        &b,
                        &k,
                        &u_id,
                        p_v,
                        multi_part_chunk_size,
                        c_b_tree,
                    )
                    .await
                    {
                        log::error!("{:?}", e);
                        err_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    };

                    let mut num = e_t.write().await;
                    *num -= 1;
                });

                parts_vec.clear();
            }
        }

        while joinset.len() > 0 {
            if err_mark.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(anyhow!("upload error"));
            }
            joinset.join_next().await;
        }

        if err_mark.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("upload error"));
        }

        let completed_parts = completed_parts_btree
            .lock()
            .await
            .clone()
            .into_values()
            .collect::<Vec<CompletedPart>>();

        Ok(completed_parts)
    }

    #[allow(dead_code)]
    #[inline]
    pub async fn abort_multi_part_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<AbortMultipartUploadOutput> {
        let abort_upload = self
            .client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await?;

        Ok(abort_upload)
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
                        if !f.filter(usize::try_from(d.secs())?) {
                            continue;
                        }
                    }
                }

                let obj_size = match obj.size() {
                    Some(s) => s,
                    None => return Err(anyhow!("object length is None")),
                };

                let obj_size_i128 = i128::from(obj_size);
                let map_key = size_distributed(obj_size_i128);
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
                            // if !f.filter(i128::from(d.secs())) {
                            //     continue;
                            // }
                            if !f.filter(usize::try_from(d.secs())?) {
                                continue;
                            }
                        }
                    }

                    let obj_size = match obj.size() {
                        Some(s) => s,
                        None => return Err(anyhow!("object length is None")),
                    };
                    let obj_size_i128 = i128::from(obj_size);
                    let map_key = size_distributed(obj_size_i128);
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

pub async fn multipart_transfer_obj_paralle_by_range(
    s_client: Arc<OssClient>,
    s_bucket: &str,
    s_key: &str,
    t_client: Arc<OssClient>,
    t_bucket: &str,
    t_key: &str,
    expires: Option<aws_smithy_types::DateTime>,
    executing_transfers: Arc<RwLock<usize>>,
    multi_part_chunk_size: usize,
    multi_part_chunks_per_batch: usize,
    multi_part_parallelism: usize,
) -> Result<()> {
    let multipart_upload_res: CreateMultipartUploadOutput = t_client
        .create_multipart_upload(t_bucket, t_key, expires)
        .await?;

    let upload_id = match multipart_upload_res.upload_id() {
        Some(id) => id,
        None => {
            return Err(anyhow!("upload id is None"));
        }
    };

    while executing_transfers.read().await.ge(&multi_part_parallelism) {
        task::yield_now().await;
    }

    let completed_parts = transfer_object_parts_by_range(
        s_client,
        s_bucket,
        s_key,
        t_client.clone(),
        t_bucket,
        t_key,
        upload_id,
        Arc::clone(&executing_transfers),
        multi_part_chunk_size,
        multi_part_chunks_per_batch,
        multi_part_parallelism,
    )
    .await?;

    // 完成上传文件合并
    t_client
        .complete_multipart_upload(t_bucket, t_key, upload_id, completed_parts)
        .await?;
    Ok(())
}

pub async fn transfer_object_parts_by_range(
    s_client: Arc<OssClient>,
    s_bucket: &str,
    s_key: &str,
    t_client: Arc<OssClient>,
    t_bucket: &str,
    t_key: &str,
    upload_id: &str,
    executing_transfers: Arc<RwLock<usize>>,
    multi_part_chunk_size: usize,
    multi_part_chunks_per_batch: usize,
    multi_part_parallelism: usize,
) -> Result<Vec<CompletedPart>> {
    let s_obj = s_client
        .client
        .get_object()
        .bucket(s_bucket)
        .key(s_key)
        .send()
        .await?;
    let vec_obj_range = gen_object_part_plan(&s_obj, multi_part_chunk_size)?;
    let err_mark = Arc::new(AtomicBool::new(false));
    let mut joinset = JoinSet::new();

    let completed_parts_btree: Arc<Mutex<BTreeMap<i32, CompletedPart>>> =
        Arc::new(Mutex::new(BTreeMap::new()));

    let mut vec_obj_range_tmp = vec![];
    let vec_obj_range_len = vec_obj_range.len();

    for range in vec_obj_range {
        let part_num = range.part_num;
        vec_obj_range_tmp.push(range);
        if vec_obj_range_tmp.len().eq(&multi_part_chunks_per_batch)
            || vec_obj_range_len.eq(&TryInto::<usize>::try_into(part_num)?)
        {
            while executing_transfers.read().await.ge(&multi_part_parallelism) {
                task::yield_now().await;
            }

            let e_t = Arc::clone(&executing_transfers);
            let s_c = Arc::clone(&s_client);
            let t_c = t_client.clone();
            let s_b = s_bucket.to_string();
            let t_b = t_bucket.to_string();
            let s_k = s_key.to_string();
            let t_k = t_key.to_string();
            let up_id = upload_id.to_string();
            let v_o_r = vec_obj_range_tmp.clone();
            let c_b_t = Arc::clone(&completed_parts_btree);
            let e_m = Arc::clone(&err_mark);

            joinset.spawn(async move {
                {
                    let mut num = e_t.write().await;
                    *num += 1;
                }
                if let Err(e) = transfer_parts_batch_by_range(
                    s_c, t_c, &s_b, &t_b, &s_k, &t_k, &up_id, v_o_r, c_b_t,
                )
                .await
                {
                    log::error!("{:?}", e);
                    e_m.store(true, std::sync::atomic::Ordering::SeqCst);
                };

                let mut num = e_t.write().await;
                *num -= 1;
            });
            vec_obj_range_tmp.clear();
        }
    }

    while joinset.len() > 0 {
        if err_mark.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("upload error"));
        }
        joinset.join_next().await;
    }

    if err_mark.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(anyhow!("upload error"));
    }

    let completed_parts = completed_parts_btree
        .lock()
        .await
        .clone()
        .into_values()
        .collect::<Vec<CompletedPart>>();

    Ok(completed_parts)
}

pub async fn transfer_parts_batch_by_range(
    s_client: Arc<OssClient>,
    t_client: Arc<OssClient>,
    s_bucket: &str,
    t_bucket: &str,
    s_key: &str,
    t_key: &str,
    upload_id: &str,
    parts_vec: Vec<ObjectRange>,
    completed_parts_btree: Arc<Mutex<BTreeMap<i32, CompletedPart>>>,
) -> Result<()> {
    for p in parts_vec {
        let range_str = gen_range_string(p.begin, p.end);
        let s_obj = s_client
            .client
            .get_object()
            .bucket(s_bucket)
            .key(s_key)
            .range(range_str)
            .send()
            .await?;
        let presigning = PresigningConfig::expires_in(std::time::Duration::from_secs(3000))?;

        let upload_part_res = t_client
            .client
            .upload_part()
            .bucket(t_bucket)
            .key(t_key)
            .upload_id(upload_id)
            .body(s_obj.body)
            .part_number(p.part_num)
            .send_with_plugins(presigning)
            // .send()
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

pub async fn upload_file_parts_batch(
    target_client: Arc<Client>,
    file_name: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts_vec: Vec<FilePart>,
    chunk_size: usize,
    completed_parts_btree: Arc<Mutex<BTreeMap<i32, CompletedPart>>>,
) -> Result<()> {
    for p in parts_vec {
        let mut f = File::open(file_name)?;
        f.seek(SeekFrom::Start(p.offset))?;
        let mut buf = vec![0; chunk_size];
        let read_count = f.read(&mut buf)?;
        let body = &buf[..read_count];

        let stream = ByteStream::new(SdkBody::from(body));
        let presigning = PresigningConfig::expires_in(std::time::Duration::from_secs(3000))?;

        let upload_part_res = target_client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .body(stream)
            .part_number(p.part_num)
            .send_with_plugins(presigning)
            // .send()
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

pub async fn download_object(
    get_object: GetObjectOutput,
    file_path: &str,
    splite_size: usize,
    chunk_size: usize,
) -> Result<()> {
    let mut t_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(file_path)?;
    let content_len = match get_object.content_length() {
        Some(l) => l,
        None => return Err(anyhow!("content length is None")),
    };
    let mut content_len_usize: usize = content_len.try_into()?;
    if content_len_usize.le(&splite_size) {
        let content = get_object.body.collect().await?;
        let bytes = content.into_bytes();
        t_file.write_all(&bytes)?;
        t_file.flush()?;
        return Ok(());
    }

    let mut byte_stream_async_reader = get_object.body.into_async_read();
    loop {
        if content_len_usize > chunk_size {
            let mut buffer = vec![0; chunk_size];
            let _ = byte_stream_async_reader.read_exact(&mut buffer).await?;
            t_file.write_all(&buffer)?;
            content_len_usize -= chunk_size;
            continue;
        } else {
            let mut buffer = vec![0; content_len_usize];
            let _ = byte_stream_async_reader.read_exact(&mut buffer).await?;
            t_file.write_all(&buffer)?;
            break;
        }
    }

    t_file.flush()?;
    Ok(())
}

pub async fn fill_parts_to_file_batch_by_range(
    s_client: Arc<Client>,
    s_bucket: &str,
    s_key: &str,
    file_path: &str,
    chunk_size: usize,
    parts_vec: Vec<ObjectRange>,
) -> Result<()> {
    for p in parts_vec {
        let range_str = gen_range_string(p.begin, p.end);
        let s_obj = s_client
            .get_object()
            .bucket(s_bucket)
            .key(s_key)
            .range(range_str)
            .send()
            .await?;
        let content_len = match s_obj.content_length() {
            Some(l) => l,
            None => return Err(anyhow!("content length is None")),
        };

        let mut t_file = OpenOptions::new().write(true).open(&file_path)?;

        let seek_offset = TryInto::<u64>::try_into(p.begin)?;
        t_file.seek(SeekFrom::Start(seek_offset))?;

        let mut content_len_usize: usize = content_len.try_into()?;

        let mut byte_stream_async_reader = s_obj.body.into_async_read();
        loop {
            if content_len_usize > chunk_size {
                let mut buffer = vec![0; chunk_size];
                let _ = byte_stream_async_reader.read_exact(&mut buffer).await?;
                t_file.write_all(&buffer)?;
                content_len_usize -= chunk_size;
                continue;
            } else {
                let mut buffer = vec![0; content_len_usize];
                let _ = byte_stream_async_reader.read_exact(&mut buffer).await?;
                t_file.write_all(&buffer)?;
                break;
            }
        }

        t_file.flush()?;
    }

    Ok(())
}

// 生成 range 字符串，用于oss range下载
pub fn gen_range_string(begin: usize, end: usize) -> String {
    format!("bytes={}-{}", begin, end)
}

pub fn gen_object_part_plan(
    get_object_output: &GetObjectOutput,
    chunk_size: usize,
) -> Result<Vec<ObjectRange>> {
    let mut vec_obj_parts = vec![];
    let obj_len = match get_object_output.content_length() {
        Some(len) => TryInto::<usize>::try_into(len)?,
        None => return Err(anyhow!("content length is None")),
    };

    let mut begin = 0;
    let quotient = obj_len / chunk_size;
    let remainder = obj_len % chunk_size;
    let part_quantities = match remainder.eq(&0) {
        true => quotient,
        false => quotient + 1,
    };
    for part_idx in 1..=part_quantities {
        let part_num = TryInto::<i32>::try_into(part_idx)?;
        let end = match part_idx.eq(&part_quantities) && !remainder.eq(&0) {
            true => begin + remainder,
            false => begin + chunk_size,
        };
        let obj_range = ObjectRange {
            part_num,
            begin,
            end,
        };
        vec_obj_parts.push(obj_range);
        begin = end + 1;
    }

    Ok(vec_obj_parts)
}
