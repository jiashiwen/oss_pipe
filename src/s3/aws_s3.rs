use crate::{
    checkpoint::{FileDescription, FilePosition, Opt, RecordDescription},
    commons::{
        gen_multi_part_plan, merge_file, size_distributed, FilePart, LastModifyFilter, RegexFilter,
    },
    tasks::{gen_file_path, MODIFIED_PREFIX, REMOVED_PREFIX, TRANSFER_OBJECT_LIST_FILE_PREFIX},
};
use anyhow::{anyhow, Result};
use aws_sdk_s3::operation::{
    abort_multipart_upload::AbortMultipartUploadOutput,
    complete_multipart_upload::CompleteMultipartUploadOutput, get_object::GetObjectOutput,
    list_multipart_uploads::ListMultipartUploadsOutput,
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
    io::{BufRead, BufReader, LineWriter, Lines, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, RwLock},
    task::{self, JoinSet},
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
        // aws_sdk_s3::output::DeleteObjectsOutput,
        // aws_sdk_s3::types::SdkError<aws_sdk_s3::error::DeleteObjectsError>,
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

        let list = obj_list.send().await?;

        let mut obj_list = None;

        // if let Some(l) = list.contents() {
        //     let mut vec = vec![];
        //     for item in l.iter() {
        //         vec.push(item.clone());
        //     }
        //     if vec.len() > 0 {
        //         obj_list = Some(vec);
        //     }
        // };
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

    // Todo
    // 通过upload_parts_quantities: Arc::<AtomicUsize> 控制并发数量
    pub async fn upload_local_file_paralle(
        &self,
        local_file: &str,
        bucket: &str,
        key: &str,
        splited_file_size: usize,
        // executing_transfers: Arc<AtomicUsize>,
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
        // self.multipart_upload_local_file_paralle(
        //     joinset,
        //     executing_uploads,
        //     max_parallelism,
        //     bucket,
        //     key,
        //     local_file,
        //     chunk_size,
        // )
        // .await

        self.multipart_upload_local_file_paralle_batch(
            local_file,
            bucket,
            key,
            Arc::clone(&executing_transfers),
            multi_part_chunk_size,
            multi_part_chunk_per_batch,
            multi_part_parallelism,
        )
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

    //ToDo
    // 假如原子计数器，函数开始+1，结束或报错-1
    pub async fn multipart_upload_local_file_paralle(
        &self,
        joinset: Arc<Mutex<&mut JoinSet<()>>>,
        executing_uploads: Arc<AtomicUsize>,
        max_parallelism: usize,
        bucket: &str,
        key: &str,
        file_path: &str,
        chuck_size: usize,
    ) -> Result<()> {
        let b_tree: BTreeMap<i32, CompletedPart> = BTreeMap::new();
        let b_tree_mutex = Arc::new(Mutex::new(b_tree));
        let err_mark = Arc::new(AtomicBool::new(false));

        let file_parts = gen_multi_part_plan(file_path, chuck_size)?;
        let left_parts = Arc::new(AtomicUsize::new(file_parts.len()));
        let multipart_upload_res: CreateMultipartUploadOutput =
            self.create_multipart_upload(bucket, key, None).await?;
        let upload_id = match multipart_upload_res.upload_id() {
            Some(id) => id,
            None => {
                return Err(anyhow!("upload id is None"));
            }
        };
        let client = Arc::new(self.client.clone());
        let mut v_parts = vec![];
        //分段上传文件并记录completer_part
        for part in file_parts {
            v_parts.push(part);

            if v_parts.len().eq(&max_parallelism) {
                let c = Arc::clone(&client);
                let up_id = upload_id.to_string();
                let b_t_arc = Arc::clone(&b_tree_mutex);
                let k = key.to_string();
                let b = bucket.to_string();
                let v = v_parts.clone();
                let f = file_path.to_string();
                let e_m: Arc<AtomicBool> = Arc::clone(&err_mark);
                let l_p = Arc::clone(&left_parts);
                let e_u = Arc::clone(&executing_uploads);

                while joinset.lock().await.len() >= max_parallelism {
                    joinset.lock().await.join_next().await;
                }
                while e_u.load(std::sync::atomic::Ordering::SeqCst) >= max_parallelism {
                    task::yield_now().await;
                    // let _ = tokio::time::sleep(Duration::from_millis(100));
                }
                joinset.lock().await.spawn(async move {
                    if e_m.load(std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }
                    e_u.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    match upload_parts(c, &up_id, &b, &k, &f, v, chuck_size, b_t_arc, l_p).await {
                        Ok(_) => {}
                        Err(e) => {
                            log::error!("{}", e);
                            e_m.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                    e_u.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                });

                v_parts.clear();
            }
        }

        if v_parts.len() > 0 {
            let c = Arc::clone(&client);
            let up_id = upload_id.to_string();
            let b_t_arc = Arc::clone(&b_tree_mutex);
            let k = key.to_string();
            let b = bucket.to_string();
            let v = v_parts.clone();
            let f = file_path.to_string();
            let e_m: Arc<AtomicBool> = Arc::clone(&err_mark);
            let l_p = Arc::clone(&left_parts);
            let e_u = Arc::clone(&executing_uploads);

            while joinset.lock().await.len() >= max_parallelism {
                joinset.lock().await.join_next().await;
            }

            joinset.lock().await.spawn(async move {
                //ToDo
                // 假如原子计数器，函数开始+1，结束或报错-1
                if e_m.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }

                while e_u.load(std::sync::atomic::Ordering::SeqCst) >= max_parallelism {
                    let _ = tokio::time::sleep(Duration::from_millis(100));
                }

                e_u.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                match upload_parts(c, &up_id, &b, &k, &f, v, chuck_size, b_t_arc, l_p).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("{}", e);
                        e_m.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                e_u.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            });
        }

        while !err_mark.load(std::sync::atomic::Ordering::SeqCst)
            && !left_parts.load(std::sync::atomic::Ordering::SeqCst).eq(&0)
        {
            joinset.lock().await.join_next().await;
        }

        if err_mark.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("multi part upload file fail"));
        }

        if !left_parts.load(std::sync::atomic::Ordering::SeqCst).eq(&0) {
            joinset.lock().await.join_next().await;
        }

        let completed_parts = b_tree_mutex.lock().await.clone().into_values().collect();
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
            .upload_parts(
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

    pub async fn upload_parts(
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
        let file_parts = gen_multi_part_plan(file_name, multi_part_chunk_size)?;
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
                let e_t = Arc::clone(&executing_transfers);
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
                        // println!("{}/{}", e_t.read().await, multi_part_parallelism);
                        log::info!("{}/{}", e_t.read().await, multi_part_parallelism);
                        let mut num = e_t.write().await;
                        *num += 1;
                    }

                    if let Err(e) = upload_parts_batch(
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

                    // for p in p_v {
                    //     let p_n = p.part_num;

                    // let stream = match file_part_to_byte_stream(&f, p, multi_part_chunk_size) {
                    //     Ok(b) => b,
                    //     Err(e) => {
                    //         log::error!("{:?}", e);
                    //         err_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    //         let mut num = e_t.write().await;
                    //         *num -= 1;
                    //         return;
                    //     }
                    // };

                    //     let presigning = match PresigningConfig::expires_in(
                    //         std::time::Duration::from_secs(3000),
                    //     ) {
                    //         Ok(p) => p,
                    //         Err(e) => {
                    //             log::error!("{:?}", e);
                    //             err_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    //             let mut num = e_t.write().await;
                    //             *num -= 1;
                    //             return;
                    //         }
                    //     };

                    //     let upload_part_res = match c
                    //         .upload_part()
                    //         .bucket(&b)
                    //         .key(&k)
                    //         .upload_id(&u_id)
                    //         .body(stream)
                    //         .part_number(p_n)
                    //         // .send()
                    //         .send_with_plugins(presigning)
                    //         .await
                    //     {
                    //         Ok(res) => res,
                    //         Err(e) => {
                    //             log::error!("{:?}", e);
                    //             err_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    //             let mut num = e_t.write().await;
                    //             *num -= 1;
                    //             return;
                    //         }
                    //     };

                    //     let completed_part = CompletedPart::builder()
                    //         .e_tag(upload_part_res.e_tag.unwrap_or_default())
                    //         .part_number(p_n)
                    //         .build();
                    //     let mut b_t = c_b_tree.lock().await;
                    //     b_t.insert(p_n, completed_part);
                    // }
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

    #[inline]
    pub async fn list_multi_parts_upload(
        &self,
        bucket: &str,
    ) -> Result<ListMultipartUploadsOutput> {
        let list_upload = self
            .client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await?;

        Ok(list_upload)
    }

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
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

        let new_object_list = gen_file_path(
            out_put_dir,
            TRANSFER_OBJECT_LIST_FILE_PREFIX,
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

        let mut process_objects = |objects: Vec<Object>| {
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
        };
        if let Some(objects) = resp.object_list {
            process_objects(objects);
        }

        while token.is_some() {
            let resp = self
                .list_objects(bucket.to_string(), source_prefix.clone(), batch_size, token)
                .await?;
            if let Some(objects) = resp.object_list {
                process_objects(objects);
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
                            if !f.filter(i128::from(d.secs())) {
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

pub async fn upload_parts(
    client: Arc<Client>,
    file_name: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts_vec: Vec<FilePart>,
    chunk_size: usize,
    completed_parts_btree: Arc<Mutex<BTreeMap<i32, CompletedPart>>>,
    left_parts: Arc<AtomicUsize>,
) -> Result<()> {
    for p in parts_vec {
        let mut f = File::open(file_name)?;
        f.seek(SeekFrom::Start(p.offset))?;
        let mut buf = vec![0; chunk_size];
        let read_count = f.read(&mut buf)?;
        let body = &buf[..read_count];

        let stream = ByteStream::new(SdkBody::from(body));
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
        left_parts.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
    Ok(())
}

pub async fn upload_parts_batch(
    client: Arc<Client>,
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

pub async fn download_object(
    get_object: GetObjectOutput,
    file: &mut File,
    splite_size: usize,
    chunk_size: usize,
) -> Result<()> {
    let content_len = match get_object.content_length() {
        Some(l) => l,
        None => return Err(anyhow!("content length is None")),
    };
    let mut content_len_usize: usize = content_len.try_into()?;
    // let mut content_len_usize: usize = get_object.content_length().try_into()?;
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

fn file_part_to_byte_stream(file_name: &str, p: FilePart, chunk_size: usize) -> Result<ByteStream> {
    let mut f = File::open(file_name)?;
    f.seek(SeekFrom::Start(p.offset))?;
    let mut buf = vec![0; chunk_size];
    let read_count = f.read(&mut buf)?;
    let body = &buf[..read_count];
    Ok(ByteStream::new(SdkBody::from(body)))
}
