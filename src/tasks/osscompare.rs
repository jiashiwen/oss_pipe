use crate::{
    checkpoint::{FilePosition, ListedRecord, Opt, RecordDescription},
    s3::{aws_s3::OssClient, OSSDescription},
};
use anyhow::Result;
use aws_sdk_s3::{error::GetObjectErrorKind, output::GetObjectOutput};
use dashmap::DashMap;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::io::AsyncReadExt;

use super::ObjectDiff;
use super::{
    gen_file_path, DateTime, DiffContent, DiffExpires, DiffLength, COMPARE_OBJECT_DIFF_PREFIX,
    ERROR_RECORD_PREFIX,
};
use super::{DiffNotExists, OFFSET_PREFIX};

#[derive(Debug, Clone)]
pub struct OssCompare {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub err_conter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub meta_dir: String,
    pub exprirs_diff_scope: i64,
    pub list_file_path: String,
}

impl OssCompare {
    pub async fn compare(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);
        let diff_file_name = gen_file_path(&self.meta_dir, COMPARE_OBJECT_DIFF_PREFIX, &subffix);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;
        let mut diff_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;
        let c_t = self.target.gen_oss_client()?;
        for record in records {
            let r = self.compare_listed_record(&record, &c_s, &c_t).await;

            match r {
                Ok(diff) => match diff {
                    Some(d) => {
                        let _ = d.save_json_to_file(&mut diff_file);
                    }
                    None => {}
                },
                Err(e) => {
                    let recorddesc = RecordDescription {
                        source_key: record.key.clone(),
                        target_key: record.key.clone(),
                        list_file_path: self.list_file_path.clone(),
                        list_file_position: FilePosition {
                            offset: record.offset,
                            line_num: record.line_num,
                        },
                        option: Opt::PUT,
                    };
                    recorddesc.handle_error(
                        // anyhow!("{}", e),
                        &self.err_conter,
                        &self.offset_map,
                        &mut error_file,
                        offset_key.as_str(),
                    );
                    log::error!("{}", e);
                }
            }

            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );
        }
        self.offset_map.remove(&offset_key);
        let _ = error_file.flush();
        match error_file.metadata() {
            Ok(meta) => {
                if meta.len() == 0 {
                    let _ = fs::remove_file(error_file_name.as_str());
                }
            }
            Err(_) => {}
        };
        match diff_file.metadata() {
            Ok(meta) => {
                if meta.len() == 0 {
                    let _ = fs::remove_file(diff_file_name.as_str());
                }
            }
            Err(_) => {}
        };

        Ok(())
    }

    async fn compare_listed_record(
        &self,
        record: &ListedRecord,
        source: &OssClient,
        target: &OssClient,
    ) -> Result<Option<ObjectDiff>> {
        let mut s_exists = false;
        let mut t_exists = false;
        let mut obj_s = GetObjectOutput::builder().build();
        let mut obj_t = GetObjectOutput::builder().build();
        match source
            .get_object(&self.source.bucket.as_str(), record.key.as_str())
            .await
        {
            core::result::Result::Ok(o) => {
                s_exists = true;
                obj_s = o;
            }
            Err(e) => {
                let service_err = e.into_service_error();
                match service_err.kind {
                    GetObjectErrorKind::NoSuchKey(_) => {}
                    _ => return Err(service_err.into()),
                }
            }
        };

        match target
            .get_object(&self.target.bucket.as_str(), record.key.as_str())
            .await
        {
            core::result::Result::Ok(o) => {
                t_exists = true;
                obj_t = o;
            }
            Err(e) => {
                // 源端文件不存在按传输成功处理
                let service_err = e.into_service_error();
                match service_err.kind {
                    GetObjectErrorKind::NoSuchKey(_) => {}
                    _ => return Err(service_err.into()),
                }
            }
        };

        if !s_exists.eq(&t_exists) {
            let diff = ObjectDiff::NotExists(DiffNotExists {
                key: record.key.to_string(),
                source_exists: s_exists,
                target_exists: t_exists,
            });
            return Ok(Some(diff));
        }

        if !s_exists && !t_exists {
            return Ok(None);
        }

        if let Some(diff) = self.compare_content_len(record, &obj_s, &obj_t) {
            return Ok(Some(diff));
        }

        if let Some(diff) = self.compare_expires(record, &obj_s, &obj_t) {
            return Ok(Some(diff));
        }

        Ok(None)
    }

    fn compare_content_len(
        &self,
        record: &ListedRecord,
        s_obj: &GetObjectOutput,
        t_obj: &GetObjectOutput,
    ) -> Option<ObjectDiff> {
        let len_s = s_obj.content_length();
        let len_t = t_obj.content_length();
        if !len_s.eq(&len_t) {
            let diff = ObjectDiff::ContentLenthDiff(DiffLength {
                key: record.key.to_string(),
                source_content_len: len_s,
                target_content_len: len_t,
            });
            return Some(diff);
        }
        None
    }

    fn compare_expires(
        &self,
        record: &ListedRecord,
        s_obj: &GetObjectOutput,
        t_obj: &GetObjectOutput,
    ) -> Option<ObjectDiff> {
        let expr_s = match s_obj.expires() {
            Some(datetime) => Some(*datetime),
            None => None,
        };

        let expr_t = match t_obj.expires() {
            Some(datetime) => Some(*datetime),
            None => None,
        };

        if !expr_s.eq(&expr_t) {
            let mut s_second = 0;
            let mut t_second = 0;
            let s_date = match expr_s {
                Some(d) => {
                    s_second = d.secs();
                    Some(DateTime {
                        seconds: d.secs(),
                        subsecond_nanos: d.subsec_nanos(),
                    })
                }
                None => None,
            };
            let t_date = match expr_t {
                Some(d) => {
                    t_second = d.secs();
                    Some(DateTime {
                        seconds: d.secs(),
                        subsecond_nanos: d.subsec_nanos(),
                    })
                }
                None => None,
            };

            if s_date.is_none() || t_date.is_none() {
                let diff = ObjectDiff::ExpiresDiff(DiffExpires {
                    key: record.key.to_string(),
                    source_expires: s_date,
                    target_expires: t_date,
                });
                return Some(diff);
            }

            if i64::abs(s_second - t_second) > self.exprirs_diff_scope {
                let diff = ObjectDiff::ExpiresDiff(DiffExpires {
                    key: record.key.to_string(),
                    source_expires: s_date,
                    target_expires: t_date,
                });
                return Some(diff);
            };
        }
        None
    }

    async fn compare_content(
        &self,
        record: &ListedRecord,
        s_obj: GetObjectOutput,
        t_obj: GetObjectOutput,
    ) -> Result<Option<ObjectDiff>> {
        let buffer_size = 1048577;
        let obj_len = TryInto::<usize>::try_into(s_obj.content_length())?;
        let mut left = obj_len.clone();
        let mut reader_s = s_obj.body.into_async_read();
        let mut reader_t = t_obj.body.into_async_read();

        loop {
            let mut buf_s = vec![0; buffer_size];
            let mut buf_t = vec![0; buffer_size];
            if left > buffer_size {
                let _ = reader_s.read_exact(&mut buf_s).await?;
                let _ = reader_t.read_exact(&mut buf_t).await?;
                left -= buffer_size;
            } else {
                buf_s = vec![0; left];
                buf_t = vec![0; left];
                let _ = reader_s.read_exact(&mut buf_s).await?;
                let _ = reader_t.read_exact(&mut buf_t).await?;
                break;
            }
            if !buf_s.eq(&buf_t) {
                for (idx, byte) in buf_s.iter().enumerate() {
                    if !byte.eq(&buf_t[idx]) {
                        let diff = DiffContent {
                            key: record.key.to_string(),
                            stream_position: obj_len - left + idx,
                            source_byte: *byte,
                            target_byte: buf_t[idx],
                        };
                        return Ok(Some(ObjectDiff::ContentDiff(diff)));
                    }
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {

    use std::{fs::File, io::Read};
    use tokio::io::AsyncReadExt;

    use crate::{
        commons::struct_to_json_string,
        tasks::osscompare::{DateTime, DiffExpires, ObjectDiff},
        s3::OSSDescription,
    };

    //cargo test osstask::osscompare::test::test_object_diff -- --nocapture
    #[test]
    fn test_object_diff() {
        let abs = i64::abs(990 - 870);
        println!("{}", abs);

        let expires_diff = DiffExpires {
            key: "abc".to_string(),
            source_expires: Some(DateTime {
                seconds: 1212139921,
                subsecond_nanos: 132132,
            }),
            target_expires: Some(DateTime {
                seconds: 1212138821,
                subsecond_nanos: 132132,
            }),
        };
        let diff = ObjectDiff::ExpiresDiff(expires_diff);
        let diff_str = struct_to_json_string(&diff);
        println!("{}", diff_str.unwrap());

        let v_a = vec![1, 2, 3];
        let v_b = vec![1, 4, 3];

        for (idx, byte) in v_a.iter().enumerate() {
            if !byte.eq(&v_b[idx]) {
                println!("{}", idx);
            }
        }
    }

    //cargo test osstask::osscompare::test::test_compare_oss_local_by_stream -- --nocapture
    #[test]
    fn test_compare_oss_local_by_stream() {
        // 获取oss连接参数
        let vec_oss = crate::commons::read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let oss_desc = vec_oss[0].clone();
        let jd_client = oss_desc.gen_oss_client().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();

        // let key = "hW*P1696820575919885000";
        // let path = "/tmp/files/hW*P1696820575919885000";

        let key = "Bn##1696829067656579000";
        let path = "/tmp/files/Bn##1696829067656579000";
        let local_file = "/tmp/files/Bn##1696829067656579000.local";

        rt.block_on(async {
            // 上传本地文件
            let _ = jd_client
                .upload_from_local("jsw-bucket-1", key, path, 209715200, 10485760)
                .await;

            // let byte_stream = jd_client
            //     .get_object_bytes("jsw-bucket-1", key)
            //     .await
            //     .unwrap();

            let resp = jd_client.get_object("jsw-bucket-1", key).await.unwrap();
            // let mut read = byte_stream.into_async_read();
            let mut obj_len = TryInto::<usize>::try_into(resp.content_length()).unwrap();
            let mut read = resp.body.into_async_read();
            let buffer_size = 1048577;
            let mut file = File::open(local_file).unwrap();

            loop {
                if obj_len > buffer_size {
                    let mut oss_buffer = vec![0; buffer_size];
                    let mut file_buffer = vec![0; buffer_size];
                    let size = read.read_exact(&mut oss_buffer).await.unwrap();
                    let _read_count = file.read(&mut file_buffer).unwrap();
                    obj_len -= buffer_size;
                    println!("{}", oss_buffer.eq(&file_buffer));
                    println!("size: {:?}", size);
                    continue;
                } else {
                    let mut oss_buffer = vec![0; obj_len];
                    let mut file_buffer = vec![0; obj_len];
                    let size = read.read_exact(&mut oss_buffer).await.unwrap();
                    let _read_count = file.read(&mut file_buffer).unwrap();
                    println!("{}", oss_buffer.eq(&file_buffer));
                    println!("size: {:?}", size);
                    break;
                }
            }

            // let buf = BufReader::new(byte_stream.into_async_read()).read_line(buf);
        });
    }
}