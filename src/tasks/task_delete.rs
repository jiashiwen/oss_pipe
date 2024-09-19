use super::{
    gen_file_path, ObjectStorage, TaskDefaultParameters, TaskStatusSaver, TransferStage,
    OFFSET_PREFIX, TRANSFER_CHECK_POINT_FILE, TRANSFER_OBJECT_LIST_FILE_PREFIX,
};
use crate::{
    checkpoint::{get_task_checkpoint, FileDescription, FilePosition, ListedRecord},
    commons::{prompt_processbar, LastModifyFilter, RegexFilter},
    s3::OSSDescription,
};
use anyhow::Result;
use anyhow::{anyhow, Context};
use aws_sdk_s3::{
    operation::RequestIdExt,
    types::{Object, ObjectIdentifier},
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{self, BufRead, Seek},
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    runtime,
    task::{self, JoinSet},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteTaskAttributes {
    #[serde(default = "TaskDefaultParameters::objects_per_batch_default")]
    pub objects_per_batch: i32,
    #[serde(default = "TaskDefaultParameters::task_parallelism_default")]
    pub task_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    // #[serde(default = "TaskDefaultParameters::start_from_checkpoint_default")]
    // pub start_from_checkpoint: bool,
    // #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    // #[serde(serialize_with = "se_usize_to_str")]
    // #[serde(deserialize_with = "de_usize_from_str")]
    // pub large_file_size: usize,
    // #[serde(default = "TaskDefaultParameters::multi_part_chunk_size_default")]
    // #[serde(serialize_with = "se_usize_to_str")]
    // #[serde(deserialize_with = "de_usize_from_str")]
    // pub multi_part_chunk_size: usize,
    // #[serde(default = "TaskDefaultParameters::multi_part_chunks_per_batch_default")]
    // pub multi_part_chunks_per_batch: usize,
    // #[serde(default = "TaskDefaultParameters::multi_part_parallelism_default")]
    // pub multi_part_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    // #[serde(default = "TaskDefaultParameters::transfer_type_default")]
    // pub transfer_type: TransferType,
    #[serde(default = "TaskDefaultParameters::last_modify_filter_default")]
    pub last_modify_filter: Option<LastModifyFilter>,
}

impl Default for DeleteTaskAttributes {
    fn default() -> Self {
        Self {
            objects_per_batch: TaskDefaultParameters::objects_per_batch_default(),
            task_parallelism: TaskDefaultParameters::task_parallelism_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
            last_modify_filter: TaskDefaultParameters::last_modify_filter_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            // start_from_checkpoint: TaskDefaultParameters::start_from_checkpoint_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskDeleteBucket {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: ObjectStorage,
    pub attributes: DeleteTaskAttributes,
}

impl Default for TaskDeleteBucket {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: ObjectStorage::OSS(OSSDescription::default()),
            attributes: DeleteTaskAttributes::default(),
        }
    }
}

//Todo
// 增加 start_from_checkpoint arttribute
impl TaskDeleteBucket {
    pub fn execute(&self) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let stop_mark = Arc::new(AtomicBool::new(false));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());

        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .enable_all()
            .build()?;

        let oss_d = match self.source.clone() {
            ObjectStorage::Local(_) => {
                return Err(anyhow::anyhow!("parse oss description error"));
            }
            ObjectStorage::OSS(d) => d,
        };

        let client = match oss_d.gen_oss_client() {
            Ok(c) => c,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(e);
            }
        };

        let c = client.clone();
        let o_d = oss_d.clone();

        // sys_set 用于执行checkpoint、notify等辅助任务
        // let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        let mut execut_set = JoinSet::new();
        rt.block_on(async move {
            let list_ossdesc = o_d.clone();
            let resp = match c
                .list_objects(
                    list_ossdesc.bucket.clone(),
                    list_ossdesc.prefix.clone(),
                    self.attributes.objects_per_batch,
                    None,
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{:?}", e);
                    stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
                    return;
                }
            };
            let mut token = resp.next_token;

            if let Some(objects) = resp.object_list {
                let del = DeleteBucketExecutor {
                    source: o_d.clone(),
                    stop_mark: stop_mark.clone(),
                    attributes: self.attributes.clone(),
                };
                execut_set.spawn(async move { del.delete_listed_records(objects).await });
            }

            while token.is_some() {
                let now = SystemTime::now();
                let resp = match c
                    .list_objects(
                        list_ossdesc.bucket.clone(),
                        list_ossdesc.prefix.clone(),
                        self.attributes.objects_per_batch,
                        None,
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("{:?}", e);
                        stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
                        return;
                    }
                };

                log::info!("list objects elapsed:{:?}", now.elapsed());

                if let Some(objects) = resp.object_list {
                    let del = DeleteBucketExecutor {
                        source: o_d.clone(),
                        stop_mark: stop_mark.clone(),
                        attributes: self.attributes.clone(),
                    };
                    while execut_set.len() >= self.attributes.task_parallelism {
                        execut_set.join_next().await;
                    }
                    execut_set.spawn(async move { del.delete_listed_records(objects).await });
                }
                token = resp.next_token;
            }
            while execut_set.len() > 0 {
                execut_set.join_next().await;
            }
            // 配置停止 offset save 标识为 true
            stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DeleteBucketExecutor {
    pub source: OSSDescription,
    pub stop_mark: Arc<AtomicBool>,
    pub attributes: DeleteTaskAttributes,
}

impl DeleteBucketExecutor {
    pub async fn delete_listed_records_batch(&self, records: Vec<Object>) {
        let regex_filter = match RegexFilter::from_vec_option(
            &self.attributes.exclude,
            &self.attributes.include,
        ) {
            Ok(reg) => reg,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };
        let source_client = match self.source.gen_oss_client() {
            Ok(c) => c,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };
        let s_c = Arc::new(source_client);

        let mut del_objs = vec![];

        for record in records {
            if let Some(ref f) = self.attributes.last_modify_filter {
                match record.last_modified() {
                    Some(d) => {
                        if !f.filter(usize::try_from(d.secs()).unwrap()) {
                            continue;
                        }
                    }
                    None => {}
                }
            }

            if let Some(key) = record.key() {
                if let Some(ref reg) = regex_filter {
                    if !reg.filter(key) {
                        continue;
                    }
                }

                match ObjectIdentifier::builder().key(key).build() {
                    Ok(k) => del_objs.push(k),
                    Err(e) => {
                        log::error!("{:?}", e);
                        continue;
                    }
                };
            }
        }

        if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let now = SystemTime::now();
        match s_c.remove_objects(&self.source.bucket, del_objs).await {
            Ok(_) => {
                log::info!("objects deleted")
            }
            Err(e) => {
                log::error!("{:?}", e);
            }
        }
        log::info!("remove elapsed:{:?}", now.elapsed());
    }

    pub async fn delete_listed_records(&self, records: Vec<Object>) {
        let regex_filter = match RegexFilter::from_vec_option(
            &self.attributes.exclude,
            &self.attributes.include,
        ) {
            Ok(reg) => reg,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };
        let source_client = match self.source.gen_oss_client() {
            Ok(c) => c,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };
        let s_c = Arc::new(source_client);

        // let mut del_objs = vec![];
        let now = SystemTime::now();
        for record in records {
            if let Some(ref f) = self.attributes.last_modify_filter {
                match record.last_modified() {
                    Some(d) => {
                        if !f.filter(usize::try_from(d.secs()).unwrap()) {
                            continue;
                        }
                    }
                    None => {}
                }
            }

            if let Some(key) = record.key() {
                if let Some(ref reg) = regex_filter {
                    if !reg.filter(key) {
                        continue;
                    }
                }

                match s_c.remove_object(&self.source.bucket, key).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("{:?}", e);
                        continue;
                    }
                }

                // match ObjectIdentifier::builder().key(key).build() {
                //     Ok(k) => del_objs.push(k),
                //     Err(e) => {
                //         log::error!("{:?}", e);
                //         continue;
                //     }
                // };
            }
        }

        if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }

        // match s_c.remove_objects(&self.source.bucket, del_objs).await {
        //     Ok(_) => {
        //         log::info!("objects deleted")
        //     }
        //     Err(e) => {
        //         log::error!("{:?}", e);
        //     }
        // }
        log::info!("remove elapsed:{:?}", now.elapsed());
    }
}
