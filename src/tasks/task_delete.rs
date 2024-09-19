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
use aws_sdk_s3::types::ObjectIdentifier;
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
    #[serde(default = "TaskDefaultParameters::start_from_checkpoint_default")]
    pub start_from_checkpoint: bool,
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
            start_from_checkpoint: TaskDefaultParameters::start_from_checkpoint_default(),
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

        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            TRANSFER_CHECK_POINT_FILE,
            "",
        );

        let mut list_file_position = FilePosition::default();

        let mut executed_file = FileDescription {
            path: gen_file_path(
                self.attributes.meta_dir.as_str(),
                TRANSFER_OBJECT_LIST_FILE_PREFIX,
                now.as_secs().to_string().as_str(),
            ),
            size: 0,
            total_lines: 0,
        };

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
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        let reg_filter = regex_filter.clone();
        let last_modify_filter = self.attributes.last_modify_filter.clone();

        let objects_list_file = match self.attributes.start_from_checkpoint {
            true => {
                let checkpoint = get_task_checkpoint(&check_point_file).context(format!(
                    "{}:{}",
                    file!(),
                    line!()
                ))?;
                let f =
                    checkpoint
                        .seeked_execute_file()
                        .context(format!("{}:{}", file!(), line!()))?;
                list_file_position = checkpoint.executed_file_position;
                executed_file = checkpoint.executed_file;
                f
            }
            false => {
                let mut interrupt = false;
                // 获取删除列表
                rt.block_on(async {
                    let pd = prompt_processbar("Generating object list ...");
                    let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());
                    match c
                        .append_object_list_to_file(
                            oss_d.bucket.clone(),
                            oss_d.prefix.clone(),
                            self.attributes.objects_per_batch,
                            &executed_file.path,
                            reg_filter,
                            last_modify_filter,
                        )
                        .await
                    {
                        Ok(f) => {
                            log::info!("append file: {:?}", f);
                            executed_file = f;
                        }
                        Err(e) => {
                            log::error!("{:?}", e);
                            interrupt = true;
                        }
                    };
                    pd.finish_with_message("object list generated");
                });

                if interrupt {
                    return Err(anyhow!("get object list error"));
                }

                let f = File::open(executed_file.path.as_str())?;
                f
            }
        };

        // let objects_list_file = File::open(executed_file.path.as_str())?;
        let o_d = oss_d.clone();

        // sys_set 用于执行checkpoint、notify等辅助任务
        let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        let mut execut_set = JoinSet::new();
        rt.block_on(async {
            // 启动checkpoint记录线程
            let stock_status_saver = TaskStatusSaver {
                check_point_path: check_point_file.clone(),
                executed_file: executed_file.clone(),
                stop_mark: Arc::clone(&stop_mark),
                list_file_positon_map: Arc::clone(&offset_map),
                file_for_notify: None,
                task_stage: TransferStage::Stock,
                interval: 3,
            };
            let task_id = self.task_id.clone();
            sys_set.spawn(async move {
                stock_status_saver.snapshot_to_file(task_id).await;
            });

            let mut vec_record = vec![];

            let lines = io::BufReader::new(objects_list_file).lines();
            for (num, line) in lines.enumerate() {
                if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                match line {
                    Ok(key) => {
                        let len = key.bytes().len() + "\n".bytes().len();
                        list_file_position.offset += len;
                        list_file_position.line_num += 1;

                        if !key.ends_with("/") {
                            let record = ListedRecord {
                                key,
                                offset: list_file_position.offset,
                                line_num: list_file_position.line_num,
                            };

                            if let Some(ref f) = regex_filter {
                                if f.filter(&record.key) {
                                    vec_record.push(record);
                                }
                            }
                        }
                    }
                    Err(e) => log::error!("{:?}", e),
                }

                if vec_record
                    .len()
                    .to_string()
                    .eq(&self.attributes.objects_per_batch.to_string())
                    || executed_file
                        .total_lines
                        .eq(&TryInto::<u64>::try_into(num + 1).unwrap())
                        && vec_record.len() > 0
                {
                    while execut_set.len() >= self.attributes.task_parallelism {
                        execut_set.join_next().await;
                    }
                    let c = match o_d.gen_oss_client() {
                        Ok(c) => c,
                        Err(e) => {
                            log::error!("{:?}", e);
                            continue;
                        }
                    };
                    let mut keys = vec![];

                    for record in &vec_record {
                        if let Ok(obj_id) = ObjectIdentifier::builder()
                            .set_key(Some(record.key.clone()))
                            .build()
                        {
                            keys.push(obj_id);
                        }
                    }
                    let bucket = o_d.bucket.clone();
                    let o_m = offset_map.clone();
                    let s_m = stop_mark.clone();
                    let subffix = &vec_record[0].offset.to_string();
                    let mut offset_key = OFFSET_PREFIX.to_string();
                    offset_key.push_str(&subffix);
                    let f_p = FilePosition {
                        offset: vec_record[0].offset,
                        line_num: vec_record[0].line_num,
                    };

                    execut_set.spawn(async move {
                        if s_m.load(std::sync::atomic::Ordering::SeqCst) {
                            return;
                        }
                        // 插入文件offset记录
                        o_m.insert(offset_key.clone(), f_p);
                        match c.remove_objects(bucket.as_str(), keys).await {
                            Ok(_) => {
                                log::info!("remove objects ok")
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                                s_m.store(true, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                        o_m.remove(&offset_key);
                    });

                    vec_record.clear();
                }
            }

            if execut_set.len() > 0 {
                execut_set.join_next().await;
            }
            // 配置停止 offset save 标识为 true
            stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);

            while sys_set.len() > 0 {
                task::yield_now().await;
                sys_set.join_next().await;
            }

            let mut checkpoint = match get_task_checkpoint(&check_point_file) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };
            checkpoint.executed_file_position.line_num = checkpoint.executed_file.total_lines;
            checkpoint.executed_file_position.offset =
                TryInto::<usize>::try_into(checkpoint.executed_file.size).unwrap();
            if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
                log::error!("{:?}", e);
            };
        });

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DeleteBucketExecutor {
    pub source: OSSDescription,
    pub stop_mark: Arc<AtomicBool>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: DeleteTaskAttributes,
    pub list_file_path: String,
}

impl DeleteBucketExecutor {
    pub async fn delete_listed_records(&self, records: Vec<ListedRecord>) {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let source_client = match self.source.gen_oss_client() {
            Ok(c) => c,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };
        let s_c = Arc::new(source_client);

        // 插入文件offset记录
        self.offset_map.insert(
            offset_key.clone(),
            FilePosition {
                offset: records[0].offset,
                line_num: records[0].line_num,
            },
        );

        let mut del_objs = vec![];

        for record in records {
            match ObjectIdentifier::builder().key(record.key).build() {
                Ok(k) => del_objs.push(k),
                Err(e) => {
                    log::error!("{:?}", e);
                    continue;
                }
            };
        }

        if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }

        if let Err(e) = s_c.remove_objects(&self.source.bucket, del_objs).await {
            log::error!("{:?}", e);
        };

        self.offset_map.remove(&offset_key);
    }
}
