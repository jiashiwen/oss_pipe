use super::{ObjectStorage, TaskDefaultParameters};
use crate::{
    commons::LastModifyFilter,
    s3::{oss_client::OssClient, OSSDescription},
};
use anyhow::Result;
use aws_sdk_s3::types::Object;
use serde::{Deserialize, Serialize};
use tokio::{runtime, task::JoinSet};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteTaskAttributes {
    #[serde(default = "TaskDefaultParameters::objects_per_batch_default")]
    pub objects_per_batch: i32,
    #[serde(default = "TaskDefaultParameters::task_parallelism_default")]
    pub task_parallelism: usize,
    // #[serde(default = "TaskDefaultParameters::max_errors_default")]
    // pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    // #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    // pub target_exists_skip: bool,
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
impl TaskDeleteBucket {
    pub fn exec_multi_threads(&self) -> Result<()> {
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .enable_all()
            .build()?;

        let mut set: JoinSet<()> = JoinSet::new();
        let mut imterrupt = false;

        rt.block_on(async {
            let oss_d = match self.source.clone() {
                ObjectStorage::Local(_) => {
                    imterrupt = true;
                    return;
                }
                ObjectStorage::OSS(d) => d,
            };

            let client = match oss_d.gen_oss_client() {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

            let resp = client
                .list_objects(
                    oss_d.bucket.clone(),
                    oss_d.prefix.clone(),
                    self.attributes.objects_per_batch,
                    None,
                )
                .await
                .unwrap();
            let mut token = resp.next_token;

            if let Some(objects) = resp.object_list {
                let c = client.clone();
                let b = oss_d.bucket.clone();
                while set.len() >= self.attributes.task_parallelism {
                    set.join_next().await;
                }
                set.spawn(async move {
                    delete_objects(c, &b, objects).await;
                });
            }

            while token.is_some() {
                log::info!("{:?}", token);
                let resp = client
                    .list_objects(
                        oss_d.bucket.clone(),
                        oss_d.prefix.clone(),
                        self.attributes.objects_per_batch,
                        None,
                    )
                    .await
                    .unwrap();

                if let Some(objects) = resp.object_list {
                    while set.len() >= self.attributes.task_parallelism {
                        set.join_next().await;
                    }
                    let c = client.clone();
                    let b = oss_d.bucket.clone();

                    set.spawn(async move {
                        delete_objects(c, &b, objects).await;
                    });
                }
                token = resp.next_token;
            }

            while set.len() > 0 {
                set.join_next().await;
            }
        });

        Ok(())
    }
}

async fn delete_objects(client: OssClient, bucket: &str, objs: Vec<Object>) {
    for obj in objs {
        let key = match obj.key() {
            Some(k) => k,
            None => continue,
        };
        if let Err(e) = client
            .client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            log::error!("{:?}", e);
        };
    }
}
