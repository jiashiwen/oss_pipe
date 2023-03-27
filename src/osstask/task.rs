use std::{fs, thread, time::Duration};

use crate::{checkpoint::CheckPoint, commons::read_lines, s3::OSSDescription};
use anyhow::Result;

use log4rs::Handle;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use snowflake::SnowflakeIdGenerator;
use tokio::{runtime, task};
use walkdir::WalkDir;

const OBJECT_LIST_FILE_NAME: &'static str = ".objlist";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TaskDescription {
    Download(TaskDownload),
    Upload(TaskUpLoad),
    Transfer(TaskTransfer),
}

// ToDo
// 抽象 task
impl TaskDescription {
    fn exec(&self) {
        match self {
            TaskDescription::Download(d) => {
                d.execute();
            }
            TaskDescription::Upload(u) => {
                u.execute();
            }
            TaskDescription::Transfer(t) => {
                t.execute();
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Task {
    #[serde(default = "TaskDownload::task_id_default")]
    pub task_id: String,
    #[serde(default = "TaskDownload::task_description_default")]
    pub description: String,
    pub task: TaskDescription,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskTransfer {
    #[serde(default = "TaskDownload::task_id_default")]
    pub task_id: String,
    #[serde(default = "TaskDownload::task_description_default")]
    pub description: String,
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub bach_size: i32,
    pub task_threads: usize,
}

impl TaskTransfer {
    pub async fn execute(&self) -> Result<()> {
        // 记录源端object列表
        let client_source = self.source.gen_oss_client_ref()?;
        let client_target = self.target.gen_oss_client_ref()?;

        // 生成文件清单，文件清单默认文件存储在文件存储目录下 .objlist
        let object_list_file = OBJECT_LIST_FILE_NAME.to_string();
        let _ = fs::remove_file(object_list_file.clone());
        let r = client_source
            .append_all_object_list_to_file(
                self.source.bucket.clone(),
                self.source.prefix.clone(),
                self.bach_size,
                object_list_file.clone(),
            )
            .await;

        if let Err(e) = r {
            log::error!("{}", e);
        };

        // 按列表传输object from source to target
        let lines = read_lines(object_list_file.clone())?;
        for line in lines {
            if let Result::Ok(f) = line {
                if !f.ends_with("/") {
                    let bytes = client_source
                        .get_object_bytes(self.source.bucket.clone(), f.clone())
                        .await;

                    match bytes {
                        Ok(b) => {
                            let r = client_target
                                .upload_object_bytes(self.target.bucket.clone(), f.clone(), b)
                                .await;

                            if let Err(e) = r {
                                log::error!("{}", e);
                                continue;
                            };
                        }
                        Err(e) => {
                            log::error!("{}", e);
                            continue;
                        }
                    }
                }
            };
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TaskDownload {
    #[serde(default = "TaskDownload::task_id_default")]
    pub task_id: String,
    #[serde(default = "TaskDownload::task_description_default")]
    pub description: String,
    pub source: OSSDescription,
    pub local_path: String,
    pub bach_size: i32,
    pub task_threads: usize,
}

impl TaskDownload {
    fn task_id_default() -> String {
        task_id_generator().to_string()
    }
    fn task_description_default() -> String {
        String::from("download")
    }

    // 多线程 rayon方案
    pub fn execute_multi_thread(&self) -> Result<()> {
        println!("exec download multhithread");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = self.source.gen_oss_client_ref()?;
        let object_list_file = self.local_path.clone() + "/" + OBJECT_LIST_FILE_NAME;

        let pool = ThreadPoolBuilder::new()
            .num_threads(self.task_threads)
            .build()?;

        let _ = fs::remove_file(object_list_file.clone());
        rt.block_on(async {
            // 生成文件清单，文件清单默认文件存储在文件存储目录下 .objlist
            let r = client
                .append_all_object_list_to_file(
                    self.source.bucket.clone(),
                    self.source.prefix.clone(),
                    self.bach_size,
                    object_list_file.clone(),
                )
                .await;
            if let Err(e) = r {
                log::error!("{}", e);
            };
        });

        let mut checkpoint_counter = 0;
        let mut fileposition: usize = 0;
        let lines = read_lines(object_list_file.clone())?;
        pool.scope(|pc| {
            let mut vec_key = vec![];
            for line in lines {
                checkpoint_counter += 1;
                if let Result::Ok(f) = line {
                    // 计算文件offset
                    let len = f.bytes().len() + "\n".bytes().len();
                    fileposition += len;

                    if !f.ends_with("/") {
                        vec_key.push(f.clone());
                    }
                };
                if checkpoint_counter.eq(&self.bach_size) {
                    let position: u64 = fileposition.try_into().unwrap();
                    let checkpoint = CheckPoint {
                        execute_file_path: object_list_file.clone(),
                        execute_position: position,
                    };

                    let checkpoint_file = self.local_path.clone() + &"/.checkpoint.yml".to_string();
                    let _ = checkpoint.save_to_file(&checkpoint_file);
                    checkpoint_counter = 0;

                    let keys = vec_key.clone();

                    pc.spawn(move |_| {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async {
                            let client = self.source.clone().gen_oss_client_ref().unwrap();
                            for key in keys {
                                let r = client
                                    .download_object_to_local(
                                        self.source.bucket.clone(),
                                        key.clone(),
                                        self.local_path.clone(),
                                    )
                                    .await;
                                if let Err(e) = r {
                                    log::error!("{}", e);
                                };
                            }
                        })
                    });

                    vec_key.clear();
                }
            }

            // 补充最后一批操作逻辑
            if vec_key.len() > 0 {
                pc.spawn(move |_| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(async {
                        let client = self.source.clone().gen_oss_client_ref().unwrap();
                        for key in vec_key {
                            let r = client
                                .download_object_to_local(
                                    self.source.bucket.clone(),
                                    key.clone(),
                                    self.local_path.clone(),
                                )
                                .await;
                            if let Err(e) = r {
                                log::error!("{}", e);
                            };
                        }
                    })
                });
                let position: u64 = fileposition.try_into().unwrap();
                let checkpoint = CheckPoint {
                    execute_file_path: object_list_file.clone(),
                    execute_position: position,
                };
                let checkpoint_file = self.local_path.clone() + &"/.checkpoint.yml".to_string();
                let _ = checkpoint.save_to_file(&checkpoint_file);
            }
        });

        println!("exec download multhithread finish");

        Ok(())
    }

    // 实现基于tokio的多线程方案
    pub fn execute_tokio(&self) -> Result<()> {
        let client = self.source.gen_oss_client_ref()?;
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(self.task_threads)
            .enable_time()
            .build()?;
        let mut v_handle: Box<Vec<task::JoinHandle<()>>> = Box::new(vec![]);
        let object_list_file = self.local_path.clone() + "/" + OBJECT_LIST_FILE_NAME;
        let lines = read_lines(object_list_file.clone())?;

        rt.block_on(async {
            // 生成文件清单，文件清单默认文件存储在文件存储目录下 .objlist

            let _ = fs::remove_file(object_list_file.clone());
            let r = client
                .append_all_object_list_to_file(
                    self.source.bucket.clone(),
                    self.source.prefix.clone(),
                    self.bach_size,
                    object_list_file.clone(),
                )
                .await;

            if let Err(e) = r {
                log::error!("{}", e);
            };

            let mut checkpoint_counter = 0;
            let mut fileposition: usize = 0;
            let mut vec_key: Vec<String> = vec![];
            // 根据清单下载文件
            for line in lines {
                checkpoint_counter += 1;
                if let Result::Ok(f) = line {
                    let len = f.bytes().len() + "\n".bytes().len();
                    fileposition += len;

                    if !f.ends_with("/") {
                        vec_key.push(f);

                        // let r = client
                        //     .download_object_to_local(
                        //         self.source.bucket.clone(),
                        //         f.clone(),
                        //         self.local_path.clone(),
                        //     )
                        //     .await;
                        // if let Err(e) = r {
                        //     log::error!("{}", e);
                        // };
                    }
                };

                if checkpoint_counter.eq(&self.bach_size) {
                    let vk = vec_key.clone();
                    let source = self.source.clone();
                    let bucket = self.source.bucket.clone();
                    let dir = self.local_path.clone();

                    let handle = tokio::spawn(async move {
                        let client = source.gen_oss_client_ref().unwrap();
                        for key in vk {
                            client
                                .download_object_to_local(bucket.clone(), key, dir.clone())
                                .await
                                .unwrap();
                        }
                    });

                    v_handle.push(handle);
                }
            }
        });
        Ok(())
    }

    pub async fn execute(&self) -> Result<()> {
        let client = self.source.gen_oss_client_ref()?;

        // 生成文件清单，文件清单默认文件存储在文件存储目录下 .objlist
        let object_list_file = self.local_path.clone() + "/" + OBJECT_LIST_FILE_NAME;
        let _ = fs::remove_file(object_list_file.clone());
        let r = client
            .append_all_object_list_to_file(
                self.source.bucket.clone(),
                self.source.prefix.clone(),
                self.bach_size,
                object_list_file.clone(),
            )
            .await;

        if let Err(e) = r {
            log::error!("{}", e);
        };

        // 根据清单下载文件
        let lines = read_lines(object_list_file.clone())?;
        for line in lines {
            if let Result::Ok(f) = line {
                if !f.ends_with("/") {
                    let r = client
                        .download_object_to_local(
                            self.source.bucket.clone(),
                            f.clone(),
                            self.local_path.clone(),
                        )
                        .await;
                    if let Err(e) = r {
                        log::error!("{}", e);
                    };
                }
            };
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TaskUpLoad {
    #[serde(default = "TaskDownload::task_id_default")]
    pub task_id: String,
    #[serde(default = "TaskDownload::task_description_default")]
    pub description: String,
    pub target: OSSDescription,
    pub local_path: String,
    pub bach_size: i32,
    pub task_threads: usize,
}

impl TaskUpLoad {
    pub async fn execute(&self) -> Result<()> {
        let client = self.target.gen_oss_client_ref()?;
        // 遍历目录并上传
        for entry in WalkDir::new(&self.local_path)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                let key = &p[self.local_path.len() + 1..];
                if key.eq(OBJECT_LIST_FILE_NAME) {
                    continue;
                }
                if let Err(e) = client
                    .upload_object_from_local(
                        self.target.bucket.clone(),
                        key.to_string(),
                        p.to_string(),
                    )
                    .await
                {
                    log::error!("{}", e);
                };
            };
        }
        Ok(())
    }
}

pub fn task_id_generator() -> i64 {
    let mut id_generator_generator = SnowflakeIdGenerator::new(1, 1);
    let id = id_generator_generator.real_time_generate();
    id
}
