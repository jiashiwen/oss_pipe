use std::fs;

use crate::{checkpoint::CheckPoint, commons::read_lines, s3::OSSDescription};
use anyhow::Result;

use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use snowflake::SnowflakeIdGenerator;
use tokio::{runtime, task};
use walkdir::WalkDir;

const OBJECT_LIST_FILE_NAME: &'static str = ".objlist";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TaskType {
    Download,
    Upload,
    Transfer,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TaskDescription {
    Download(TaskDownload),
    Upload(TaskUpLoad),
    Transfer(TaskTransfer),
}

// ToDo
// 抽象 task
impl TaskDescription {
    pub async fn exec(&self) -> Result<()> {
        return match self {
            TaskDescription::Download(d) => d.execute().await,
            TaskDescription::Upload(u) => u.execute().await,
            TaskDescription::Transfer(t) => t.execute().await,
        };
    }

    pub fn exec_rayon(&self) -> Result<()> {
        match self {
            TaskDescription::Download(_) => todo!(),
            TaskDescription::Upload(_) => todo!(),
            TaskDescription::Transfer(t) => t.execute_rayon(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Task {
    #[serde(default = "Task::id_default")]
    pub task_id: String,
    #[serde(default = "Task::name_default")]
    pub name: String,
    pub task_desc: TaskDescription,
}

impl Task {
    fn id_default() -> String {
        task_id_generator().to_string()
    }
    fn name_default() -> String {
        "default_name".to_string()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskTransfer {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub bach_size: i32,
    pub task_threads: usize,
}

impl TaskTransfer {
    pub fn task_type(&self) -> TaskType {
        TaskType::Transfer
    }

    pub fn execute_rayon(&self) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let pool = ThreadPoolBuilder::new()
            .num_threads(self.task_threads)
            .build()?;
        let object_list_file = OBJECT_LIST_FILE_NAME.to_string();
        let _ = fs::remove_file(object_list_file.clone());

        // 生成文件清单，文件清单默认文件存储在文件存储目录下 .objlist
        rt.block_on(async {
            println!("exec rayon");
            let source_client = self.source.gen_oss_client_ref().unwrap();
            let r = source_client
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

                    let checkpoint_file = ".checkpoint.yml".to_string();
                    let _ = checkpoint.save_to_file(&checkpoint_file);
                    checkpoint_counter = 0;

                    let keys = vec_key.clone();
                    pc.spawn(move |_| {
                        let client_source = self.source.gen_oss_client_ref().unwrap();
                        let client_target = self.target.gen_oss_client_ref().unwrap();
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async {
                            for key in keys {
                                let bytes = client_source
                                    .get_object_bytes(self.source.bucket.clone(), key.clone())
                                    .await;
                                match bytes {
                                    Ok(b) => {
                                        let r = client_target
                                            .upload_object_bytes(self.target.bucket.clone(), key, b)
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
                        });
                    });
                }
            }
        });
        Ok(())
    }

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
    pub source: OSSDescription,
    pub local_path: String,
    pub bach_size: i32,
    pub task_threads: usize,
}

impl TaskDownload {
    pub fn task_type(&self) -> TaskType {
        TaskType::Download
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
                        println!("keys: {:?}", keys.clone());
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
            .enable_all()
            .max_io_events_per_tick(self.task_threads)
            .build()?;
        let mut v_handle: Box<Vec<task::JoinHandle<()>>> = Box::new(vec![]);
        let object_list_file = self.local_path.clone() + "/" + OBJECT_LIST_FILE_NAME;

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
            let lines = read_lines(object_list_file.clone()).unwrap();
            // 根据清单下载文件
            for line in lines {
                checkpoint_counter += 1;
                if let Result::Ok(f) = line {
                    let len = f.bytes().len() + "\n".bytes().len();
                    fileposition += len;

                    if !f.ends_with("/") {
                        vec_key.push(f);
                    }
                };

                while v_handle.len() >= self.task_threads {
                    v_handle.retain(|h| !h.is_finished());
                }

                if checkpoint_counter.eq(&self.bach_size) {
                    let vk = vec_key.clone();
                    let source = self.source.clone();
                    let bucket = self.source.bucket.clone();
                    let dir = self.local_path.clone();
                    let client = source.gen_oss_client_ref().unwrap();
                    let handle = tokio::spawn(async move {
                        // let client = source.gen_oss_client_ref().unwrap();
                        if let Err(e) = client
                            .download_objects_to_local(bucket.clone(), vk, dir.clone())
                            .await
                        {
                            log::error!("{}", e);
                        };
                        // for key in vk {
                        //     if let Err(e) = client
                        //         .download_object_to_local(bucket.clone(), key, dir.clone())
                        //         .await
                        //     {
                        //         log::error!("{}", e);
                        //     };
                        // }
                    });
                    // 新增连接池handle
                    v_handle.push(handle);

                    // 记录checkpoint
                    let position: u64 = fileposition.try_into().unwrap();
                    let checkpoint = CheckPoint {
                        execute_file_path: object_list_file.clone(),
                        execute_position: position,
                    };
                    let checkpoint_file = self.local_path.clone() + &"/.checkpoint.yml".to_string();
                    let _ = checkpoint.save_to_file(&checkpoint_file);

                    // 清理临时Vec
                    vec_key.clear();
                }
            }

            if vec_key.len() > 0 {
                let vk = vec_key.clone();
                let source = self.source.clone();
                let bucket = self.source.bucket.clone();
                let dir = self.local_path.clone();
                while v_handle.len() >= self.task_threads {
                    v_handle.retain(|h| !h.is_finished());
                }
                let handle = tokio::spawn(async move {
                    let client = source.gen_oss_client_ref().unwrap();
                    for key in vk {
                        if let Err(e) = client
                            .download_object_to_local(bucket.clone(), key, dir.clone())
                            .await
                        {
                            log::error!("{}", e);
                        };
                    }
                });

                // 记录checkpoint
                let position: u64 = fileposition.try_into().unwrap();
                let checkpoint = CheckPoint {
                    execute_file_path: object_list_file.clone(),
                    execute_position: position,
                };
                let checkpoint_file = self.local_path.clone() + &"/.checkpoint.yml".to_string();
                let _ = checkpoint.save_to_file(&checkpoint_file);
                v_handle.push(handle);
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
    pub target: OSSDescription,
    pub local_path: String,
    pub bach_size: i32,
    pub task_threads: usize,
}

impl TaskUpLoad {
    pub fn task_type(&self) -> TaskType {
        TaskType::Upload
    }
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

#[cfg(test)]
mod test {
    use crate::{
        commons::{struct_to_json_string, struct_to_yaml_string},
        s3::OSSDescription,
    };

    use super::{task_id_generator, Task, TaskDescription, TaskDownload};

    //cargo test osstask::task::test::test_task -- --nocapture
    #[test]
    fn test_task() {
        let task_desc = TaskDownload {
            source: OSSDescription::default(),
            local_path: "/tmp".to_string(),
            bach_size: 200,
            task_threads: 2,
        };

        let task = Task {
            task_id: task_id_generator().to_string(),
            name: "desc".to_string(),
            task_desc: TaskDescription::Download(task_desc),
        };

        println!("struct {:?}", task);

        let task_yml = struct_to_yaml_string(&task).unwrap();
        println!("yaml is {}", task_yml);

        // let t = read_yaml_file::<Task>("task_example_yml/download_emun.yml").unwrap();
        // println!("{:?}", t);

        let t_json = struct_to_json_string(&task).unwrap();
        println!("json {}", t_json);
    }
}
