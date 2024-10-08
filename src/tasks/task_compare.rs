use super::{
    de_usize_from_str, gen_file_path, se_usize_to_str, task_actions::CompareTaskActions,
    ObjectStorage, TaskCompareLocal2Local, TaskCompareLocal2Oss, TaskCompareOss2Local,
    TaskCompareOss2Oss, TaskDefaultParameters, TaskStatusSaver, TransferStage,
    COMPARE_CHECK_POINT_FILE, COMPARE_RESULT_PREFIX, COMPARE_SOURCE_OBJECT_LIST_FILE_PREFIX,
    OFFSET_PREFIX,
};
use crate::{
    checkpoint::{get_task_checkpoint, CheckPoint, FileDescription, FilePosition, ListedRecord},
    commons::{
        json_to_struct, prompt_processbar, quantify_processbar, LastModifyFilter, RegexFilter,
    },
};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{self},
    fs::{self, File},
    io::{BufRead, BufReader, Lines, Write},
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tabled::builder::Builder;
use tokio::{runtime, sync::Semaphore, task::JoinSet};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObjectDiff {
    pub source: String,
    pub target: String,
    pub diff: Diff,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Diff {
    ExistsDiff(DiffExists),
    LengthDiff(DiffLength),
    ExpiresDiff(DiffExpires),
    ContentDiff(DiffContent),
    MetaDiff(DiffMeta),
}

impl Diff {
    pub fn name(&self) -> String {
        match self {
            Diff::ExistsDiff(_) => "exists_diff".to_string(),
            Diff::LengthDiff(_) => "length_diff".to_string(),
            Diff::ExpiresDiff(_) => "exprires_diff".to_string(),
            Diff::ContentDiff(_) => "content_diff".to_string(),
            Diff::MetaDiff(_) => "meta_data_diff".to_string(),
        }
    }
}

impl fmt::Display for Diff {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Diff::ExistsDiff(d) => {
                write!(f, "{};{}", d.source_exists, d.target_exists)
            }
            Diff::LengthDiff(d) => {
                write!(f, "{};{}", d.source_content_len, d.target_content_len)
            }
            Diff::ExpiresDiff(d) => {
                write!(f, "{:?};{:?}", d.source_expires, d.target_expires)
            }
            Diff::ContentDiff(d) => {
                write!(
                    f,
                    "{};{};{}",
                    d.stream_position, d.source_byte, d.target_byte
                )
            }
            Diff::MetaDiff(d) => {
                write!(f, "{:?};{:?}", d.source_meta, d.target_meta)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffExists {
    pub source_exists: bool,
    pub target_exists: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffExpires {
    pub source_expires: Option<DateTime>,
    pub target_expires: Option<DateTime>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DateTime {
    pub seconds: i64,
    pub subsecond_nanos: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffLength {
    pub source_content_len: i128,
    pub target_content_len: i128,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffContent {
    pub stream_position: usize,
    pub source_byte: u8,
    pub target_byte: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffMeta {
    pub source_meta: Option<HashMap<std::string::String, std::string::String>>,
    pub target_meta: Option<HashMap<std::string::String, std::string::String>>,
}

impl ObjectDiff {
    pub fn save_json_to_file(&self, file: &mut File) -> Result<()> {
        // 获取文件路径，若不存在则创建路径
        let mut json = serde_json::to_string(self)?;
        json.push_str("\n");
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompareTaskAttributes {
    #[serde(default = "TaskDefaultParameters::objects_per_batch_default")]
    pub objects_per_batch: i32,
    #[serde(default = "TaskDefaultParameters::task_parallelism_default")]
    pub task_parallelism: usize,
    // #[serde(default = "TaskDefaultParameters::max_errors_default")]
    // pub max_errors: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub start_from_checkpoint: bool,
    #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub large_file_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunk_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub multi_part_chunk: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_max_parallelism_default")]
    pub multi_part_max_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::exprirs_diff_scope_default")]
    pub exprirs_diff_scope: i64,
    // #[serde(default = "TaskDefaultParameters::continuous_default")]
    // pub continuous: bool,
    #[serde(default = "TaskDefaultParameters::last_modify_filter_default")]
    pub last_modify_filter: Option<LastModifyFilter>,
}

impl Default for CompareTaskAttributes {
    fn default() -> Self {
        Self {
            objects_per_batch: TaskDefaultParameters::objects_per_batch_default(),
            task_parallelism: TaskDefaultParameters::task_parallelism_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            start_from_checkpoint: TaskDefaultParameters::target_exists_skip_default(),
            large_file_size: TaskDefaultParameters::large_file_size_default(),
            multi_part_chunk: TaskDefaultParameters::multi_part_chunk_size_default(),
            multi_part_max_parallelism: TaskDefaultParameters::multi_part_max_parallelism_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
            last_modify_filter: TaskDefaultParameters::last_modify_filter_default(),
            exprirs_diff_scope: TaskDefaultParameters::exprirs_diff_scope_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompareCheckOption {
    #[serde(default = "CompareCheckOption::default_check_content_length")]
    check_content_length: bool,
    #[serde(default = "CompareCheckOption::default_check_expires")]
    check_expires: bool,
    #[serde(default = "CompareCheckOption::default_check_content")]
    check_content: bool,
    #[serde(default = "CompareCheckOption::default_check_meta_data")]
    check_meta_data: bool,
}

impl Default for CompareCheckOption {
    fn default() -> Self {
        Self {
            check_content_length: CompareCheckOption::default_check_content_length(),
            check_expires: CompareCheckOption::default_check_expires(),
            check_content: CompareCheckOption::default_check_content(),
            check_meta_data: CompareCheckOption::default_check_meta_data(),
        }
    }
}

impl CompareCheckOption {
    pub fn default_check_content_length() -> bool {
        true
    }

    pub fn default_check_expires() -> bool {
        false
    }

    pub fn default_check_content() -> bool {
        false
    }

    pub fn default_check_meta_data() -> bool {
        false
    }

    pub fn check_content_length(&self) -> bool {
        self.check_content_length
    }

    pub fn check_expires(&self) -> bool {
        self.check_expires
    }

    pub fn check_meta_data(&self) -> bool {
        self.check_meta_data
    }

    pub fn check_content(&self) -> bool {
        self.check_content
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct CompareTask {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: ObjectStorage,
    pub target: ObjectStorage,
    pub check_option: CompareCheckOption,
    pub attributes: CompareTaskAttributes,
}

impl Default for CompareTask {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: ObjectStorage::default(),
            target: ObjectStorage::default(),
            check_option: CompareCheckOption::default(),
            attributes: CompareTaskAttributes::default(),
        }
    }
}

impl CompareTask {
    pub fn gen_compare_actions(&self) -> Arc<dyn CompareTaskActions + Send + Sync> {
        match &self.source {
            ObjectStorage::Local(path_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TaskCompareLocal2Local {
                        source: path_s.to_string(),
                        target: path_t.to_string(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TaskCompareLocal2Oss {
                        source: path_s.to_string(),
                        target: oss_t.clone(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
            },
            ObjectStorage::OSS(oss_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TaskCompareOss2Local {
                        source: oss_s.clone(),
                        target: path_t.to_string(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TaskCompareOss2Oss {
                        source: oss_s.clone(),
                        target: oss_t.clone(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
            },
        }
    }

    pub fn start_compare(&self) -> Result<()> {
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .enable_all()
            .max_io_events_per_tick(self.attributes.task_parallelism)
            .build()?;

        // sys_set 用于执行checkpoint、notify等辅助任务
        let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        let mut execut_set = JoinSet::new();

        let task_stop_mark = Arc::new(AtomicBool::new(false));
        let task_err_occur = Arc::new(AtomicBool::new(false));
        let task_multi_part_semaphore =
            Arc::new(Semaphore::new(self.attributes.multi_part_max_parallelism));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());

        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            COMPARE_CHECK_POINT_FILE,
            "",
        );
        let regex_filter =
            RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)?;

        rt.block_on(async {
            let task_compare = self.gen_compare_actions();

            let (compare_list_file, compare_list_file_desc, mut compare_list_file_position) =
                match self
                    .generate_list_file(
                        task_stop_mark.clone(),
                        task_multi_part_semaphore.clone(),
                        task_compare,
                    )
                    .await
                {
                    Ok((f, d, p)) => (f, d, p),
                    Err(e) => {
                        log::error!("{:?}", e);
                        task_err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                };

            let task_id = self.task_id.clone();

            // 启动checkpoint记录线程
            let stock_status_saver = TaskStatusSaver {
                check_point_path: check_point_file.clone(),
                executed_file: compare_list_file_desc.clone(),
                stop_mark: Arc::clone(&task_stop_mark),
                list_file_positon_map: Arc::clone(&offset_map),
                file_for_notify: None,
                task_stage: TransferStage::Stock,
                interval: 3,
            };
            sys_set.spawn(async move {
                stock_status_saver.snapshot_to_file(task_id).await;
            });

            // 启动进度条线程
            let map = Arc::clone(&offset_map);
            let stop_mark = Arc::clone(&task_stop_mark);
            let total = compare_list_file_desc.total_lines;
            let cp = check_point_file.clone();
            sys_set.spawn(async move {
                // Todo 调整进度条
                quantify_processbar(total, stop_mark.clone(), map, &cp, OFFSET_PREFIX).await;
            });
            let task_compare = self.gen_compare_actions();
            let mut vec_keys = vec![];
            // 按列表传输object from source to target
            let lines: Lines<BufReader<File>> = BufReader::new(compare_list_file).lines();
            let s_m = task_stop_mark.clone();
            let to_be_executed =
                match TryInto::<usize>::try_into(total - compare_list_file_position.line_num) {
                    Ok(t) => t,
                    Err(e) => {
                        log::error!("{:?}", e);
                        task_err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                };
            let obj_perbatch = match TryInto::<usize>::try_into((self.attributes.objects_per_batch))
            {
                Ok(t) => t,
                Err(e) => {
                    log::error!("{:?}", e);
                    task_err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
            };

            for (idx, line) in lines.enumerate() {
                if s_m.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                if let Result::Ok(key) = line {
                    let len = key.bytes().len() + "\n".bytes().len();
                    if !key.ends_with("/") {
                        let record = ListedRecord {
                            key,
                            offset: compare_list_file_position.offset,
                            line_num: compare_list_file_position.line_num,
                        };

                        if regex_filter.filter(&record.key) {
                            vec_keys.push(record);
                        }
                    }
                    compare_list_file_position.offset += len;
                    compare_list_file_position.line_num += 1;
                };

                if vec_keys.len().eq(&obj_perbatch)
                    || idx.eq(&(to_be_executed - 1)) && vec_keys.len() > 0
                {
                    while execut_set.len() >= self.attributes.task_parallelism {
                        execut_set.join_next().await;
                    }
                    let vk = vec_keys.clone();
                    let comparator = task_compare.gen_compare_executor(
                        task_stop_mark.clone(),
                        task_err_occur.clone(),
                        task_multi_part_semaphore.clone(),
                        offset_map.clone(),
                    );
                    execut_set.spawn(async move {
                        if let Err(e) = comparator.compare_listed_records(vk).await {
                            log::error!("{:?}", e);
                        }
                    });

                    // 清理临时key vec
                    vec_keys.clear();
                }
            }

            while execut_set.len() > 0 {
                execut_set.join_next().await;
            }
            // 配置停止 offset save 标识为 true
            task_stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);

            log::info!("{:?}", compare_list_file_position);

            let mut checkpoint = match get_task_checkpoint(check_point_file.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

            checkpoint.executed_file_position = compare_list_file_position;
            if let Err(e) = checkpoint.save_to(check_point_file.as_str()) {
                log::error!("{:?}", e);
            };

            while sys_set.len() > 0 {
                sys_set.join_next().await;
            }
        });

        if task_err_occur.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("compare task error"));
        }
        // Todo
        // 持续同步逻辑，循环比较不相等记录，并指定校验次数
        // if self.attributes.continuous {}

        for entry in WalkDir::new(&self.attributes.meta_dir)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.attributes.meta_dir) {
                    continue;
                }

                let key = match &self.attributes.meta_dir.ends_with("/") {
                    true => &p[self.attributes.meta_dir.len()..],
                    false => &p[self.attributes.meta_dir.len() + 1..],
                };

                if key.starts_with(&COMPARE_RESULT_PREFIX) {
                    let result_file = gen_file_path(&self.attributes.meta_dir, key, "");
                    let _ = show_compare_result(&result_file);
                }
            };
        }
        Ok(())
    }

    async fn generate_list_file(
        &self,
        stop_mark: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        task: Arc<dyn CompareTaskActions + Send + Sync>,
    ) -> Result<(File, FileDescription, FilePosition)> {
        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            COMPARE_CHECK_POINT_FILE,
            "",
        );
        return match self.attributes.start_from_checkpoint {
            true => {
                let checkpoint = get_task_checkpoint(check_point_file.as_str())?;
                let list_file_desc = checkpoint.executed_file.clone();
                let list_file_position = checkpoint.executed_file_position.clone();
                let list_file = checkpoint.seeked_execute_file()?;
                Ok((list_file, list_file_desc, list_file_position))
            }
            false => {
                let pd = prompt_processbar("Generating object list ...");
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
                let compare_source_list = gen_file_path(
                    self.attributes.meta_dir.as_str(),
                    COMPARE_SOURCE_OBJECT_LIST_FILE_PREFIX,
                    now.as_secs().to_string().as_str(),
                );
                let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());
                let list_file_desc = task.gen_list_file(&compare_source_list).await?;
                let list_file = File::open(list_file_desc.path.as_str())?;
                pd.finish_with_message("object list generated");
                Ok((list_file, list_file_desc, FilePosition::default()))
            }
        };
    }
}

pub fn show_compare_result(result_file: &str) -> Result<()> {
    let file = File::open(result_file)?;
    let lines: Lines<BufReader<File>> = BufReader::new(file).lines();
    let mut builder = Builder::default();
    for line in lines {
        if let Ok(str) = line {
            let result = json_to_struct::<ObjectDiff>(&str)?;
            let source = result.source;
            let target = result.target;
            let diff = result.diff;

            let raw = vec![source, target, diff.name(), diff.to_string()];
            builder.push_record(raw);
        }
    }

    let header = vec!["source", "target", "diff_type", "diff"];
    builder.insert_record(0, header);

    let table = builder.build();
    // table.with(Style::ascii_rounded());
    println!("{}", table);
    Ok(())
}
