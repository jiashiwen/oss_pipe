use super::{TransferStage, OFFSET_PREFIX};
use crate::checkpoint::{get_task_checkpoint, CheckPoint, FileDescription, FilePosition};
use dashmap::DashMap;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::{
    cmp::min,
    fmt::Write,
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::task::yield_now;

pub struct TaskStatusSaver {
    pub check_point_path: String,
    pub executed_file: FileDescription,
    pub stop_mark: Arc<AtomicBool>,
    pub list_file_positon_map: Arc<DashMap<String, FilePosition>>,
    pub file_for_notify: Option<String>,
    pub task_stage: TransferStage,
    pub interval: u64,
}

impl TaskStatusSaver {
    pub async fn snapshot_to_file(&self, task_id: String) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let mut checkpoint = match get_task_checkpoint(&self.check_point_path) {
            Ok(c) => c,
            Err(_) => {
                let mut checkpoint = CheckPoint {
                    task_id,
                    executed_file: self.executed_file.clone(),
                    executed_file_position: FilePosition {
                        offset: 0,
                        line_num: 0,
                    },
                    file_for_notify: self.file_for_notify.clone(),
                    task_stage: self.task_stage,
                    modify_checkpoint_timestamp: i128::from(now.as_secs()),
                    task_begin_timestamp: i128::from(now.as_secs()),
                };
                let _ = checkpoint.save_to(&self.check_point_path);
                checkpoint
            }
        };

        while !self.stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
            let mut file_position = checkpoint.executed_file_position;
            let mut idx = 0;
            for item in self.list_file_positon_map.iter() {
                if item.key().starts_with(OFFSET_PREFIX) {
                    if idx.eq(&0) {
                        file_position = item.value().clone();
                        idx += 1;
                    } else {
                        if file_position.offset > item.value().offset {
                            file_position = item.value().clone();
                        }
                    }
                }
            }

            self.list_file_positon_map.shrink_to_fit();
            checkpoint.executed_file_position = file_position.clone();
            if let Err(e) = checkpoint.save_to(&self.check_point_path) {
                log::error!("{:?},{:?}", e, self.check_point_path);
            } else {
                log::debug!("checkpoint:\n{:?}", checkpoint);
            };

            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
            yield_now().await;
        }
    }
}

// 存量传输进度统计
pub async fn stock_progress_statistics(
    total: u64,
    stop_mark: Arc<AtomicBool>,
    status_map: Arc<DashMap<String, FilePosition>>,
    key_prefix: &str,
    with_progressbar: bool,
) {
    let pb = ProgressBar::new(total);
    let progress_style = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
    )
    .unwrap()
    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
        write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
    })
    .progress_chars("#>-");
    pb.set_style(progress_style);

    while !stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        let line_num = status_map
            .iter()
            .filter(|f| f.key().starts_with(key_prefix))
            .map(|m| m.line_num)
            .min();
        match line_num {
            Some(current) => {
                let new = min(current, total);
                pb.set_position(new);
                log::info!("total:{},executed:{}", total, new)
            }
            None => {}
        }
        yield_now().await;
    }
    log::info!("total:{},executed:{}", total, total);
    pb.set_position(total);
    pb.finish_with_message("Finish");
}
