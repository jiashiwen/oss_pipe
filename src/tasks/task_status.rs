use super::{TransferStage, OFFSET_PREFIX};
use crate::checkpoint::{CheckPoint, FileDescription, FilePosition};
use dashmap::DashMap;
use std::{
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
        let mut checkpoint = CheckPoint {
            task_id,
            executed_file: self.executed_file.clone(),
            executed_file_position: FilePosition {
                offset: 0,
                line_num: 0,
            },
            file_for_notify: self.file_for_notify.clone(),
            task_stage: self.task_stage,
            modify_checkpoint_timestamp: 0,
            task_begin_timestamp: i128::from(now.as_secs()),
        };
        let _ = checkpoint.save_to(&self.check_point_path);

        while !self.stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
            let mut file_position = FilePosition {
                offset: 0,
                line_num: 0,
            };
            // 获取最小offset的FilePosition
            self.list_file_positon_map
                .iter()
                .filter(|item| item.key().starts_with(OFFSET_PREFIX))
                .map(|m| {
                    file_position = m.clone();
                    m.offset
                })
                .min();

            self.list_file_positon_map.shrink_to_fit();
            checkpoint.executed_file_position = file_position.clone();

            if let Err(e) = checkpoint.save_to(&self.check_point_path) {
                log::error!("{},{}", e, self.check_point_path);
            } else {
                log::info!("checkpoint:\n{:?}", checkpoint);
            };

            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
            yield_now().await;
        }
    }
}
