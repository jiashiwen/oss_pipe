use super::{TaskStage, OFFSET_PREFIX};
use crate::checkpoint::{CheckPoint, FilePosition};
use dashmap::DashMap;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::task::yield_now;

pub struct TaskStatusSaver {
    pub check_point_path: String,
    pub execute_file_path: String,
    pub stop_mark: Arc<AtomicBool>,
    pub list_file_positon_map: Arc<DashMap<String, FilePosition>>,
    pub file_for_notify: Option<String>,
    pub task_stage: TaskStage,
    pub interval: u64,
}

impl TaskStatusSaver {
    pub async fn snapshot_to_file(&self) {
        let mut checkpoint = CheckPoint {
            execute_file_path: self.execute_file_path.clone(),
            execute_file_position: FilePosition {
                offset: 0,
                line_num: 0,
            },
            file_for_notify: self.file_for_notify.clone(),
            task_stage: self.task_stage,
            timestampe: 0,
        };
        let _ = checkpoint.save_to(&self.check_point_path);

        while !self.stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
            let notify = self.file_for_notify.clone();
            let offset = match self
                .list_file_positon_map
                .iter()
                .filter(|item| item.key().starts_with(OFFSET_PREFIX))
                .map(|m| m.offset)
                .min()
            {
                Some(o) => o,
                None => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
                    continue;
                }
            };

            let mut offset_key = OFFSET_PREFIX.to_string();
            offset_key.push_str(offset.to_string().as_str());

            let file_position = match self.list_file_positon_map.get(&offset_key) {
                Some(p) => p,
                None => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
                    continue;
                }
            };

            let mut checkpoint = CheckPoint {
                execute_file_path: self.execute_file_path.clone(),
                execute_file_position: file_position.value().clone(),
                file_for_notify: notify,
                task_stage: self.task_stage,
                timestampe: 0,
            };

            if let Err(e) = checkpoint.save_to(&self.check_point_path) {
                log::error!("{}", e);
            };

            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
            yield_now().await;
        }
    }
}
