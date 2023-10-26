use std::sync::{atomic::AtomicBool, Arc};

use crypto::buffer::ReadBuffer;
use dashmap::DashMap;
use tokio::task::yield_now;

use crate::checkpoint::{CheckPoint, FilePosition};

use super::{TaskRunningStatus, OFFSET_PREFIX};

pub struct TaskStatusSaver {
    pub save_to: String,
    pub execute_file_path: String,
    pub stop_mark: Arc<AtomicBool>,
    pub list_file_positon_map: Arc<DashMap<String, FilePosition>>,
    pub file_for_notify: Option<String>,
    pub task_running_status: TaskRunningStatus,
    pub interval: u64,
}

impl TaskStatusSaver {
    pub async fn snapshot_to_file(&self) {
        let checkpoint = CheckPoint {
            execute_file_path: self.execute_file_path.clone(),
            execute_file_position: FilePosition {
                offset: 0,
                line_num: 0,
            },
            file_for_notify: self.file_for_notify.clone(),
            task_running_satus: self.task_running_status,
        };
        let _ = checkpoint.save_to(&self.save_to);

        while !self.stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
            let notify = self.file_for_notify.clone();
            let offset = match self
                .list_file_positon_map
                .iter()
                .filter(|f| f.key().starts_with(OFFSET_PREFIX))
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

            let checkpoint = CheckPoint {
                execute_file_path: self.execute_file_path.clone(),
                execute_file_position: file_position.value().clone(),
                file_for_notify: notify,
                task_running_satus: self.task_running_status,
            };

            if let Err(e) = checkpoint.save_to(&self.save_to) {
                log::error!("{}", e);
            };
            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
            yield_now().await;
        }
    }
}
