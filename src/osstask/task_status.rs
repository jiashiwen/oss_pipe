use std::sync::{atomic::AtomicBool, Arc};

use dashmap::DashMap;
use tokio::task::yield_now;

use crate::checkpoint::CheckPoint;

use super::{TaskRunningStatus, CURRENT_LINE_PREFIX, OFFSET_PREFIX};

pub struct TaskStatusSaver {
    pub save_to: String,
    pub execute_file_path: String,
    pub stop_mark: Arc<AtomicBool>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub file_for_notify: Option<String>,
    pub task_running_status: TaskRunningStatus,
    pub interval: u64,
}

impl TaskStatusSaver {
    pub async fn snapshot_to_file(&self) {
        let checkpoint = CheckPoint {
            execute_file_path: self.execute_file_path.clone(),
            execute_position: 0,
            line_number: 0,
            file_for_notify: self.file_for_notify.clone(),
            task_running_satus: self.task_running_status,
        };
        let _ = checkpoint.save_to(&self.save_to);

        while !self.stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
            let notify = self.file_for_notify.clone();
            let offset = match self
                .offset_map
                .iter()
                .filter(|f| f.key().starts_with(OFFSET_PREFIX))
                .map(|m| *m.value())
                .min()
            {
                Some(o) => o,
                None => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
                    continue;
                }
            };
            let line_num = match self
                .offset_map
                .iter()
                .filter(|f| f.key().starts_with(CURRENT_LINE_PREFIX))
                .map(|m| *m.value())
                .min()
            {
                Some(l) => l,
                None => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
                    continue;
                }
            };
            match offset.try_into() {
                Ok(position) => {
                    let checkpoint = CheckPoint {
                        execute_file_path: self.execute_file_path.clone(),
                        execute_position: position,
                        line_number: line_num,
                        file_for_notify: notify,
                        task_running_satus: self.task_running_status,
                    };
                    let _ = checkpoint.save_to(&self.save_to);
                }
                _ => {}
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
            yield_now().await;
        }
    }
}
