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
    // Todo
    // debug 全量为何无position数据
    // 解析问题，需要遍历list_file_positon_map找出最小值对应的对象
    pub async fn snapshot_to_file(&self) {
        let mut checkpoint = CheckPoint {
            execute_file: self.execute_file_path.clone(),
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
            // log::warn!("execute checkpoint,file positon:{:?}", file_position);
            self.list_file_positon_map.shrink_to_fit();
            let mut checkpoint = CheckPoint {
                execute_file: self.execute_file_path.clone(),
                execute_file_position: file_position,
                file_for_notify: notify,
                task_stage: self.task_stage,
                timestampe: 0,
            };

            if let Err(e) = checkpoint.save_to(&self.check_point_path) {
                log::error!("{},{}", e, self.check_point_path);
            };

            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
            yield_now().await;
        }
    }
}
