use std::sync::{atomic::AtomicU64, Arc};

#[derive(Debug, Clone)]
pub struct IncrementAssistant {
    pub check_point_path: String,
    pub local_notify: Option<LocalNotify>,
    pub last_modify_timestamp: Option<i64>,
}

impl Default for IncrementAssistant {
    fn default() -> Self {
        Self {
            last_modify_timestamp: None,
            local_notify: None,
            check_point_path: "".to_string(),
        }
    }
}

impl IncrementAssistant {
    pub fn set_local_notify(&mut self, local_notify: Option<LocalNotify>) {
        self.local_notify = local_notify;
    }

    pub fn get_notify_file_path(&self) -> Option<String> {
        match self.local_notify.clone() {
            Some(n) => Some(n.notify_file_path),
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalNotify {
    pub notify_file_path: String,
    pub notify_file_size: Arc<AtomicU64>,
}
