#[derive(Debug, Clone)]
pub struct LogInfo<T> {
    pub task_id: String,
    pub msg: String,
    pub additional: Option<T>,
}
