pub use thiserror::Error;

#[derive(Error, Debug)]
pub enum TaskError {
    #[error("task yml file '{0}` error")]
    TaskYmlFileError(String),
    #[error("Task execute error")]
    TaskExecuteError(),
    // TaskYmlFileNotFund(#[from] io::Error),
    #[error("the data for key `{0}` is not available")]
    Redaction(String),
    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader { expected: String, found: String },
    #[error("unknown data store error")]
    Unknown,
}
