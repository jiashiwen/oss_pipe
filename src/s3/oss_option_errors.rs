use thiserror::Error;

#[derive(Error, Debug)]
pub enum OssError {
    #[error("upload error")]
    UpLoadError,

    #[error("upload id is None")]
    UpLoadIdIsNone,

    #[error("object content length is None")]
    ObjectContentLenghtIsNone,
}
