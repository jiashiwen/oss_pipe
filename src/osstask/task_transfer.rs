use super::{
    task_actions::TransferTaskActions, TransferLocal2Local, TransferLocal2Oss, TransferOss2Local,
    TransferOss2Oss, TransferTaskAttributes,
};
use crate::s3::OSSDescription;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ObjectStorage {
    Local(String),
    OSS(OSSDescription),
}

impl Default for ObjectStorage {
    fn default() -> Self {
        ObjectStorage::OSS(OSSDescription::default())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferTask {
    pub source: ObjectStorage,
    pub target: ObjectStorage,
    pub attributes: TransferTaskAttributes,
}

impl TransferTask {
    pub fn gen_transfer_actions(&self) -> Box<dyn TransferTaskActions> {
        match &self.source {
            ObjectStorage::Local(path_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferLocal2Local {
                        source: path_s.to_string(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferLocal2Oss {
                        source: path_s.to_string(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
            },
            ObjectStorage::OSS(oss_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferOss2Local {
                        source: oss_s.clone(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferOss2Oss {
                        source: oss_s.clone(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Box::new(t)
                }
            },
        }
    }
}
