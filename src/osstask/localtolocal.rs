use super::{gen_file_path, ERROR_RECORD_PREFIX, OFFSET_PREFIX};
use crate::checkpoint::{FilePosition, Opt, RecordDescription};
use crate::{checkpoint::ListedRecord, commons::multi_parts_copy_file};
use anyhow::anyhow;
use anyhow::Result;
use dashmap::DashMap;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

#[derive(Debug, Clone)]
pub struct LocalToLocal {
    pub source_path: String,
    pub target_path: String,
    pub error_conter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
    pub list_file_path: String,
}

impl LocalToLocal {
    // Todo
    // 如果在批次处理开始前出现报错则整批数据都不执行，需要有逻辑执行错误记录
    pub async fn exec(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);
        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        // 先写首行日志，避免错误漏记
        self.offset_map.insert(
            offset_key.clone(),
            FilePosition {
                offset: records[0].offset,
                line_num: records[0].line_num,
            },
        );

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        for record in records {
            let s_file_name = gen_file_path(self.source_path.as_str(), record.key.as_str(), "");
            let t_file_name = gen_file_path(self.target_path.as_str(), record.key.as_str(), "");

            if let Err(e) = self
                .record_handler(
                    offset_key.as_str(),
                    &record,
                    s_file_name.as_str(),
                    t_file_name.as_str(),
                )
                .await
            {
                // 记录错误记录
                let recorddesc = RecordDescription {
                    source_key: s_file_name,
                    target_key: t_file_name,
                    list_file_path: self.list_file_path.clone(),
                    list_file_position: FilePosition {
                        offset: record.offset,
                        line_num: record.line_num,
                    },
                    option: Opt::PUT,
                };
                recorddesc.handle_error(
                    anyhow!("{}", e),
                    &self.error_conter,
                    &self.offset_map,
                    &mut error_file,
                    offset_key.as_str(),
                );
            };

            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );
        }

        let _ = error_file.flush();
        match error_file.metadata() {
            Ok(meta) => {
                if meta.len() == 0 {
                    let _ = fs::remove_file(error_file_name.as_str());
                }
            }
            Err(_) => {}
        };
        self.offset_map.remove(&offset_key);
        let _ = fs::remove_file(offset_key.as_str());

        Ok(())
    }

    async fn record_handler(
        &self,
        offset_key: &str,
        record: &ListedRecord,
        source_file: &str,
        target_file: &str,
    ) -> Result<()> {
        // 判断源文件是否存在，若不存判定为成功传输
        let s_path = Path::new(source_file);
        if !s_path.exists() {
            self.offset_map.insert(
                offset_key.to_string(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );
            return Ok(());
        }

        let t_path = Path::new(target_file);
        if let Some(p) = t_path.parent() {
            std::fs::create_dir_all(p)?
        };

        // 目标object存在则不推送
        if self.target_exist_skip {
            if t_path.exists() {
                self.offset_map.insert(
                    offset_key.to_string(),
                    FilePosition {
                        offset: record.offset,
                        line_num: record.line_num,
                    },
                );
                return Ok(());
            }
        }

        let s_file = OpenOptions::new().read(true).open(source_file)?;
        let mut t_file = OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(target_file)?;

        let s_file_len = s_file.metadata()?.len();
        let len: usize = TryInto::try_into(s_file_len)?;

        // 大文件走 multi part upload 分支
        match len > self.large_file_size {
            true => multi_parts_copy_file(source_file, target_file, self.multi_part_chunk),
            false => {
                let data = fs::read(source_file)?;
                t_file.write_all(&data)?;
                t_file.flush()?;
                Ok(())
            }
        }
    }
}
