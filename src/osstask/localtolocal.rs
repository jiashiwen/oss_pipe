use crate::{checkpoint::Record, commons::multi_parts_copy_file, exception::save_error_record};
use anyhow::anyhow;
use anyhow::Result;
use dashmap::DashMap;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

use super::CURRENT_LINE_PREFIX;
use super::{err_process, gen_file_path, ERROR_RECORD_PREFIX, OFFSET_EXEC_PREFIX};

#[derive(Debug, Clone)]
pub struct LocalToLocal {
    pub source_path: String,
    pub target_path: String,
    pub error_conter: Arc<AtomicUsize>,
    pub offset_map: Arc<DashMap<String, usize>>,
    pub meta_dir: String,
    pub target_exist_skip: bool,
    pub large_file_size: usize,
    pub multi_part_chunk: usize,
}

impl LocalToLocal {
    pub async fn exec(&self, records: Vec<Record>) -> Result<()> {
        // let mut line_num = self.begin_line_number;
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_EXEC_PREFIX.to_string();
        let mut current_line_key = CURRENT_LINE_PREFIX.to_string();
        offset_key.push_str(&subffix);
        // current_line_key.push_str(&self.begin_line_number.to_string());

        let error_file_name = gen_file_path(&self.meta_dir, ERROR_RECORD_PREFIX, &subffix);

        // 先写首行日志，避免错误漏记
        self.offset_map
            .insert(offset_key.clone(), records[0].offset);

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        for record in records {
            let s_file_name = gen_file_path(self.source_path.as_str(), &record.key.as_str(), "");
            let t_file_name = gen_file_path(self.target_path.as_str(), record.key.as_str(), "");

            // 判断源文件是否存在
            let s_path = Path::new(s_file_name.as_str());
            if !s_path.exists() {
                self.offset_map.insert(offset_key.clone(), record.offset);
                continue;
            }

            let t_path = Path::new(t_file_name.as_str());
            if let Some(p) = t_path.parent() {
                if let Err(e) = std::fs::create_dir_all(p) {
                    // log::error!("{}", e);
                    // save_error_record(&self.error_conter, record.clone(), &mut error_file);
                    // self.offset_map.insert(offset_key.clone(), record.offset);
                    err_process(
                        &self.error_conter,
                        anyhow!(e.to_string()),
                        record,
                        &mut error_file,
                        offset_key.as_str(),
                        current_line_key.as_str(),
                        &self.offset_map,
                    );
                    continue;
                };
            };

            let s_file = match OpenOptions::new().read(true).open(s_file_name.as_str()) {
                Ok(p) => p,
                Err(e) => {
                    log::error!("{}", e);
                    save_error_record(&self.error_conter, record.clone(), &mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            };

            let mut t_file = match OpenOptions::new()
                .truncate(true)
                .create(true)
                .write(true)
                .open(t_file_name.as_str())
            {
                Ok(p) => p,
                Err(e) => {
                    log::error!("{}", e);
                    save_error_record(&self.error_conter, record.clone(), &mut error_file);
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            };

            // 目标object存在则不推送
            if self.target_exist_skip {
                if t_path.exists() {
                    self.offset_map.insert(offset_key.clone(), record.offset);
                    continue;
                }
            }

            let s_file_len = match s_file.metadata() {
                Ok(m) => m.len(),
                Err(e) => {
                    log::error!("{}", e);
                    self.error_conter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = record.save_json_to_file(&mut error_file);
                    continue;
                }
            };

            let len: usize = match s_file_len.try_into() {
                Ok(l) => l,
                Err(e) => {
                    log::error!("{}", e);
                    self.error_conter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = record.save_json_to_file(&mut error_file);
                    continue;
                }
            };

            // 大文件走 multi part upload 分支
            match match len > self.large_file_size {
                true => multi_parts_copy_file(
                    s_file_name.as_str(),
                    t_file_name.as_str(),
                    self.multi_part_chunk,
                ),
                false => {
                    let data = fs::read(s_file_name.as_str())?;
                    t_file.write_all(&data)?;
                    t_file.flush()?;
                    Ok(())
                }
            } {
                Err(e) => {
                    log::error!("{}", e);
                    self.error_conter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = record.save_json_to_file(&mut error_file);
                }
                _ => (),
            };

            self.offset_map.insert(offset_key.clone(), record.offset);
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
}
