use super::FilePosition;
use crate::{
    commons::{read_yaml_file, struct_to_yaml_string},
    tasks::TaskStage,
};
use anyhow::{Error, Ok, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExecutedFile {
    pub path: String,
    pub size: u64,
    pub total_lines: u64,
}

impl Default for ExecutedFile {
    fn default() -> Self {
        Self {
            path: "".to_string(),
            size: 0,
            total_lines: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckPoint {
    //当前全量对象列表
    pub current_stock_object_list_file: String,
    // 对象列表命名规则：OBJECT_LIST_FILE_PREFIX+秒级unix 时间戳 'objeclt_list_unixtimestampe'
    pub executed_file: ExecutedFile,
    // 文件执行位置，既执行到的offset，用于断点续传
    pub executed_file_position: FilePosition,
    pub file_for_notify: Option<String>,
    pub task_stage: TaskStage,
    // 记录 checkpoint 时点的时间戳
    pub timestampe: u128,
}

impl Default for CheckPoint {
    fn default() -> Self {
        Self {
            executed_file: Default::default(),
            executed_file_position: FilePosition {
                offset: 0,
                line_num: 0,
            },
            file_for_notify: Default::default(),
            task_stage: TaskStage::Stock,
            timestampe: 0,
            current_stock_object_list_file: "".to_string(),
        }
    }
}

impl FromStr for CheckPoint {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let r = serde_yaml::from_str::<Self>(s)?;
        Ok(r)
    }
}

impl CheckPoint {
    pub fn seeked_execute_file(&self) -> Result<File> {
        let mut file = File::open(&self.executed_file.path)?;
        let seek_offset = TryInto::<u64>::try_into(self.executed_file_position.offset)?;
        file.seek(SeekFrom::Start(seek_offset))?;
        Ok(file)
    }
    pub fn save_to(&mut self, path: &str) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        self.timestampe = u128::from(now.as_secs());

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let constent = struct_to_yaml_string(self)?;
        file.write_all(constent.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    pub fn save_to_file(&self, file: &mut File) -> Result<()> {
        let constent = struct_to_yaml_string(self)?;
        file.write_all(constent.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}

// pub fn get_task_checkpoint(checkpoint_file: &str, meta_dir: &str) -> Result<CheckPoint> {
pub fn get_task_checkpoint(checkpoint_file: &str) -> Result<CheckPoint> {
    let checkpoint = read_yaml_file::<CheckPoint>(checkpoint_file)?;
    Ok(checkpoint)
}

#[cfg(test)]
mod test {

    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use std::{
        fs::{self, File},
        io::{self, BufRead},
        str::FromStr,
    };

    use crate::checkpoint::checkpoint::get_task_checkpoint;
    use crate::checkpoint::checkpoint::CheckPoint;
    use crate::checkpoint::checkpoint::ExecutedFile;
    use crate::checkpoint::FilePosition;
    use crate::commons::scan_folder_files_to_file;
    //cargo test checkpoint::checkpoint::test::test_get_task_checkpoint -- --nocapture
    #[test]
    fn test_get_task_checkpoint() {
        println!("get_task_checkpoint");
        let c = get_task_checkpoint("/tmp/meta_dir/checkpoint.yml");
        println!("{:?}", c);
    }

    //cargo test checkpoint::checkpoint::test::test_checkpoint -- --nocapture
    #[test]
    fn test_checkpoint() {
        let path = "/tmp/jddownload/objlist";
        let _ = fs::remove_file(path);
        scan_folder_files_to_file("/tmp", path).unwrap();
        let mut f = File::open(path).unwrap();

        let mut positon = 0;
        let mut line_num = 0;

        let lines = io::BufReader::new(&f).lines();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let save_path = "/tmp/checkpoint";
        for line in lines {
            if line_num > 3 {
                println!("positon:{}", positon);
                let file_position = FilePosition {
                    offset: positon,
                    line_num,
                };
                let mut checkpoint = CheckPoint {
                    executed_file: ExecutedFile {
                        path: path.to_string(),
                        size: 0,
                        total_lines: 0,
                    },
                    executed_file_position: file_position,
                    file_for_notify: None,
                    task_stage: crate::tasks::TaskStage::Stock,
                    timestampe: u128::from(now.as_secs()),
                    current_stock_object_list_file: path.to_string(),
                };

                let _ = checkpoint.save_to(save_path);
                break;
            }
            match line {
                Ok(l) => {
                    let len = l.bytes().len() + "\n".bytes().len();
                    positon = positon + len;
                    println!("line: {}", l);
                    line_num += 1;
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }

        let content = fs::read_to_string(save_path).unwrap();

        let ck = CheckPoint::from_str(content.as_str()).unwrap();
        f = ck.seeked_execute_file().unwrap();

        let lines = io::BufReader::new(&f).lines();

        for line in lines {
            if line_num > 5 {
                break;
            }
            match line {
                Ok(l) => {
                    let len = l.bytes().len() + "\n".bytes().len();

                    positon = positon + len;
                    println!("line: {}", l);
                    line_num += 1;
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }

        println!("content {:?}", content);

        println!("ck is {:?}", ck);
        println!("end!!!!");
    }
}
