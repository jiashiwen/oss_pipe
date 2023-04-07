use std::{
    fs::{File, OpenOptions},
    io::Write,
    str::FromStr,
};

use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::commons::{json_to_struct, read_lines, read_yaml_file, struct_to_yaml_string};

use super::Record;

const CHECKPOINT_FILE_NAME: &'static str = "checkpoint";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckPoint {
    pub execute_file_path: String,
    pub execute_position: u64,
}

impl FromStr for CheckPoint {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let r = serde_yaml::from_str::<Self>(s)?;
        Ok(r)
    }
}
impl CheckPoint {
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        let constent = struct_to_yaml_string(self)?;
        file.write_all(constent.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}

pub fn get_task_checkpoint(checkpoint_file: &str, error_record_dir: &str) -> Result<CheckPoint> {
    let mut checkpoint = read_yaml_file::<CheckPoint>(checkpoint_file)?;
    let mut tmp = usize::try_from(checkpoint.execute_position)?;

    // 遍历error record 目录，并提取错误记录offset
    for entry in WalkDir::new(error_record_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
    {
        if let Some(p) = entry.path().to_str() {
            if let Ok(lines) = read_lines(p) {
                for line in lines {
                    if let Ok(content) = line {
                        if let Ok(r) = json_to_struct::<Record>(&content) {
                            if r.offset < tmp {
                                tmp = r.offset
                            }
                        };
                    }
                }
            };
        };
    }

    let tmp_u64 = u64::try_from(tmp)?;
    if tmp_u64 < checkpoint.execute_position {
        checkpoint.execute_position = tmp_u64;
    }

    Ok(checkpoint)
}

#[cfg(test)]
mod test {
    use std::io::SeekFrom;
    use std::io::{Read, Seek};
    use std::{
        fs::{self, File},
        io::{self, BufRead},
        str::FromStr,
    };

    use crate::checkpoint::checkpoint::get_task_checkpoint;
    use crate::checkpoint::checkpoint::CheckPoint;
    //cargo test checkpoint::checkpoint::test::test_get_task_checkpoint -- --nocapture
    #[test]
    fn test_get_task_checkpoint() {
        println!("get_task_checkpoint");
        let c = get_task_checkpoint("checkpoint.yml", "/tmp/err_dir");
        println!("{:?}", c);
    }

    //cargo test checkpoint::checkpoint::test::test_checkpoint -- --nocapture
    #[test]
    fn test_checkpoint() {
        let path = "/tmp/jddownload/.objlist";
        let mut f = File::open(path).unwrap();

        let mut positon = 0u64;
        let mut line_num = 0;

        let lines = io::BufReader::new(&f).lines();

        for line in lines {
            if line_num > 3 {
                break;
            }
            match line {
                Ok(l) => {
                    let len = l.bytes().len() + "\n".bytes().len();
                    let len_u64: u64 = len.try_into().unwrap();
                    positon = positon + len_u64;
                    println!("line: {}", l);
                    line_num += 1;
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }

        println!("positon:{}", positon);

        f.seek(SeekFrom::Start(positon)).unwrap();
        let mut line = String::new();

        let lines = io::BufReader::new(&f).lines();
        line_num = 0;

        for line in lines {
            if line_num > 5 {
                break;
            }
            match line {
                Ok(l) => {
                    let len = l.bytes().len() + "\n".bytes().len();
                    let len_u64: u64 = len.try_into().unwrap();
                    positon = positon + len_u64;
                    println!("line: {}", l);
                    line_num += 1;
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }
        // let mut fb = io::BufReader::new(&f);
        // fb.read_line(&mut line);
        // persistence_checkpoint(positon, "/tmp/.checkpoint".to_string());
        let checkpoint = CheckPoint {
            execute_file_path: path.to_string(),
            execute_position: positon,
        };

        checkpoint.save_to_file("/tmp/.checkpoint");

        let content = fs::read_to_string(path).unwrap();
        println!("content {:?}", content);

        let ck = CheckPoint::from_str(content.as_str()).unwrap();

        println!("ck is {:?}", ck);

        println!("end!!!!");
    }
}
