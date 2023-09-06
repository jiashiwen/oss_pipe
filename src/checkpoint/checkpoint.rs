use std::{
    fs::{File, OpenOptions},
    io::Write,
    str::FromStr,
};

use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::{
    commons::{read_lines, read_yaml_file, struct_to_yaml_string},
    osstask::OFFSET_PREFIX,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckPoint {
    // 对象列表命名规则：OBJECT_LIST_FILE_PREFIX+秒级unix 时间戳 'objeclt_list_unixtimestampe'
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
    pub fn save_to(&self, path: &str) -> Result<()> {
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

pub fn get_task_checkpoint(checkpoint_file: &str, meta_dir: &str) -> Result<CheckPoint> {
    let mut checkpoint = read_yaml_file::<CheckPoint>(checkpoint_file)?;

    // 遍历offset 日志文件，选取每个文件中最大的offset，当offset 小于checkpoint中的offset，则取较小值
    for entry in WalkDir::new(meta_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir() && e.file_name().to_str().is_some())
    {
        let file_name = entry.file_name().to_str().unwrap();

        if !file_name.starts_with(OFFSET_PREFIX) {
            continue;
        };

        if let Some(p) = entry.path().to_str() {
            if let Ok(lines) = read_lines(p) {
                let mut max_offset_in_the_file = 0;
                for line in lines {
                    if let Ok(content) = line {
                        match content.parse::<u64>() {
                            Ok(offset) => {
                                if offset > max_offset_in_the_file {
                                    max_offset_in_the_file = offset
                                }
                            }
                            Err(_) => {
                                continue;
                            }
                        };
                    }
                }
                if max_offset_in_the_file < checkpoint.execute_position {
                    checkpoint.execute_position = max_offset_in_the_file
                }
            };
        };
    }
    Ok(checkpoint)
}

#[cfg(test)]
mod test {
    use std::io::Seek;
    use std::io::SeekFrom;
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
        let c = get_task_checkpoint("/tmp/meta_dir/checkpoint.yml", "/tmp/meta_dir");
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

        let checkpoint = CheckPoint {
            execute_file_path: path.to_string(),
            execute_position: positon,
        };

        let _ = checkpoint.save_to("/tmp/.checkpoint");

        let content = fs::read_to_string(path).unwrap();
        println!("content {:?}", content);

        let ck = CheckPoint::from_str(content.as_str()).unwrap();

        println!("ck is {:?}", ck);

        println!("end!!!!");
    }
}
