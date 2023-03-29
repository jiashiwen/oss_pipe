use std::{
    fs::{File, OpenOptions},
    io::Write,
    str::FromStr,
};

use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::commons::struct_to_yaml_string;

const CHECKPOINT_FILE_NAME: &'static str = ".checkpoint";

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

#[cfg(test)]
mod test {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::{
        fs::{self, File},
        io::{self, BufRead},
        str::FromStr,
    };

    use crate::checkpoint::checkpoint::CheckPoint;

    //cargo test checkpoint::checkpoint::test::test_checkpoint -- --nocapture
    #[test]
    fn test_checkpoint() {
        let path = "/tmp/jddownload/.objlist";
        let mut f = File::open(path).unwrap();

        // move the cursor 42 bytes from the start of the file
        // f.seek(SeekFrom::Start(26)).unwrap();
        // let before = f.stream_position().unwrap();
        // let mut fb = io::BufReader::new(&f);
        // let mut line = String::new();
        // fb.read_line(&mut line);
        // println!("line:{:?}", line);
        // println!("line size:{:?}", line.bytes().len());
        // let after = f.stream_position().unwrap();
        // println!("before:{},after:{}", before, after);

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
