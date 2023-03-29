use anyhow::Result;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, LineWriter, Write},
    path::Path,
};
use walkdir::WalkDir;

pub fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

pub fn scan_folder_files_to_file(folder: &str, file_name: &str) -> Result<()> {
    //写入文件
    let file_ref = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(file_name)?;
    let mut file = LineWriter::new(file_ref);

    // 遍历目录并上传
    for entry in WalkDir::new(folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
    {
        if let Some(p) = entry.path().to_str() {
            println!("{}", p);
            if p.eq(folder) {
                continue;
            }
            let mut key = "";
            if folder.ends_with("/") {
                key = &p[folder.len()..];
            } else {
                key = &p[folder.len() + 1..];
            }

            let _ = file.write_all(key.as_bytes());
            let _ = file.write_all("\n".as_bytes());
        };
        file.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::commons::fileutiles::scan_folder_files_to_file;

    //cargo test commons::fileutiles::test::test_scan_folder_files_to_file -- --nocapture
    #[test]
    fn test_scan_folder_files_to_file() {
        let r = scan_folder_files_to_file("/tmp/", "/tmp/test_scan");
        println!("test scan result {:?}", r);
    }
}
