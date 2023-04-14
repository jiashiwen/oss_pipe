use super::rand_util::rand_string;
use anyhow::Result;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, LineWriter, Read, Write},
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

pub fn multi_parts_copy_file(source: &str, target: &str, chunk_size: usize) -> Result<()> {
    let mut f_source = OpenOptions::new().read(true).open(source)?;
    let mut f_target = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(target)?;

    loop {
        let mut buffer = vec![0; chunk_size];
        let read_count = f_source.read(&mut buffer)?;
        let buf = &buffer[..read_count];
        f_target.write_all(&buf)?;
        if read_count != chunk_size {
            break;
        }
    }
    f_target.flush()?;
    Ok(())
}

fn remove_dir_contents<P: AsRef<Path>>(path: P) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        if entry.file_type()?.is_dir() {
            remove_dir_contents(&path)?;
            fs::remove_dir(path)?;
        } else {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

pub fn scan_folder_files_to_file(folder: &str, file_name: &str) -> Result<()> {
    let path = std::path::Path::new(file_name);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    };
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
            if p.eq(folder) {
                continue;
            }
            let mut key: &str = "";
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

// 生成指定字节数的文件
pub fn generate_file(file_size: usize, batch: usize, file_name: &str) -> Result<()> {
    let str_len = file_size / batch;
    let remainder = file_size % batch;

    // 生成文件目录
    let store_path = Path::new(file_name);
    let path = std::path::Path::new(store_path);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    };

    let file_ref = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(file_name.clone())?;
    let mut file = LineWriter::new(file_ref);
    let str = rand_string(batch);
    for _ in 0..str_len {
        let _ = file.write_all(str.as_bytes());
    }

    if remainder > 0 {
        let str = rand_string(batch);
        let _ = file.write_all(str.as_bytes());
    }

    file.flush()?;

    Ok(())
}

// 批量生成文件，文件名定长随机
// pub fn generate_file_batch(files: usize,file_size:usize,dir:&str)

// 生成指定行数，指定每行字节数的文件
pub fn generate_line_file(line_base_size: usize, lines: usize, file_name: &str) -> Result<()> {
    // 生成文件目录
    let store_path = Path::new(file_name);
    let path = std::path::Path::new(store_path);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    };

    let file_ref = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(file_name.clone())?;
    let mut file = LineWriter::new(file_ref);
    let str = rand_string(line_base_size);
    for i in 0..lines {
        let mut line = str.clone();
        line.push_str(&i.to_string());
        line.push_str("\n");
        let _ = file.write_all(line.as_bytes());
    }
    file.flush()?;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::commons::{
        fileutiles::{generate_file, scan_folder_files_to_file},
        multi_parts_copy_file,
    };

    use super::generate_line_file;

    //cargo test commons::fileutiles::test::test_generate_line_file -- --nocapture
    #[test]
    fn test_generate_line_file() {
        let r = generate_line_file(1020, 1048576, "/tmp/gen/gen_line_file");
        println!("test scan result {:?}", r);
    }

    //cargo test commons::fileutiles::test::test_gen_file -- --nocapture
    #[test]
    fn test_gen_file() {
        let r = generate_file(128, 8, "/tmp/gen/gen_file");
        // println!("test scan result {:?}", r);
    }

    //cargo test commons::fileutiles::test::test_scan_folder_files_to_file -- --nocapture
    #[test]
    fn test_scan_folder_files_to_file() {
        let r = scan_folder_files_to_file("/tmp/", "/tmp/test_scan");
        println!("test scan result {:?}", r);
    }

    //cargo test commons::fileutiles::test::test_multi_parts_copy_file -- --nocapture
    #[test]
    fn test_multi_parts_copy_file() {
        let r = multi_parts_copy_file("/tmp/oss_pipe", "/tmp/genfilecp", 1024);
        println!("test scan result {:?}", r);
    }
}
