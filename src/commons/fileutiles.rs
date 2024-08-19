use super::{rand_util::rand_string, size_distributed, LastModifyFilter, RegexFilter};
use crate::checkpoint::FileDescription;
use anyhow::Result;
use dashmap::DashMap;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, LineWriter, Read, Write},
    path::Path,
    time::UNIX_EPOCH,
};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct FilePart {
    pub part_num: i32,
    pub offset: u64,
}

pub fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

pub fn copy_file(
    source: &str,
    target: &str,
    multi_parts_size: usize,
    chunk_size: usize,
) -> Result<()> {
    let f_source = OpenOptions::new().read(true).open(source)?;
    let mut f_target = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(target)?;
    let len = f_source.metadata()?.len();
    let len_usize = TryInto::<usize>::try_into(len)?;
    match len_usize.gt(&multi_parts_size) {
        true => {
            multi_parts_copy_file(source, target, chunk_size)?;
        }
        false => {
            let data = fs::read(source)?;
            f_target.write_all(&data)?;
            f_target.flush()?;
        }
    }
    Ok(())
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

#[allow(dead_code)]
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

pub fn analyze_folder_files_size(
    folder: &str,
    regex_filter: Option<RegexFilter>,
    last_modify_filter: Option<LastModifyFilter>,
) -> Result<DashMap<String, i128>> {
    let size_map = DashMap::<String, i128>::new();
    for entry in WalkDir::new(folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
    {
        if let Some(p) = entry.path().to_str() {
            if p.eq(folder) {
                continue;
            }

            if let Some(f) = &regex_filter {
                if !f.filter(p) {
                    continue;
                }
            }

            if let Some(f) = &last_modify_filter {
                let modified_time = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();
                // if !f.filter(i128::from(modified_time)) {
                //     continue;
                // }
                if !f.filter(usize::try_from(modified_time).unwrap()) {
                    continue;
                }
            }

            let obj_size = i128::from(entry.metadata()?.len());
            let key = size_distributed(obj_size);
            let mut size = match size_map.get(&key) {
                Some(m) => *m.value(),
                None => 0,
            };
            size += 1;
            size_map.insert(key, size);
        };
    }
    Ok(size_map)
}

// Todo
// 加入正则过滤功能
pub fn scan_folder_files_to_file(
    folder: &str,
    file_name: &str,
    last_modify_filter: Option<LastModifyFilter>,
) -> Result<FileDescription> {
    let mut total_lines = 0;
    let path = std::path::Path::new(file_name);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    };
    //写入文件
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_name)?;
    let mut line_writer = LineWriter::new(&file);

    // 遍历目录并将文件路径写入文件
    for entry in WalkDir::new(folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
    {
        if let Some(p) = entry.path().to_str() {
            if p.eq(folder) {
                continue;
            }

            if let Some(f) = last_modify_filter {
                let modified_time = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();
                if !f.filter(usize::try_from(modified_time).unwrap()) {
                    continue;
                }
            }

            let key = match folder.ends_with("/") {
                true => &p[folder.len()..],
                false => &p[folder.len() + 1..],
            };

            let _ = line_writer.write_all(key.as_bytes());
            let _ = line_writer.write_all("\n".as_bytes());
            total_lines += 1;
        };
        line_writer.flush()?;
    }
    let size = file.metadata()?.len();
    let executed_file = FileDescription {
        path: file_name.to_string(),
        size,
        total_lines,
    };
    Ok(executed_file)
}

// 生成指定字节数的文件
pub fn generate_file(file_size: usize, chunk_size: usize, file_name: &str) -> Result<()> {
    let str_len = file_size / chunk_size;
    let remainder = file_size % chunk_size;

    // 生成文件目录
    let store_path = Path::new(file_name);
    let path = std::path::Path::new(store_path);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    };

    let file_ref = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_name)?;
    let mut file = LineWriter::new(file_ref);
    let str = rand_string(chunk_size);
    for _ in 0..str_len {
        let _ = file.write_all(str.as_bytes());
    }

    if remainder > 0 {
        let str = rand_string(remainder);
        let _ = file.write_all(str.as_bytes());
    }

    file.flush()?;

    Ok(())
}

// 用 0 填充临时文件
pub fn fill_file_with_zero(file_size: usize, chunk_size: usize, file_name: &str) -> Result<()> {
    let batch = file_size / chunk_size;
    let remainder = file_size % chunk_size;

    // 生成文件目录
    let store_path = Path::new(file_name);
    let path = std::path::Path::new(store_path);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    };

    let mut file_ref = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_name)?;
    // let mut file = LineWriter::new(file_ref);
    let buffer = vec![0; chunk_size];
    for _ in 0..batch {
        let _ = file_ref.write_all(&buffer);
    }

    if remainder > 0 {
        let buffer = vec![0; remainder];
        let _ = file_ref.write_all(&buffer);
    }

    file_ref.flush()?;

    Ok(())
}

pub fn merge_file<P: AsRef<Path>>(file: P, merge_to: P, chunk_size: usize) -> Result<()> {
    let mut f = OpenOptions::new().read(true).open(file)?;
    let mut merge_to = OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(merge_to)?;

    loop {
        let mut buffer = vec![0; chunk_size];
        let read_count = f.read(&mut buffer)?;
        let buf = &buffer[..read_count];
        merge_to.write_all(&buf)?;
        if read_count != chunk_size {
            break;
        }
    }
    merge_to.flush()?;
    Ok(())
}

pub fn merge_files(filename: &str, chunk_size: usize, file_parts: Vec<String>) -> Result<()> {
    let merged_path = Path::new(filename);
    if let Some(p) = merged_path.parent() {
        std::fs::create_dir_all(p)?;
    };
    let mut merged_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(filename)?;
    for part in file_parts {
        let mut part_file = OpenOptions::new().read(true).open(&part)?;
        loop {
            let mut buffer = vec![0; chunk_size];
            let read_count = part_file.read(&mut buffer)?;
            let buf = &buffer[..read_count];
            merged_file.write_all(&buf)?;
            if read_count != chunk_size {
                break;
            }
        }
        merged_file.flush()?;
    }

    Ok(())
}
// 生成指定行数，指定每行字节数的文件
#[allow(dead_code)]
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
        .open(file_name)?;
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

pub fn generate_files(
    dir: &str,
    file_prefix_len: usize,
    file_size: usize,
    chunk_size: usize,
    file_quantity: usize,
) -> Result<()> {
    let dir_path = Path::new(dir);
    if !dir_path.exists() {
        std::fs::create_dir_all(dir_path)?;
    };

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()?;

    let batch = file_size / chunk_size;
    let remainder = file_size % chunk_size;
    let chunk = rand_string(chunk_size);
    let last_chunk = match remainder > 0 {
        true => rand_string(remainder),
        false => "".to_string(),
    };

    pool.scope(|s| {
        for _ in 0..file_quantity {
            let ck = chunk.clone();
            let l_ck = last_chunk.clone();
            s.spawn(move |_| {
                let mut file_prefix = rand_string(file_prefix_len);
                // let mut file_name = file_prefix.clone();
                let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
                file_prefix.push_str(now.to_string().as_str());

                let file_path = match dir.ends_with("/") {
                    true => {
                        let mut file_path = dir.to_string();
                        file_path.push_str(file_prefix.as_str());
                        file_path
                    }
                    false => {
                        let mut file_path = dir.to_string();
                        file_path.push_str("/");
                        file_path.push_str(file_prefix.as_str());
                        file_path
                    }
                };
                // 生成文件目录
                let store_path = Path::new(&file_path);
                let path = std::path::Path::new(store_path);
                if let Some(p) = path.parent() {
                    if let Err(e) = std::fs::create_dir_all(p) {
                        log::error!("{:?}", e);
                        return;
                    };
                };

                let mut file = match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(file_path)
                {
                    Ok(f) => f,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                };

                for _ in 0..batch {
                    let _ = file.write_all(ck.as_bytes());
                }

                if remainder > 0 {
                    let _ = file.write_all(l_ck.as_bytes());
                }

                if let Err(e) = file.flush() {
                    log::error!("{}", e);
                };
            });
        }
    });

    Ok(())
}

pub fn gen_file_part_plan(file_path: &str, chunk_size: usize) -> Result<Vec<FilePart>> {
    let mut vec_file_parts: Vec<FilePart> = vec![];
    let f = File::open(file_path)?;
    let meta = f.metadata()?;
    let file_len = meta.len();
    let chunk_size_u64 = TryInto::<u64>::try_into(chunk_size)?;
    let mut offset = 0;
    let quotient = file_len / chunk_size_u64;
    let remainder = file_len % chunk_size_u64;
    let part_quantities = match remainder.eq(&0) {
        true => quotient,
        false => quotient + 1,
    };
    for part_num_u64 in 1..=part_quantities {
        let part_num = TryInto::<i32>::try_into(part_num_u64)?;
        let file_part = FilePart { part_num, offset };
        vec_file_parts.push(file_part);
        offset += chunk_size_u64;
    }

    Ok(vec_file_parts)
}

#[cfg(test)]
mod test {
    use crate::commons::{fileutiles::generate_file, fill_file_with_zero, multi_parts_copy_file};

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
        let _ = generate_file(128, 8, "/tmp/gen/gen_file");
        // println!("test scan result {:?}", r);
    }

    //cargo test commons::fileutiles::test::test_multi_parts_copy_file -- --nocapture
    #[test]
    fn test_multi_parts_copy_file() {
        let r = multi_parts_copy_file("/tmp/oss_pipe", "/tmp/genfilecp", 1024);
        println!("test scan result {:?}", r);
    }

    //cargo test commons::fileutiles::test::test_fill_file_with_zero -- --nocapture
    #[test]
    fn test_fill_file_with_zero() {
        let r = fill_file_with_zero(1024 * 1024 * 1024 * 10, 1024 * 1024, "/tmp/zero_file");
        println!("test_fill_file_with_zero {:?}", r);
    }
}
