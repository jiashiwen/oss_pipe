use std::{
    fs::File,
    io::{self, BufRead, LineWriter, Seek, SeekFrom, Write},
    path::Path,
    sync::mpsc::Receiver,
    thread,
    time::Duration,
};

use notify::{
    event::CreateKind, Config, Error, Event, EventKind, FsEventWatcher, RecommendedWatcher,
    RecursiveMode, Watcher,
};

use serde::{Deserialize, Serialize};
use tokio::task::yield_now;

use super::struct_to_json_string;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum PathType {
    Folder,
    File,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ModifyType {
    Unkown,
    Create,
    Delete,
    Modify,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct Modified {
    pub path: String,
    pub path_type: PathType,
    pub modify_type: ModifyType,
}

impl Modified {
    pub fn new() -> Self {
        Self {
            path: "".to_string(),
            path_type: PathType::File,
            modify_type: ModifyType::Unkown,
        }
    }
}

#[derive(Debug)]
pub struct NotifyWatcher {
    watcher: FsEventWatcher,
    reciver: Receiver<Result<Event, Error>>,
    pub watched_dir: String,
}

impl NotifyWatcher {
    pub fn new<P: AsRef<Path>>(watched_dir: P) -> notify::Result<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = RecommendedWatcher::new(tx, Config::default())?;
        watcher.watch(watched_dir.as_ref(), RecursiveMode::Recursive)?;

        Ok(Self {
            watcher,
            watched_dir: watched_dir.as_ref().to_str().unwrap().to_string(),
            reciver: rx,
        })
    }

    pub async fn watch_to_file(self, file: File) {
        let mut linewiter = LineWriter::new(file);

        for res in self.reciver {
            let mut modified = Modified::new();
            match res {
                Ok(event) => {
                    modified.path = event.paths[0].as_path().display().to_string();
                    match event.kind {
                        EventKind::Create(c) => match c {
                            CreateKind::File => {
                                modified.modify_type = ModifyType::Create;
                                modified.path_type = PathType::File;
                            }
                            CreateKind::Folder => {
                                modified.modify_type = ModifyType::Create;
                                modified.path_type = PathType::Folder;
                            }
                            CreateKind::Any => {}
                            CreateKind::Other => {}
                        },
                        EventKind::Modify(m) => match m {
                            notify::event::ModifyKind::Data(_) => {
                                modified.modify_type = ModifyType::Modify;
                                modified.path_type = PathType::File;
                            }
                            _ => {}
                        },
                        EventKind::Remove(r) => match r {
                            notify::event::RemoveKind::File => {
                                modified.modify_type = ModifyType::Delete;
                                modified.path_type = PathType::File;
                            }
                            notify::event::RemoveKind::Folder => {
                                modified.modify_type = ModifyType::Delete;
                                modified.path_type = PathType::Folder;
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }
                Err(error) => println!("{}", error),
            }

            match modified.modify_type {
                ModifyType::Unkown => {}
                _ => {
                    match struct_to_json_string(&modified) {
                        Ok(json) => {
                            let _ = linewiter.write_all(json.as_bytes());
                            let _ = linewiter.write_all("\n".as_bytes());
                        }
                        Err(_) => {}
                    };
                }
            }
            yield_now().await;
        }
    }
}

pub async fn read_modifed_file(file_name: &str) -> std::io::Result<()> {
    let mut offset = 0;
    let mut line_num = 0;

    loop {
        let mut file = File::open(file_name)?;
        // 获取文件长度
        let file_len = match file.metadata() {
            Ok(m) => m.len(),
            Err(e) => {
                return Err(e);
            }
        };
        println!("offset:{};file len:{}", offset, file_len);
        if file_len > offset {
            file.seek(SeekFrom::Start(offset))?;

            let lines = io::BufReader::new(file).lines();
            for line in lines {
                if file_len.eq(&offset) {
                    break;
                }
                if let Result::Ok(key) = line {
                    let len = key.bytes().len() + "\n".bytes().len();
                    let len_u64 = u64::try_from(len).unwrap();
                    offset += Into::<u64>::into(len_u64);
                    line_num += 1;
                    println!("{}", key);
                }
            }
        }
        thread::sleep(Duration::from_secs(1));
        yield_now().await;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;

    use tokio::{
        runtime,
        task::{self, yield_now, JoinSet},
    };

    use super::{read_modifed_file, NotifyWatcher};

    //cargo test commons::notify_utile::test::test_watcher -- --nocapture
    #[test]
    fn test_watcher() {
        let watch_file_name = "/tmp/watch.log";
        let rt = runtime::Builder::new_current_thread()
            // .max_blocking_threads(1)
            .build()
            .unwrap();
        // let rt = runtime::Builder::new_multi_thread()
        //     .worker_threads(2)
        //     .enable_all()
        //     // .max_io_events_per_tick(self.task_threads)
        //     .build()
        //     .unwrap();
        // let mut handles = Vec::new();

        let mut set: JoinSet<()> = JoinSet::new();
        let rt_rs = rt.block_on(async move {
            let rs_watch = set.spawn(async move {
                println!("begin watch");
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(watch_file_name)
                    .unwrap();
                let notify_watcher = NotifyWatcher::new("/tmp/files").unwrap();
                notify_watcher.watch_to_file(file).await;
            });
            let rs_read = set.spawn(async move {
                println!("begin read watch file");
                let _ = read_modifed_file(watch_file_name).await;
            });
            if set.len() > 0 {
                set.join_next().await;
            }
        });
    }

    //cargo test commons::notify_utile::test::test_read_modifed_file -- --nocapture
    #[test]
    fn test_read_modifed_file() {
        let watch_file_name = "/tmp/watch.log";
        let _ = read_modifed_file(watch_file_name);
    }
}
