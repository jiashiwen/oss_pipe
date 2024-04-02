use super::struct_to_json_string;
use notify::{
    event::CreateKind, Config, Error, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{LineWriter, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::Receiver,
        Arc,
    },
};
use tokio::task::yield_now;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
    pub watcher: RecommendedWatcher,
    reciver: Receiver<Result<Event, Error>>,
    writing_file_status: bool,
    pub watched_dir: String,
}

// Todo
// 新增write_stop用来替换notify 文件
impl NotifyWatcher {
    pub fn new<P: AsRef<Path>>(watched_dir: P) -> notify::Result<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = RecommendedWatcher::new(tx, Config::default())?;
        watcher.watch(watched_dir.as_ref(), RecursiveMode::Recursive)?;

        Ok(Self {
            watcher,
            watched_dir: watched_dir.as_ref().to_str().unwrap().to_string(),
            reciver: rx,
            writing_file_status: false,
        })
    }

    pub async fn watch_to_file(mut self, file: File, file_size: Arc<AtomicU64>) {
        let mut linewiter = LineWriter::new(&file);

        self.writing_file_status = true;
        for res in self.reciver {
            if !self.writing_file_status {
                return;
            }
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
                            notify::event::ModifyKind::Any => {}
                            notify::event::ModifyKind::Metadata(_) => {}
                            notify::event::ModifyKind::Name(_) => {}
                            notify::event::ModifyKind::Other => {}
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
                            notify::event::RemoveKind::Any => {}
                            notify::event::RemoveKind::Other => {}
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
                        Err(e) => {
                            log::error!("{}", e)
                        }
                    };
                }
            }

            match file.metadata() {
                Ok(meta) => {
                    file_size.store(meta.len(), Ordering::SeqCst);
                }
                Err(_) => {}
            };
            yield_now().await;
        }
    }

    #[allow(dead_code)]
    pub fn stop_write_file(&mut self) {
        self.writing_file_status = false;
    }

    #[allow(dead_code)]
    pub fn writing_file_status(&self) -> bool {
        self.writing_file_status
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::OpenOptions,
        sync::{atomic::AtomicU64, Arc},
    };

    use tokio::{runtime, task::JoinSet};

    use super::NotifyWatcher;

    //cargo test commons::notify_utile::test::test_watcher -- --nocapture
    #[test]
    fn test_watcher() {
        let watch_file_name = "/tmp/watch.log";
        let rt = runtime::Builder::new_current_thread().build().unwrap();

        let mut set: JoinSet<()> = JoinSet::new();
        let file_size = Arc::new(AtomicU64::new(0));
        let _rt_rs = rt.block_on(async move {
            let _rs_watch = set.spawn(async move {
                println!("begin watch");
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(watch_file_name)
                    .unwrap();
                let notify_watcher = NotifyWatcher::new("/tmp/files").unwrap();
                notify_watcher
                    .watch_to_file(file, Arc::clone(&file_size))
                    .await;
            });
            let _rs_read = set.spawn(async move {
                println!("begin read watch file");
            });
            if set.len() > 0 {
                set.join_next().await;
            }
        });
    }
}
