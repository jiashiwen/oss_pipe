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

use notify::{
    event::CreateKind, Config, Error, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};

use serde::{Deserialize, Serialize};
use tokio::task::yield_now;

use super::struct_to_json_string;

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

    pub async fn watch_to_file(self, file: File, file_size: Arc<AtomicU64>) {
        let mut linewiter = LineWriter::new(&file);
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
            match file.metadata() {
                Ok(meta) => {
                    file_size.store(meta.len(), Ordering::SeqCst);
                }
                Err(_) => {}
            };

            yield_now().await;
        }
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
