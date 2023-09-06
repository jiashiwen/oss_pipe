use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
};

use dashmap::DashMap;
use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

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
            modify_type: ModifyType::Create,
        }
    }
}

#[derive(Debug)]
pub struct InotifyWatcher {
    inotify: Inotify,
    watched_dir: String,
    map_wdid_dir: DashMap<i32, String>,
    map_dir_wd: DashMap<String, WatchDescriptor>,
}

impl InotifyWatcher {
    pub fn new(dir: &str) -> io::Result<Self> {
        let inotify = Inotify::init()?;
        let watched_dir = dir.to_string();
        let map_wdid_dir = DashMap::new();
        let map_dir_wd = DashMap::new();

        for entry in WalkDir::new(dir)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_dir())
        {
            match inotify.watches().add(
                entry.path(),
                WatchMask::MODIFY | WatchMask::CREATE | WatchMask::DELETE,
            ) {
                Ok(wd) => {
                    map_wdid_dir.insert(
                        wd.get_watch_descriptor_id(),
                        entry.path().display().to_string(),
                    );
                    map_dir_wd.insert(entry.path().display().to_string(), wd);
                }
                Err(e) => {
                    return Err(e);
                }
            };
        }
        // let watched_dir = map_wdid_dir.get(&1).unwrap().value().to_string();
        Ok(Self {
            inotify,
            watched_dir,
            map_wdid_dir,
            map_dir_wd,
        })
    }

    pub fn watch_to_file<P: AsRef<Path>>(mut self, file_path: P) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)?;

        let mut buffer = [0u8; 4096];

        loop {
            let events = match self.inotify.read_events_blocking(&mut buffer) {
                Ok(events) => events,
                Err(e) => {
                    println!("{}", e);
                    continue;
                }
            };

            for event in events {
                let mut modify = Modified::new();
                let idx = event.wd.get_watch_descriptor_id();
                let mut path = match self.map_wdid_dir.get(&idx) {
                    Some(kv) => kv.value().to_string(),
                    None => {
                        continue;
                    }
                };
                match event.name {
                    Some(s) => {
                        path.push('/');
                        path.push_str(s.to_str().unwrap());
                    }
                    None => {}
                }

                println!("{:?}", event.mask);

                if event.mask.contains(EventMask::CREATE) {
                    modify.modify_type = ModifyType::Create;
                    if event.mask.contains(EventMask::ISDIR) {
                        modify.path_type = PathType::Folder;
                        match self.inotify.watches().add(
                            &path,
                            WatchMask::MODIFY | WatchMask::CREATE | WatchMask::DELETE,
                        ) {
                            Ok(wd) => {
                                self.map_wdid_dir
                                    .insert(wd.get_watch_descriptor_id(), path.clone());
                                self.map_dir_wd.insert(path.clone(), wd.clone());
                            }
                            Err(e) => {
                                println!("{}", e);
                                continue;
                            }
                        };
                    } else {
                        modify.path_type = PathType::File;
                    }
                    modify.path = path.clone();
                }

                if event.mask.contains(EventMask::DELETE) {
                    modify.modify_type = ModifyType::Delete;
                    if event.mask.contains(EventMask::ISDIR) {
                        modify.path_type = PathType::Folder;
                        let wd = self.map_dir_wd.get(&path).unwrap();
                        let v = wd.value();

                        match self.inotify.watches().remove(v.clone()) {
                            Ok(()) => {}
                            Err(e) => {
                                println!("{}", e)
                            }
                        };
                        self.map_wdid_dir
                            .remove(&v.clone().get_watch_descriptor_id());
                        self.map_dir_wd.remove(&path);
                    } else {
                        modify.path_type = PathType::File;
                    }
                    modify.path = path.clone();
                }

                if event.mask.contains(EventMask::MODIFY) {
                    modify.path_type = PathType::Folder;
                    modify.modify_type = ModifyType::Modify;
                    if event.mask.contains(EventMask::ISDIR) {
                    } else {
                        modify.path_type = PathType::File;
                    }
                    modify.path = path.clone();
                }

                if !modify.path.is_empty() {
                    match struct_to_json_string(&modify) {
                        Ok(json) => {
                            let _ = file.write_all(json.as_bytes());
                            let _ = file.write_all("\n".as_bytes());
                        }
                        Err(_) => {}
                    };
                }
            }
        }
        // Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::InotifyWatcher;

    //cargo test commons::inotify_utile::test::test_ -- --nocapture
    #[test]
    fn test_() {
        let inotify_watcher = InotifyWatcher::new("/tmp").unwrap();
        inotify_watcher.watch_to_file("/root/watch.log");
    }
}
