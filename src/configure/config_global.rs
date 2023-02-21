use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::sync::RwLock;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_yaml::from_str;

use crate::configure::config_error::{ConfigError, ConfigErrorType};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JdCloud {
    pub ak: String,
    pub sk: String,
    pub region: String,
    pub s3_endpoint: String,
}

impl JdCloud {
    pub fn default() -> Self {
        Self {
            ak: "".to_string(),
            sk: "".to_string(),
            region: "cn-north-1".to_string(),
            s3_endpoint: "http://s3.cn-north-1.jdcloud-oss.com".to_string(),
        }
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskConfig {
    pub threads: usize,
    pub ticker_sec: usize,
    pub task_desc_local_path: String,
    pub task_desc_s3_bucket: String,
    pub task_desc_s3_prefix: String,
}

impl TaskConfig {
    pub fn default() -> Self {
        Self {
            threads: 1,
            ticker_sec: 60,
            task_desc_local_path: "./".to_string(),
            task_desc_s3_bucket: "pingdata".to_string(),
            task_desc_s3_prefix: "pingdata/6f0de4620aa567399a84c282ba6b2594/".to_string(),
        }
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub threads: usize,
    pub ticker_sec: usize,
    pub ping_file: String,
    pub curl_file: String,
    pub jdcloud: JdCloud,
    pub task_config: TaskConfig,
}

impl Config {
    pub fn default() -> Self {
        Self {
            threads: 1,
            ticker_sec: 10,
            ping_file: "ping.json".to_string(),
            curl_file: "curl.json".to_string(),
            jdcloud: JdCloud::default(),
            task_config: TaskConfig::default(),
        }
    }

    pub fn set_self(&mut self, config: Config) {
        self.threads = config.threads;
        self.ticker_sec = config.ticker_sec;
        self.ping_file = config.ping_file;
        self.curl_file = config.curl_file;
        self.jdcloud = config.jdcloud;
        self.task_config = config.task_config;
    }

    pub fn get_config_image(&self) -> Self {
        self.clone()
    }
}

pub fn generate_default_config(path: &str) -> Result<()> {
    let config = Config::default();
    let yml = serde_yaml::to_string(&config)?;
    fs::write(path, yml)?;
    Ok(())
}

lazy_static::lazy_static! {
    static ref GLOBAL_CONFIG: Mutex<Config> = {
        let global_config = Config::default();
        Mutex::new(global_config)
    };
    static ref CONFIG_FILE_PATH: RwLock<String> = RwLock::new({
        let path = "".to_string();
        path
    });
}

pub fn set_config(path: &str) {
    if path.is_empty() {
        if Path::new("config.yml").exists() {
            let contents =
                fs::read_to_string("config.yml").expect("Read config file config.yml error!");
            let config = from_str::<Config>(contents.as_str()).expect("Parse config.yml error!");
            GLOBAL_CONFIG.lock().unwrap().set_self(config);
        }
        return;
    }

    let err_str = format!("Read config file {} error!", path);
    let contents = fs::read_to_string(path).expect(err_str.as_str());
    let config = from_str::<Config>(contents.as_str()).expect("Parse config.yml error!");
    GLOBAL_CONFIG.lock().unwrap().set_self(config);
}

pub fn set_config_file_path(path: String) {
    CONFIG_FILE_PATH
        .write()
        .expect("clear config file path error!")
        .clear();
    CONFIG_FILE_PATH.write().unwrap().push_str(path.as_str());
}

pub fn get_config_file_path() -> String {
    CONFIG_FILE_PATH.read().unwrap().clone()
}

pub fn get_config() -> Result<Config> {
    let locked_config = GLOBAL_CONFIG.lock().map_err(|e| {
        return ConfigError::from_err(e.to_string(), ConfigErrorType::UnknowErr);
    })?;
    Ok(locked_config.get_config_image())
}

pub fn get_current_config_yml() -> Result<String> {
    let c = get_config()?;
    let yml = serde_yaml::to_string(&c)?;
    Ok(yml)
}
