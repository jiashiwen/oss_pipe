use crate::configure::get_config;
use anyhow::{anyhow, Result};
use std::fs;

//检查配置目录是否存在，不存在则创建
#[allow(dead_code)]
pub fn check_local_desc_path() -> Result<()> {
    let cfg = get_config()?;
    let meta = fs::metadata(cfg.task_config.task_desc_local_path.clone());
    return match meta {
        Ok(m) => {
            if m.is_dir() {
                Ok(())
            } else {
                Err(anyhow!("not a dir"))
            }
        }
        Err(_) => {
            return fs::create_dir(cfg.task_config.task_desc_local_path.clone())
                .map_err(|e| anyhow::Error::new(e));
        }
    };
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::configure::set_config;

    //cargo test checkers::init_check::test::test_check_local_desc_path -- --nocapture
    #[test]
    fn test_check_local_desc_path() {
        set_config("");
        println!("{:?}", check_local_desc_path());
    }
}
