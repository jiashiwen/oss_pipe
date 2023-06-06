use anyhow::{Ok, Result};
use async_trait::async_trait;

#[async_trait]
pub trait TaskActions {
    type Item;
    fn error_record_retry(&self) -> Result<()>;
    fn gen_executor(&self) -> Self::Item;
}

#[async_trait]
pub trait Excutor {
    fn print_name(&self);
}

pub struct TaskTxt {
    id: String,
}

pub struct Taskexecutor {
    name: String,
}

impl Excutor for Taskexecutor {
    fn print_name(&self) {
        println!("{}", self.name);
    }
}

impl TaskActions for TaskTxt {
    type Item = Taskexecutor;
    fn error_record_retry(&self) -> Result<()> {
        Ok(())
    }
    fn gen_executor(&self) -> Taskexecutor {
        let taskexecutor = Taskexecutor {
            name: "exec".to_string(),
        };
        taskexecutor
    }
}

#[cfg(test)]
mod test {

    //cargo test osstask::task_actions::test::test_task_trait -- --nocapture
    #[test]
    fn test_task_trait() {
        println!("save offset");
    }
}
