use clap::{Arg, Command};

pub fn new_task_cmd() -> Command {
    clap::Command::new("task")
        .subcommand(task_exec())
        .subcommand(task_analyze_source())
}

fn task_exec() -> Command {
    clap::Command::new("exec")
        .about("execute task description yaml file")
        .args(&[Arg::new("filepath")
            .value_name("filepath")
            .required(true)
            .index(1)
            .help("execute task description yaml file")])
}

fn task_analyze_source() -> Command {
    clap::Command::new("analyze")
        .about("analyze source objects destributed")
        .args(&[Arg::new("filepath")
            .value_name("filepath")
            .required(true)
            .index(1)
            .help("analyze source objects destributed")])
}
