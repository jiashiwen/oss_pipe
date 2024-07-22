use clap::Command;

pub fn new_parameters_cmd() -> Command {
    clap::Command::new("parameters")
        .about("show task config parameters list")
        .subcommand(parameters_provider())
        .subcommand(parameters_task_type())
}

fn parameters_provider() -> Command {
    clap::Command::new("provider").about("show suppored oss providers")
}

fn parameters_task_type() -> Command {
    clap::Command::new("task_type").about("show task types")
}
