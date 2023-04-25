use clap::Command;

pub fn new_exit_cmd() -> Command {
    clap::Command::new("exit").about("exit oss pipe")
}
