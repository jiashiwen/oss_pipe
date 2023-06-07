use clap::{Arg, Command};

pub fn new_osstask_cmd() -> Command {
    clap::Command::new("osstask")
        .about("osstask")
        .args(&[Arg::new("filepath")
            .value_name("filepath")
            .required(true)
            .index(1)
            .help("transfer task description file")])
}
