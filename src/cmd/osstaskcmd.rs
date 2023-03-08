use clap::{Arg, Command};

pub fn new_osstask_cmd() -> Command {
    clap::Command::new("osstask")
        .about("osstask")
        .subcommand(osstask_transfer())
        .subcommand(osstask_download())
        .subcommand(osstask_template())
}

fn osstask_transfer() -> Command {
    clap::Command::new("transfer")
        .about("execute transfer task")
        .args(&[Arg::new("filepath")
            .value_name("filepath")
            .required(true)
            .index(1)
            .help("transfer task description file")])
}

fn osstask_download() -> Command {
    clap::Command::new("download")
        .about("execute download task")
        .args(&[Arg::new("filepath")
            .value_name("filepath")
            .required(true)
            .index(1)
            .help("download task description file")])
}

fn osstask_template() -> Command {
    clap::Command::new("template")
        .about("generate oss task description yaml template")
        .subcommand(osstask_template_download())
        .subcommand(osstask_template_transfer())
}

fn osstask_template_download() -> Command {
    clap::Command::new("download").about("generate oss task description template for download task")
}

fn osstask_template_transfer() -> Command {
    clap::Command::new("transfer").about("generate oss task description template for transfer task")
}
