use clap::{Arg, Command};

pub fn new_osstask_cmd() -> Command {
    clap::Command::new("osstask")
        .about("osstask")
        // .subcommand(osstask_truncatebucket())
        .subcommand(osstask_template())
        .args(&[Arg::new("filepath")
            .value_name("filepath")
            .required(false)
            .index(1)
            .help("transfer task description file")])
}

fn osstask_template() -> Command {
    clap::Command::new("template")
        .about("generate oss task description yaml template")
        .subcommand(osstask_template_download())
        .subcommand(osstask_template_transfer())
        .subcommand(osstask_template_upload())
        .subcommand(osstask_local_to_local())
        .subcommand(osstask_truncate_bucket())
}

fn osstask_template_download() -> Command {
    clap::Command::new("download").about("generate oss task description template for download task")
}

fn osstask_template_transfer() -> Command {
    clap::Command::new("transfer").about("generate oss task description template for transfer task")
}

fn osstask_template_upload() -> Command {
    clap::Command::new("upload").about("generate oss task description template for upload task")
}

fn osstask_local_to_local() -> Command {
    clap::Command::new("localtolocal")
        .about("generate oss task description template for local to local task")
}

fn osstask_truncate_bucket() -> Command {
    clap::Command::new("truncate_bucket")
        .about("generate oss task description template for local to local task")
}
