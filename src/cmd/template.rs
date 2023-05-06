use clap::{Arg, Command};

pub fn new_template() -> Command {
    clap::Command::new("template")
        .about("generate oss task description yaml template")
        .subcommand(template_download())
        .subcommand(template_transfer())
        .subcommand(template_upload())
        .subcommand(local_to_local())
        .subcommand(truncate_bucket())
        .subcommand(oss_compare())
}

fn template_download() -> Command {
    clap::Command::new("download")
        .about("generate oss task description template for download task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn template_transfer() -> Command {
    clap::Command::new("transfer")
        .about("generate oss task description template for transfer task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn template_upload() -> Command {
    clap::Command::new("upload")
        .about("generate oss task description template for upload task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn local_to_local() -> Command {
    clap::Command::new("localtolocal")
        .about("generate oss task description template for local to local task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn truncate_bucket() -> Command {
    clap::Command::new("truncate_bucket")
        .about("generate oss task description template for local to local task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn oss_compare() -> Command {
    clap::Command::new("oss_compare")
        .about("generate oss task description template for local to local task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}
