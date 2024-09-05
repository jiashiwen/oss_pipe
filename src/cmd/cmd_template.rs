use clap::{Arg, Command};

pub fn new_template() -> Command {
    clap::Command::new("template")
        .about("generate oss task description yaml template")
        .subcommand(template_transfer())
        .subcommand(delete_bucket())
        .subcommand(compare())
}

fn template_transfer() -> Command {
    clap::Command::new("transfer")
        .subcommand(template_transfer_oss2oss())
        .subcommand(template_transfer_oss2local())
        .subcommand(template_transfer_local2oss())
        .subcommand(template_transfer_local2local())
}

fn template_transfer_oss2oss() -> Command {
    clap::Command::new("oss2oss")
        .about("generate oss task description template for oss to oss task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn template_transfer_oss2local() -> Command {
    clap::Command::new("oss2local")
        .about("generate oss task description template for download from oss to local task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn template_transfer_local2oss() -> Command {
    clap::Command::new("local2oss")
        .about("generate oss task description template for local upload to oss task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn template_transfer_local2local() -> Command {
    clap::Command::new("local2local")
        .about("generate oss task description template for local copy task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn delete_bucket() -> Command {
    clap::Command::new("delete_bucket")
        .about("generate oss task description template for local to local task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}

fn compare() -> Command {
    clap::Command::new("compare")
        .about("generate oss task description template for local to local task")
        .args(&[Arg::new("file")
            .value_name("file")
            .required(false)
            .index(1)
            .help("specific output file path")])
}
