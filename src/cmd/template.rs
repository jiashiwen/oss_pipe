use clap::{Arg, Command};

pub fn new_template() -> Command {
    clap::Command::new("template")
        .about("generate oss task description yaml template")
        .subcommand(template_transfer())
        .subcommand(truncate_bucket())
        .subcommand(oss_compare())
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

// fn template_upload() -> Command {
//     clap::Command::new("upload")
//         .about("generate oss task description template for upload task")
//         .args(&[Arg::new("file")
//             .value_name("file")
//             .required(false)
//             .index(1)
//             .help("specific output file path")])
// }

// fn local_to_local() -> Command {
//     clap::Command::new("localtolocal")
//         .about("generate oss task description template for local to local task")
//         .args(&[Arg::new("file")
//             .value_name("file")
//             .required(false)
//             .index(1)
//             .help("specific output file path")])
// }

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
