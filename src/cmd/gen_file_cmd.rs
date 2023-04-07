use clap::{value_parser, Arg, Command};

pub fn new_gen_file_cmd() -> Command {
    clap::Command::new("gen_file")
        .about("gen_file")
        .args(&[Arg::new("file_size")
            .value_name("file_size")
            .value_parser(value_parser!(usize))
            .required(true)
            .index(1)
            .help("specific generated file size")])
        .args(&[Arg::new("batch")
            .value_name("batch")
            .required(true)
            .value_parser(value_parser!(usize))
            .index(2)
            .help("specific generated file write batch size")])
        .args(&[Arg::new("file_name")
            .value_name("file_name")
            .required(true)
            .index(3)
            .help("specific generated file path")])
}
