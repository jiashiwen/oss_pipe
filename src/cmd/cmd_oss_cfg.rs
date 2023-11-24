use clap::{Arg, Command};

pub fn new_osscfg_cmd() -> Command {
    clap::Command::new("osscfg")
        .about("osscfg")
        .subcommand(osscfg_generate_default())
}

fn osscfg_generate_default() -> Command {
    clap::Command::new("gendefault")
        .about("generate default oss config to file")
        .args(&[Arg::new("filepath").value_name("filepath").index(1)])
}
