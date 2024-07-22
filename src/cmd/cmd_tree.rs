use clap::Command;

pub fn new_command_tree_cmd() -> Command {
    clap::Command::new("tree").about("show command tree")
}
