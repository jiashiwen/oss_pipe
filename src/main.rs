use crate::logger::init_log;
mod checkers;
pub mod checkpoint;
mod cmd;
mod commons;
mod configure;

mod interact;
mod logger;
mod osstask;
mod s3;

fn main() {
    // env_logger::init();
    init_log();
    cmd::run_app();
}
