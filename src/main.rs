use crate::logger::init_log;
mod checkers;
mod cmd;
mod commons;
mod configure;
mod interact;
mod logger;
mod osstask;
mod s3;
pub mod checkpoint;

fn main() {
    init_log();
    cmd::run_app();
}
