use crate::logger::init_log;
mod checkers;
pub mod checkpoint;
mod cmd;
mod commons;
mod configure;

mod exception;
mod interact;
mod logger;
mod s3;
mod tasks;

fn main() {
    // console_subscriber::init();
    init_log();
    cmd::run_app();
}
